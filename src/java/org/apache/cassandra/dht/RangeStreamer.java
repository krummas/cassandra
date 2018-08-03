/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.dht;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.locator.LocalStrategy;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Assists in streaming ranges to a node.
 */
public class RangeStreamer
{
    private static final Logger logger = LoggerFactory.getLogger(RangeStreamer.class);

    /* bootstrap tokens. can be null if replacing the node. */
    private final Collection<Token> tokens;
    /* current token ring */
    private final TokenMetadata metadata;
    /* address of this node */
    private final InetAddressAndPort address;
    /* streaming description */
    private final String description;
    private final Multimap<String, Map.Entry<InetAddressAndPort, Collection<Range<Token>>>> toFetch = HashMultimap.create();
    private final Set<ISourceFilter> sourceFilters = new HashSet<>();
    private final boolean useStrictConsistency;
    private final IEndpointSnitch snitch;
    private final StreamStateStore stateStore;
    private final StreamOperation streamOperation;
    private final int connectionsPerHost;
    private final boolean connectSequentially;

    /**
     * A filter applied to sources to stream from when constructing a fetch map.
     */
    public static interface ISourceFilter
    {
        public boolean shouldInclude(InetAddressAndPort endpoint);
    }

    /**
     * Source filter which excludes any endpoints that are not alive according to a
     * failure detector.
     */
    public static class FailureDetectorSourceFilter implements ISourceFilter
    {
        private final IFailureDetector fd;

        public FailureDetectorSourceFilter(IFailureDetector fd)
        {
            this.fd = fd;
        }

        public boolean shouldInclude(InetAddressAndPort endpoint)
        {
            return fd.isAlive(endpoint);
        }
    }

    /**
     * Source filter which excludes any endpoints that are not in a specific data center.
     */
    public static class SingleDatacenterFilter implements ISourceFilter
    {
        private final String sourceDc;
        private final IEndpointSnitch snitch;

        public SingleDatacenterFilter(IEndpointSnitch snitch, String sourceDc)
        {
            this.sourceDc = sourceDc;
            this.snitch = snitch;
        }

        public boolean shouldInclude(InetAddressAndPort endpoint)
        {
            return snitch.getDatacenter(endpoint).equals(sourceDc);
        }
    }

    /**
     * Source filter which excludes the current node from source calculations
     */
    public static class ExcludeLocalNodeFilter implements ISourceFilter
    {
        public boolean shouldInclude(InetAddressAndPort endpoint)
        {
            return !FBUtilities.getBroadcastAddressAndPort().equals(endpoint);
        }
    }

    /**
     * Source filter which only includes endpoints contained within a provided set.
     */
    public static class WhitelistedSourcesFilter implements ISourceFilter
    {
        private final Set<InetAddressAndPort> whitelistedSources;

        public WhitelistedSourcesFilter(Set<InetAddressAndPort> whitelistedSources)
        {
            this.whitelistedSources = whitelistedSources;
        }

        public boolean shouldInclude(InetAddressAndPort endpoint)
        {
            return whitelistedSources.contains(endpoint);
        }
    }

    public RangeStreamer(TokenMetadata metadata,
                         Collection<Token> tokens,
                         InetAddressAndPort address,
                         StreamOperation streamOperation,
                         boolean useStrictConsistency,
                         IEndpointSnitch snitch,
                         StreamStateStore stateStore,
                         boolean connectSequentially,
                         int connectionsPerHost)
    {
        Preconditions.checkArgument(streamOperation == StreamOperation.BOOTSTRAP || streamOperation == StreamOperation.REBUILD, streamOperation);
        this.metadata = metadata;
        this.tokens = tokens;
        this.address = address;
        this.description = streamOperation.getDescription();
        this.streamOperation = streamOperation;
        this.connectionsPerHost = connectionsPerHost;
        this.connectSequentially = connectSequentially;
        this.useStrictConsistency = useStrictConsistency;
        this.snitch = snitch;
        this.stateStore = stateStore;

    }

    public void addSourceFilter(ISourceFilter filter)
    {
        sourceFilters.add(filter);
    }

    /**
     * Add ranges to be streamed for given keyspace.
     *
     * @param keyspaceName keyspace name
     * @param ranges ranges to be streamed
     */
    public void addRanges(String keyspaceName, Collection<Range<Token>> ranges)
    {
        Keyspace ks = Keyspace.open(keyspaceName);
        if (ks.getReplicationStrategy() instanceof LocalStrategy)
        {
            logger.info("Not adding ranges for Local Strategy keyspace={}", keyspaceName);
            return;
        }

        boolean useStrictSource = useStrictSourcesForRanges(keyspaceName);
        Multimap<Range<Token>, InetAddressAndPort> rangesForKeyspace = useStrictSource
                ? getAllRangesWithStrictSourcesFor(keyspaceName, ranges) : getAllRangesWithSourcesFor(keyspaceName, ranges);

        for (Map.Entry<Range<Token>, InetAddressAndPort> entry : rangesForKeyspace.entries())
            logger.info("{}: range {} exists on {} for keyspace {}", description, entry.getKey(), entry.getValue(), keyspaceName);
        AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();
        Multimap<InetAddressAndPort, Range<Token>> rangeFetchMap = useStrictSource || strat == null || strat.getReplicationFactor() == 1
                                                            ? getRangeFetchMap(rangesForKeyspace, sourceFilters, keyspaceName, useStrictConsistency)
                                                            : getOptimizedRangeFetchMap(rangesForKeyspace, sourceFilters, keyspaceName);

        // only BOOTSTRAP and REBUILD uses this - if we want to support REBUILD, add parameter to nodetool rebuild?
        if (!useStrictSource && streamOperation == StreamOperation.BOOTSTRAP)
        {
            ConsistencyLevel cl = DatabaseDescriptor.getBootstrapConsistencyLevel(keyspaceName);
            if (doConsistentBootstrapFor(strat, cl))
            {
                if (cl != ConsistencyLevel.ANY)
                    rangeFetchMap.putAll(getEndpointsForConsistencyLevel(cl, ks, rangesForKeyspace, rangeFetchMap, description, sourceFilters));
            }
            else
            {
                logger.warn("Not doing consistent bootstrap for {} - replication factor is too small {}", keyspaceName, cl.isDatacenterLocal() ? " in dc="+DatabaseDescriptor.getLocalDataCenter() : "");
            }
        }

        for (Map.Entry<InetAddressAndPort, Collection<Range<Token>>> entry : rangeFetchMap.asMap().entrySet())
        {
            if (logger.isTraceEnabled())
            {
                for (Range<Token> r : entry.getValue())
                    logger.trace("{}: range {} from source {} for keyspace {}", description, r, entry.getKey(), keyspaceName);
            }
            toFetch.put(keyspaceName, entry);
        }
    }

    /**
     * We only do consistent bootstrap if the replication factor is larger than 3. If we request a local CL, the RF in
     * the local CL needs to be >= 3
     */
    private boolean doConsistentBootstrapFor(AbstractReplicationStrategy strategy, ConsistencyLevel cl)
    {
        if (strategy == null || strategy.getReplicationFactor() < 3)
            return false;
        if (cl.isDatacenterLocal() && strategy instanceof NetworkTopologyStrategy)
        {
            if (((NetworkTopologyStrategy)strategy).getReplicationFactor(DatabaseDescriptor.getLocalDataCenter()) < 3)
                return false;
        }
        return true;
    }

    @VisibleForTesting
    static Multimap<InetAddressAndPort, Range<Token>> getEndpointsForConsistencyLevel(ConsistencyLevel cl,
                                                                                      Keyspace ks,
                                                                                      Multimap<Range<Token>, InetAddressAndPort> allRangesForKeyspace,
                                                                                      Multimap<InetAddressAndPort, Range<Token>> rangeFetchMap,
                                                                                      String description,
                                                                                      Set<ISourceFilter> sourceFilters)
    {
        Multimap<InetAddressAndPort, Range<Token>> clRangeFetchMap = HashMultimap.create();
        logger.info("{}: Streaming extra ranges for {} with consistency level {}", description, ks.getName(), cl);
        int blockFor = cl.blockFor(ks);
        for (Map.Entry<Range<Token>, Collection<InetAddressAndPort>> allEndpointEntry : allRangesForKeyspace.asMap().entrySet())
        {
            Range<Token> currentRange = allEndpointEntry.getKey();

            List<InetAddressAndPort> potentialEndpoints = Lists.newArrayList(getPotentialCLEndpoints(allEndpointEntry.getValue(),
                                                                                                     cl,
                                                                                                     sourceFilters));
            // todo: optimize where we stream from, currently just shuffles the
            // todo: list of replicas to (hopefully) avoid streaming all unrepaired data from a single node
            Collections.shuffle(potentialEndpoints);

            logger.debug("{}: Potential bootstrap consistencylevel endpoints = {}", description, potentialEndpoints);
            int addedCount = 0;
            // first check if we are already fetching the range from a potential endpoint (if so, we only need to add blockFor - 1 other below)
            // for example, RF=3, we bootstrap with QUORUM, we fetch everything from one node, and need to fetch the unrepaired data
            // from *one* additional node
            for (InetAddressAndPort candidate : potentialEndpoints)
            {
                if (rangeFetchMap.containsKey(candidate) && rangeFetchMap.get(candidate).contains(currentRange))
                {
                    logger.debug("{}: Already fetching {} from {} - counting toward consistencylevel {}", description, currentRange, candidate, cl);
                    addedCount++;
                    break;
                }
            }

            for (InetAddressAndPort candidate : potentialEndpoints)
            {
                if (addedCount >= blockFor)
                    break;
                if (!rangeFetchMap.containsKey(candidate) || !rangeFetchMap.get(candidate).contains(currentRange))
                {
                    clRangeFetchMap.put(candidate, new UnrepairedRange(currentRange));
                    addedCount++;
                    logger.debug("{}: Fetching unrepaired range {} from {}", description, currentRange, candidate);
                }
            }
            if (addedCount < blockFor)
            {
                String msg = String.format("Could not achieve %s from %s for %s in %s", cl, potentialEndpoints, currentRange, ks.getName());
                logger.error("{}: {}", description, msg);
                throw new IllegalStateException(msg);
            }
        }
        return clRangeFetchMap;
    }

    /**
     * get the potential endpoints to stream from based on the consistency level and the source filter
     *
     * Source filters are typically RangeStreamer.FailureDetectorSourceFilter and RangeStreamer.ExcludeLocalNodeFilter
     * which remove down nodes and the local node respectively.
     */
    @VisibleForTesting
    static List<InetAddressAndPort> getPotentialCLEndpoints(Collection<InetAddressAndPort> allEndpoints, ConsistencyLevel cl, Set<ISourceFilter> sourceFilters)
    {
        return allEndpoints.stream()
                           .filter(address -> !cl.isDatacenterLocal() || cl.isLocal(address))
                           .filter(address -> sourceFilters.stream().allMatch(f -> f.shouldInclude(address)))
                           .collect(Collectors.toList());
    }

    static class UnrepairedRange extends Range<Token>
    {
        UnrepairedRange(Range<Token> r)
        {
            super(r.left, r.right);
        }

        public String toString()
        {
            return 'U' + super.toString();
        }
    }


    /**
     * @param keyspaceName keyspace name to check
     * @return true when the node is bootstrapping, useStrictConsistency is true and # of nodes in the cluster is more than # of replica
     */
    private boolean useStrictSourcesForRanges(String keyspaceName)
    {
        AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();
        return useStrictConsistency
                && tokens != null
                && metadata.getSizeOfAllEndpoints() != strat.getReplicationFactor();
    }

    /**
     * Get a map of all ranges and their respective sources that are candidates for streaming the given ranges
     * to us. For each range, the list of sources is sorted by proximity relative to the given destAddress.
     *
     * @throws java.lang.IllegalStateException when there is no source to get data streamed
     */
    private Multimap<Range<Token>, InetAddressAndPort> getAllRangesWithSourcesFor(String keyspaceName, Collection<Range<Token>> desiredRanges)
    {
        AbstractReplicationStrategy strat = Keyspace.open(keyspaceName).getReplicationStrategy();
        Multimap<Range<Token>, InetAddressAndPort> rangeAddresses = strat.getRangeAddresses(metadata.cloneOnlyTokenMap());

        Multimap<Range<Token>, InetAddressAndPort> rangeSources = ArrayListMultimap.create();
        for (Range<Token> desiredRange : desiredRanges)
        {
            for (Range<Token> range : rangeAddresses.keySet())
            {
                if (range.contains(desiredRange))
                {
                    List<InetAddressAndPort> preferred = snitch.getSortedListByProximity(address, rangeAddresses.get(range));
                    rangeSources.putAll(desiredRange, preferred);
                    break;
                }
            }

            if (!rangeSources.keySet().contains(desiredRange))
                throw new IllegalStateException("No sources found for " + desiredRange);
        }

        return rangeSources;
    }

    /**
     * Get a map of all ranges and the source that will be cleaned up once this bootstrapped node is added for the given ranges.
     * For each range, the list should only contain a single source. This allows us to consistently migrate data without violating
     * consistency.
     *
     * @throws java.lang.IllegalStateException when there is no source to get data streamed, or more than 1 source found.
     */
    private Multimap<Range<Token>, InetAddressAndPort> getAllRangesWithStrictSourcesFor(String keyspace, Collection<Range<Token>> desiredRanges)
    {
        assert tokens != null;
        AbstractReplicationStrategy strat = Keyspace.open(keyspace).getReplicationStrategy();

        // Active ranges
        TokenMetadata metadataClone = metadata.cloneOnlyTokenMap();
        Multimap<Range<Token>, InetAddressAndPort> addressRanges = strat.getRangeAddresses(metadataClone);

        // Pending ranges
        metadataClone.updateNormalTokens(tokens, address);
        Multimap<Range<Token>, InetAddressAndPort> pendingRangeAddresses = strat.getRangeAddresses(metadataClone);

        // Collects the source that will have its range moved to the new node
        Multimap<Range<Token>, InetAddressAndPort> rangeSources = ArrayListMultimap.create();

        for (Range<Token> desiredRange : desiredRanges)
        {
            for (Map.Entry<Range<Token>, Collection<InetAddressAndPort>> preEntry : addressRanges.asMap().entrySet())
            {
                if (preEntry.getKey().contains(desiredRange))
                {
                    Set<InetAddressAndPort> oldEndpoints = Sets.newHashSet(preEntry.getValue());
                    Set<InetAddressAndPort> newEndpoints = Sets.newHashSet(pendingRangeAddresses.get(desiredRange));

                    // Due to CASSANDRA-5953 we can have a higher RF then we have endpoints.
                    // So we need to be careful to only be strict when endpoints == RF
                    if (oldEndpoints.size() == strat.getReplicationFactor())
                    {
                        oldEndpoints.removeAll(newEndpoints);
                        assert oldEndpoints.size() == 1 : "Expected 1 endpoint but found " + oldEndpoints.size();
                    }

                    rangeSources.put(desiredRange, oldEndpoints.iterator().next());
                }
            }

            // Validate
            Collection<InetAddressAndPort> addressList = rangeSources.get(desiredRange);
            if (addressList == null || addressList.isEmpty())
                throw new IllegalStateException("No sources found for " + desiredRange);

            if (addressList.size() > 1)
                throw new IllegalStateException("Multiple endpoints found for " + desiredRange);

            InetAddressAndPort sourceIp = addressList.iterator().next();
            EndpointState sourceState = Gossiper.instance.getEndpointStateForEndpoint(sourceIp);
            if (Gossiper.instance.isEnabled() && (sourceState == null || !sourceState.isAlive()))
                throw new RuntimeException("A node required to move the data consistently is down (" + sourceIp + "). " +
                                           "If you wish to move the data from a potentially inconsistent replica, restart the node with -Dcassandra.consistent.rangemovement=false");
        }

        return rangeSources;
    }

    /**
     * @param rangesWithSources The ranges we want to fetch (key) and their potential sources (value)
     * @param sourceFilters A (possibly empty) collection of source filters to apply. In addition to any filters given
     *                      here, we always exclude ourselves.
     * @param keyspace keyspace name
     * @return Map of source endpoint to collection of ranges
     */
    private static Multimap<InetAddressAndPort, Range<Token>> getRangeFetchMap(Multimap<Range<Token>, InetAddressAndPort> rangesWithSources,
                                                                               Collection<ISourceFilter> sourceFilters, String keyspace,
                                                                               boolean useStrictConsistency)
    {
        Multimap<InetAddressAndPort, Range<Token>> rangeFetchMapMap = HashMultimap.create();
        for (Range<Token> range : rangesWithSources.keySet())
        {
            boolean foundSource = false;

            outer:
            for (InetAddressAndPort address : rangesWithSources.get(range))
            {
                for (ISourceFilter filter : sourceFilters)
                {
                    if (!filter.shouldInclude(address))
                        continue outer;
                }

                if (address.equals(FBUtilities.getBroadcastAddressAndPort()))
                {
                    // If localhost is a source, we have found one, but we don't add it to the map to avoid streaming locally
                    foundSource = true;
                    continue;
                }

                rangeFetchMapMap.put(address, range);
                foundSource = true;
                break; // ensure we only stream from one other node for each range
            }

            if (!foundSource)
            {
                AbstractReplicationStrategy strat = Keyspace.open(keyspace).getReplicationStrategy();
                if (strat != null && strat.getReplicationFactor() == 1)
                {
                    if (useStrictConsistency)
                        throw new IllegalStateException("Unable to find sufficient sources for streaming range " + range + " in keyspace " + keyspace + " with RF=1. " +
                                                        "Ensure this keyspace contains replicas in the source datacenter.");
                    else
                        logger.warn("Unable to find sufficient sources for streaming range {} in keyspace {} with RF=1. " +
                                    "Keyspace might be missing data.", range, keyspace);
                }
                else
                    throw new IllegalStateException("Unable to find sufficient sources for streaming range " + range + " in keyspace " + keyspace);
            }
        }

        return rangeFetchMapMap;
    }


    private static Multimap<InetAddressAndPort, Range<Token>> getOptimizedRangeFetchMap(Multimap<Range<Token>, InetAddressAndPort> rangesWithSources,
                                                                                        Collection<ISourceFilter> sourceFilters, String keyspace)
    {
        RangeFetchMapCalculator calculator = new RangeFetchMapCalculator(rangesWithSources, sourceFilters, keyspace);
        Multimap<InetAddressAndPort, Range<Token>> rangeFetchMapMap = calculator.getRangeFetchMap();
        logger.info("Output from RangeFetchMapCalculator for keyspace {}", keyspace);
        validateRangeFetchMap(rangesWithSources, rangeFetchMapMap, keyspace);
        return rangeFetchMapMap;
    }

    /**
     * Verify that source returned for each range is correct
     * @param rangesWithSources
     * @param rangeFetchMapMap
     * @param keyspace
     */
    private static void validateRangeFetchMap(Multimap<Range<Token>, InetAddressAndPort> rangesWithSources, Multimap<InetAddressAndPort, Range<Token>> rangeFetchMapMap, String keyspace)
    {
        for (Map.Entry<InetAddressAndPort, Range<Token>> entry : rangeFetchMapMap.entries())
        {
            if(entry.getKey().equals(FBUtilities.getBroadcastAddressAndPort()))
            {
                throw new IllegalStateException("Trying to stream locally. Range: " + entry.getValue()
                                        + " in keyspace " + keyspace);
            }

            if (!rangesWithSources.get(entry.getValue()).contains(entry.getKey()))
            {
                throw new IllegalStateException("Trying to stream from wrong endpoint. Range: " + entry.getValue()
                                                + " in keyspace " + keyspace + " from endpoint: " + entry.getKey());
            }

            logger.info("Streaming range {} from endpoint {} for keyspace {}", entry.getValue(), entry.getKey(), keyspace);
        }
    }

    public static Multimap<InetAddressAndPort, Range<Token>> getWorkMap(Multimap<Range<Token>, InetAddressAndPort> rangesWithSourceTarget, String keyspace,
                                                                        IFailureDetector fd, boolean useStrictConsistency)
    {
        return getRangeFetchMap(rangesWithSourceTarget, Collections.<ISourceFilter>singleton(new FailureDetectorSourceFilter(fd)), keyspace, useStrictConsistency);
    }

    @VisibleForTesting
    Multimap<String, Map.Entry<InetAddressAndPort, Collection<Range<Token>>>> toFetch()
    {
        return toFetch;
    }

    @VisibleForTesting
    StreamPlan createStreamPlan(boolean onlyUnrepaired)
    {
        StreamPlan streamPlan = new StreamPlan(streamOperation, connectionsPerHost, connectSequentially, null, PreviewKind.NONE, onlyUnrepaired);
        if (!onlyUnrepaired) // only resumable bootstrap for full streams
            streamPlan.listeners(stateStore);
        for (Map.Entry<String, Map.Entry<InetAddressAndPort, Collection<Range<Token>>>> entry : toFetch.entries())
        {
            String keyspace = entry.getKey();
            InetAddressAndPort source = entry.getValue().getKey();
            List<Range<Token>> ranges = entry.getValue().getValue().stream().filter(r -> (r instanceof UnrepairedRange) == onlyUnrepaired).collect(Collectors.toList());

            if (!onlyUnrepaired) // we currently only support resumable bootstrap for the full streams
            {
                // filter out already streamed ranges
                Set<Range<Token>> availableRanges = stateStore.getAvailableRanges(keyspace, StorageService.instance.getTokenMetadata().partitioner);
                if (ranges.removeAll(availableRanges))
                {
                    logger.info("Some ranges of {} are already available. Skipping streaming those ranges.", availableRanges);
                }
            }
            if (logger.isTraceEnabled())
                logger.trace("{}ing {} data from {} ranges {}", description, onlyUnrepaired ? "unrepaired" : "all", source, StringUtils.join(ranges, ", "));
            /* Send messages to respective folks to stream data over to me */
            if (ranges.size() > 0)
                streamPlan.requestRanges(source, keyspace, ranges);
        }
        return streamPlan;
    }

    private static ListenableFuture<StreamState> executeStreamPlan(StreamPlan streamPlan, StreamEventHandler handler)
    {
        if (streamPlan.isEmpty())
            return Futures.immediateFuture(null);
        if (logger.isDebugEnabled())
            logger.debug("[Stream #{}] Starting stream plan {}", streamPlan.planId, streamPlan);
        StreamResultFuture future = streamPlan.execute();
        if (handler != null)
            future.addEventListener(handler);
        return future;
    }

    public ListenableFuture<Set<StreamState>> fetchAsync(StreamEventHandler handler)
    {
        // reason to first fetch the unrepaired data is to avoid a small race where a repair finishes
        // and we miss a row that moved from unrepaired to repaired, for example:
        // * We have nodes A, B, C: A does not have row x
        // * A, B, C are repairing
        // * D starts bootstrap and streams everything from A (this misses row x since A doesn't have it)
        // * repair finishes, moves row x from unrepaired to repaired on A, B, C
        // * D streams unrepaired from B, misses x
        // by streaming the unrepaired first, we avoid this (if something jumps from unrepaired to repaired, we are
        // bound to get it when we stream everything since that means the repair finished)
        return FluentFuture.from(executeStreamPlan(createStreamPlan(true), handler))
                           .transform((streamState) -> createStreamState(streamState,
                                                                         executeStreamPlan(createStreamPlan(false), handler)),
                                      MoreExecutors.directExecutor());
    }

    private static Set<StreamState> createStreamState(@Nullable StreamState previousStreamState, ListenableFuture<StreamState> future)
    {
        try
        {
            StreamState streamState = future.get();
            if (previousStreamState != null && streamState != null)
                return Sets.newHashSet(previousStreamState, streamState);
            else if (previousStreamState != null)
                return Sets.newHashSet(previousStreamState);
            else
                return Sets.newHashSet(streamState);
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }
}
