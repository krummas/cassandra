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

package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;

public class DiskBoundaryManager
{
    private static final Logger logger = LoggerFactory.getLogger(DiskBoundaryManager.class);
    private static final NoSpamLogger noSpamMinute = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    private final Map<UUID, DiskBoundaryValue> cachedBoundaries = new ConcurrentHashMap<>();
    private volatile long cachedRingVersion = -1;

    public DiskBoundaryValue getDiskBoundaries(ColumnFamilyStore cfs, Directories dirs)
    {
        Directories.DataDirectory[] directories = cfs.getDirectories().getWriteableLocations();
        if (!cfs.getPartitioner().splitter().isPresent())
            return new DiskBoundaryValue(directories, null);
        // we don't support caching with custom sstable directories from compaction strategies
        if (!Arrays.equals(dirs.getWriteableLocations(), directories))
        {
            noSpamMinute.info("Caching disk boundaries with custom compaction strategies is not supported, {} != {}", Arrays.toString(dirs.getWriteableLocations()), Arrays.toString(directories));
            return getDiskBoundaryValue(cfs, dirs.getWriteableLocations());
        }

        long currentRingVersion = StorageService.instance.getTokenMetadata().getRingVersion();
        if (cachedRingVersion != currentRingVersion)
        {
            logger.debug("Cached ring version {} != current ring version {}", cachedRingVersion, currentRingVersion);
            cachedBoundaries.clear();
            cachedRingVersion = currentRingVersion;
        }

        DiskBoundaryValue dbv = cachedBoundaries.get(cfs.metadata.cfId);
        if (dbv != null)
            return dbv;

        synchronized (this)
        {
            dbv = cachedBoundaries.get(cfs.metadata.cfId);
            if (dbv != null)
                return dbv;
            logger.debug("Refreshing disk boundary cache for {}.{}", cfs.keyspace.getName(), cfs.getTableName());
            dbv = getDiskBoundaryValue(cfs, directories);
            cachedBoundaries.put(cfs.metadata.cfId, dbv);
            return dbv;
        }
    }

    private DiskBoundaryValue getDiskBoundaryValue(ColumnFamilyStore cfs, Directories.DataDirectory[] directories)
    {
        Collection<Range<Token>> lr;

        if (StorageService.instance.isBootstrapMode())
        {
            lr = StorageService.instance.getTokenMetadata().getPendingRanges(cfs.keyspace.getName(), FBUtilities.getBroadcastAddress());
        }
        else
        {
            // Reason we use use the future settled TMD is that if we decommission a node, we want to stream
            // from that node to the correct location on disk, if we didn't, we would put new files in the wrong places.
            // We do this to minimize the amount of data we need to move in rebalancedisks once everything settled
            TokenMetadata tmd = StorageService.instance.getTokenMetadata().cloneAfterAllSettled();
            lr = cfs.keyspace.getReplicationStrategy().getAddressRanges(tmd).get(FBUtilities.getBroadcastAddress());
        }

        if (lr == null || lr.isEmpty())
            return new DiskBoundaryValue(directories, null);

        List<Range<Token>> localRanges = Range.sort(lr);

        List<PartitionPosition> positions = getDiskBoundaries(localRanges, cfs.getPartitioner(), directories);
        return new DiskBoundaryValue(directories, positions);
    }

    /**
     * Returns a list of disk boundaries, the result will differ depending on whether vnodes are enabled or not.
     *
     * What is returned are upper bounds for the disks, meaning everything from partitioner.minToken up to
     * getDiskBoundaries(..).get(0) should be on the first disk, everything between 0 to 1 should be on the second disk
     * etc.
     *
     * The final entry in the returned list will always be the partitioner maximum tokens upper key bound
     */
    private List<PartitionPosition> getDiskBoundaries(List<Range<Token>> localRanges, IPartitioner partitioner, Directories.DataDirectory[] dataDirectories)
    {
        assert partitioner.splitter().isPresent();
        Splitter splitter = partitioner.splitter().get();
        boolean dontSplitRanges = DatabaseDescriptor.getNumTokens() > 1;
        List<Token> boundaries = splitter.splitOwnedRanges(dataDirectories.length, localRanges, dontSplitRanges);
        // If we can't split by ranges, split evenly to ensure utilisation of all disks
        if (dontSplitRanges && boundaries.size() < dataDirectories.length)
            boundaries = splitter.splitOwnedRanges(dataDirectories.length, localRanges, false);

        List<PartitionPosition> diskBoundaries = new ArrayList<>();
        for (int i = 0; i < boundaries.size() - 1; i++)
            diskBoundaries.add(boundaries.get(i).maxKeyBound());
        diskBoundaries.add(partitioner.getMaximumToken().maxKeyBound());
        return diskBoundaries;
    }

    public void invalidateCache()
    {
        cachedRingVersion = -1;
    }

    /**
     * Contains the boundaries for a keyspace with the data directories used to create those boundaries
     */
    public static class DiskBoundaryValue
    {
        private final Directories.DataDirectory[] directories;
        private final List<PartitionPosition> positions;
        private DiskBoundaryValue(Directories.DataDirectory[] directories, List<PartitionPosition> positions)
        {
            this.directories = directories;
            this.positions = positions;
        }

        public Directories.DataDirectory[] getDirectories()
        {
            return Arrays.copyOf(directories, directories.length);
        }

        public List<PartitionPosition> getPositions()
        {
            if (positions == null)
                return null;
            return ImmutableList.copyOf(positions);
        }
    }
}
