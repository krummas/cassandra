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

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class DiskBoundaryManager
{
    private static final Logger logger = LoggerFactory.getLogger(DiskBoundaryManager.class);
    private volatile DiskBoundaries diskBoundaries;

    public DiskBoundaries getDiskBoundaries(ColumnFamilyStore cfs)
    {
        // copy the reference to avoid getting nulled out by invalidate() below
        // - it is ok to race at times, compaction will move any incorrect tokens to their correct places, but
        // returning null would be bad
        DiskBoundaries db = diskBoundaries;
        if (isOutOfDate(diskBoundaries))
        {
            synchronized (this)
            {
                db = diskBoundaries;
                if (isOutOfDate(diskBoundaries))
                {
                    logger.debug("Refreshing disk boundary cache for {}.{}", cfs.keyspace.getName(), cfs.getTableName());
                    db = diskBoundaries = getDiskBoundaryValue(cfs);
                }
            }
        }
        return db;
    }

    /**
     * check if the given disk boundaries are out of date due not being set or to having too old diskVersion or ringVersion
     */
    private boolean isOutOfDate(DiskBoundaries db)
    {
        if (db == null)
            return true;
        long currentRingVersion = StorageService.instance.getTokenMetadata().getRingVersion();
        int currentDiskVersion = BlacklistedDirectories.getDiskVersion();
        return currentRingVersion != db.ringVersion || currentDiskVersion != db.diskVersion;
    }

    private DiskBoundaries getDiskBoundaryValue(ColumnFamilyStore cfs)
    {
        Collection<Range<Token>> lr;

        long ringVersion;
        TokenMetadata tmd;
        do
        {
            tmd = StorageService.instance.getTokenMetadata();
            ringVersion = tmd.getRingVersion();
            if (StorageService.instance.isBootstrapMode())
            {
                lr = tmd.getPendingRanges(cfs.keyspace.getName(), FBUtilities.getBroadcastAddress());
            }
            else
            {
                // Reason we use use the future settled TMD is that if we decommission a node, we want to stream
                // from that node to the correct location on disk, if we didn't, we would put new files in the wrong places.
                // We do this to minimize the amount of data we need to move in rebalancedisks once everything settled
                lr = cfs.keyspace.getReplicationStrategy().getAddressRanges(tmd.cloneAfterAllLeft()).get(FBUtilities.getBroadcastAddress());
            }
            logger.debug("Got local ranges {} (ringVersion = {})", lr, ringVersion);
        }
        while (ringVersion != tmd.getRingVersion()); // if ringVersion is different here it means that
                                                     // it might have changed before we calculated lr - recalculate

        int diskVersion = -1;
        Directories.DataDirectory[] dirs = null;
        while (diskVersion != BlacklistedDirectories.getDiskVersion())
        {
            dirs = cfs.getDirectories().getWriteableLocations();
            diskVersion = BlacklistedDirectories.getDiskVersion();
        }

        if (lr == null || lr.isEmpty())
            return new DiskBoundaries(dirs, null, ringVersion, diskVersion);

        List<Range<Token>> localRanges = Range.sort(lr);

        List<PartitionPosition> positions = getDiskBoundaries(localRanges, cfs.getPartitioner(), dirs);
        return new DiskBoundaries(dirs, positions, ringVersion, diskVersion);
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

    public void invalidate()
    {
        diskBoundaries = null;
    }

    public static class DiskBoundaries
    {
        public final Directories.DataDirectory[] directories;
        public final ImmutableList<PartitionPosition> positions;
        private final long ringVersion;
        private final int diskVersion;

        private DiskBoundaries(Directories.DataDirectory[] directories, List<PartitionPosition> positions, long ringVersion, int diskVersion)
        {
            this.directories = directories;
            this.positions = positions == null ? null : ImmutableList.copyOf(positions);
            this.ringVersion = ringVersion;
            this.diskVersion = diskVersion;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DiskBoundaries that = (DiskBoundaries) o;

            if (ringVersion != that.ringVersion) return false;
            if (diskVersion != that.diskVersion) return false;
            if (!Arrays.equals(directories, that.directories)) return false;
            return positions != null ? positions.equals(that.positions) : that.positions == null;
        }

        public int hashCode()
        {
            int result = Arrays.hashCode(directories);
            result = 31 * result + (positions != null ? positions.hashCode() : 0);
            result = 31 * result + (int) (ringVersion ^ (ringVersion >>> 32));
            result = 31 * result + diskVersion;
            return result;
        }
    }
}
