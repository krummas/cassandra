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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.compaction.Verifier;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.Refs;

public class SSTableImporter
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyStore.class);

    private final ColumnFamilyStore cfs;

    public SSTableImporter(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
    }

    @VisibleForTesting
    synchronized void importNewSSTables(Options options)
    {
        logger.info("Loading new SSTables for {}/{}: {}",
                    cfs.keyspace.getName(), cfs.getTableName(), options);

        File dir = null;
        if (options.srcPath != null && !options.srcPath.isEmpty())
        {
            dir = new File(options.srcPath);
            if (!dir.exists())
            {
                throw new RuntimeException(String.format("Directory %s does not exist", options.srcPath));
            }
            if (!Directories.verifyFullPermissions(dir, options.srcPath))
            {
                throw new RuntimeException("Insufficient permissions on directory " + options.srcPath);
            }
        }

        Set<Descriptor> currentDescriptors = new HashSet<>();
        for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
            currentDescriptors.add(sstable.descriptor);
        Set<SSTableReader> newSSTables = new HashSet<>();
        Directories.SSTableLister lister = dir == null ?
                                           cfs.getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).skipTemporary(true) :
                                           cfs.getDirectories().sstableLister(dir, Directories.OnTxnErr.IGNORE).skipTemporary(true);

        // verify first to avoid starting to copy sstables to the data directories and then have to abort.
        if (options.verifySSTables || options.verifyTokens)
        {
            for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
            {
                Descriptor descriptor = entry.getKey();
                if (!currentDescriptors.contains(entry.getKey()))
                {
                    try
                    {
                        verifySSTableForImport(descriptor, entry.getValue(), options.verifyTokens, options.extendedVerify);
                    }
                    catch (Throwable t)
                    {
                        logger.error("Failed verifying sstable "+descriptor, t);
                        throw new RuntimeException("Failed verify sstable "+descriptor, t);
                    }
                }
            }
        }

        for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
        {
            SSTableReader sstable = importAndOpenSSTable(entry.getKey(), entry.getValue(), options);
            newSSTables.add(sstable);
        }

        if (newSSTables.isEmpty())
        {
            logger.info("No new SSTables were found for {}/{}", cfs.keyspace.getName(), cfs.getTableName());
            return;
        }

        logger.info("Loading new SSTables and building secondary indexes for {}/{}: {}", cfs.keyspace.getName(), cfs.getTableName(), newSSTables);

        try (Refs<SSTableReader> refs = Refs.ref(newSSTables))
        {
            cfs.getTracker().addSSTables(newSSTables);
        }

        logger.info("Done loading load new SSTables for {}/{}", cfs.keyspace.getName(), cfs.getTableName());
    }

    /**
     * Iterates over all keys in the sstable index and invalidates the row cache
     *
     * also counts the number of tokens that should be on each disk in JBOD-config to minimize the amount of data compaction
     * needs to move around
     */
    @VisibleForTesting
    static File findBestDiskAndInvalidateCaches(ColumnFamilyStore cfs, Descriptor desc, String srcPath, boolean clearCaches, boolean jbodCheck) throws IOException
    {
        int boundaryIndex = 0;
        DiskBoundaries boundaries = cfs.getDiskBoundaries();
        boolean shouldCountKeys = boundaries.positions != null && jbodCheck;
        if (!cfs.isRowCacheEnabled() || !clearCaches)
        {
            if (srcPath == null) // user has dropped the sstables in the data directory, use it directly
                return desc.directory;
            if (boundaries.directories != null && boundaries.directories.size() == 1) // only a single data directory, use it without counting keys
                return cfs.getDirectories().getLocationForDisk(boundaries.directories.get(0));
            if (!shouldCountKeys) // for non-random partitioners positions can be null, get the directory with the most space available
                return cfs.getDirectories().getWriteableLocationToLoadFile(new File(desc.baseFilename()));
        }

        long count = 0;
        int maxIndex = 0;
        long maxCount = 0;

        try (KeyIterator iter = new KeyIterator(desc, cfs.metadata()))
        {
            while (iter.hasNext())
            {
                DecoratedKey decoratedKey = iter.next();
                if (clearCaches)
                    cfs.invalidateCachedPartition(decoratedKey);
                if (shouldCountKeys)
                {
                    while (boundaries.positions.get(boundaryIndex).compareTo(decoratedKey) < 0)
                    {
                        logger.debug("{} has {} keys in {}", desc, count, boundaries.positions.get(boundaryIndex));
                        if (count > maxCount)
                        {
                            maxIndex = boundaryIndex;
                            maxCount = count;
                        }
                        boundaryIndex++;
                        count = 0;
                    }
                    count++;
                }
            }
            if (shouldCountKeys)
            {
                if (count > maxCount)
                    maxIndex = boundaryIndex;
                logger.debug("{} has {} keys in {}", desc, count, boundaries.positions.get(boundaryIndex));
            }
        }
        File dir;
        if (srcPath == null)
            dir = desc.directory;
        else if (shouldCountKeys)
            dir = cfs.getDirectories().getLocationForDisk(boundaries.directories.get(maxIndex));
        else
            dir = cfs.getDirectories().getWriteableLocationToLoadFile(new File(desc.baseFilename()));
        return dir;
    }

    /**
     * verifies the sstables returned by the SSTableLister given
     *
     * @param lister         the sstables to verify
     * @param verifyTokens   if we should verify that the tokens are owned by the current node
     * @param extendedVerify if an extended verify should be done - this verifies each value in the sstables
     */
    private void verifySSTableForImport(Descriptor descriptor, Set<Component> components, boolean verifyTokens, boolean extendedVerify)
    {
        SSTableReader reader = null;
        try
        {
            reader = SSTableReader.open(descriptor, components, cfs.metadata);
            Verifier.Options verifierOptions = Verifier.options()
                                                       .extendedVerification(extendedVerify)
                                                       .checkOwnsTokens(verifyTokens)
                                                       .invokeDiskFailurePolicy(false)
                                                       .mutateRepairStatus(false).build();
            try (Verifier verifier = new Verifier(cfs, reader, false, verifierOptions))
            {
                verifier.verify();
            }
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Can't import sstable " + descriptor, t);
        }
        finally
        {
            if (reader != null)
                reader.selfRef().release();
        }
    }

    private SSTableReader importAndOpenSSTable(Descriptor descriptor, Set<Component> components, Options options)
    {
        if (!descriptor.isCompatible())
            throw new RuntimeException(String.format("Can't open incompatible SSTable! Current version %s, found file: %s",
                                                     descriptor.getFormat().getLatestVersion(),
                                                     descriptor));

        File targetDirectory;
        try
        {
            if (new File(descriptor.filenameFor(Component.STATS)).exists())
            {
                if (options.resetLevel)
                {
                    descriptor.getMetadataSerializer().mutateLevel(descriptor, 0);
                }
                if (options.clearRepaired)
                {
                    descriptor.getMetadataSerializer().mutateRepaired(descriptor,
                                                                      ActiveRepairService.UNREPAIRED_SSTABLE,
                                                                      null);
                }
            }
            targetDirectory = findBestDiskAndInvalidateCaches(cfs, descriptor, options.srcPath, options.invalidateCaches, options.jbodCheck);
            logger.debug("{} will get copied to {}", descriptor, targetDirectory);
        }
        catch (IOException e)
        {
            logger.error("{} is corrupt, can't import", descriptor, e);
            throw new RuntimeException(e);
        }

        Descriptor newDescriptor = cfs.getUniqueDescriptorFor(descriptor, targetDirectory);

        logger.info("Renaming new SSTable {} to {}", descriptor, newDescriptor);
        SSTableWriter.rename(descriptor, newDescriptor, components);

        SSTableReader reader;
        try
        {
            reader = SSTableReader.open(newDescriptor, components, cfs.metadata);
        }
        catch (Throwable t)
        {
            logger.error("Aborting import of sstables. {} was corrupt", newDescriptor);
            throw new RuntimeException(newDescriptor + " is corrupt, can't import", t);
        }
        return reader;
    }



    public static class Options
    {
        public final String srcPath;
        public final boolean resetLevel;
        public final boolean clearRepaired;
        public final boolean verifySSTables;
        public final boolean verifyTokens;
        public final boolean invalidateCaches;
        public final boolean jbodCheck;
        public final boolean extendedVerify;

        public Options(String srcPath, boolean resetLevel, boolean clearRepaired, boolean verifySSTables, boolean verifyTokens, boolean invalidateCaches, boolean jbodCheck, boolean extendedVerify)
        {
            this.srcPath = srcPath;
            this.resetLevel = resetLevel;
            this.clearRepaired = clearRepaired;
            this.verifySSTables = verifySSTables;
            this.verifyTokens = verifyTokens;
            this.invalidateCaches = invalidateCaches;
            this.jbodCheck = jbodCheck;
            this.extendedVerify = extendedVerify;
        }

        public static Builder options(@Nullable String srcDir)
        {
            return new Builder(srcDir);
        }

        public static Builder options()
        {
            return options(null);
        }

        @Override
        public String toString()
        {
            return "Options{" +
                   "srcPath='" + srcPath + '\'' +
                   ", resetLevel=" + resetLevel +
                   ", clearRepaired=" + clearRepaired +
                   ", verifySSTables=" + verifySSTables +
                   ", verifyTokens=" + verifyTokens +
                   ", invalidateCaches=" + invalidateCaches +
                   ", extendedVerify=" + extendedVerify +
                   '}';
        }

        static class Builder
        {
            private final String srcPath;
            private boolean resetLevel = false;
            private boolean clearRepaired = false;
            private boolean verifySSTables = false;
            private boolean verifyTokens = false;
            private boolean invalidateCaches = false;
            private boolean jbodCheck = false;
            private boolean extendedVerify = false;

            private Builder(String srcPath)
            {
                this.srcPath = srcPath;
            }

            public Builder resetLevel(boolean value)
            {
                resetLevel = value;
                return this;
            }

            public Builder clearRepaired(boolean value)
            {
                clearRepaired = value;
                return this;
            }

            public Builder verifySSTables(boolean value)
            {
                verifySSTables = value;
                return this;
            }

            public Builder verifyTokens(boolean value)
            {
                verifyTokens = value;
                return this;
            }

            public Builder invalidateCaches(boolean value)
            {
                invalidateCaches = value;
                return this;
            }

            public Builder jbodCheck(boolean value)
            {
                jbodCheck = value;
                return this;
            }

            public Builder extendedVerify(boolean value)
            {
                extendedVerify = value;
                return this;
            }

            public Options build()
            {
                return new Options(srcPath, resetLevel, clearRepaired, verifySSTables, verifyTokens, invalidateCaches, jbodCheck, extendedVerify);
            }
        }
    }

}
