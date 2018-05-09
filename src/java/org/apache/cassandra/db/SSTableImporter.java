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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Refs;

public class SSTableImporter
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnFamilyStore.class);

    private final ColumnFamilyStore cfs;

    public SSTableImporter(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
    }

    /**
     * Imports sstables from the directories given in options.srcPaths
     *
     * If import fails in any of the directories, that directory is skipped and the failed directories
     * are returned so that the user can re-upload files or remove corrupt files.
     *
     * If one of the directories in srcPaths is not readable/does not exist, we exit immediately to let
     * the user change permissions or similar on the directory.
     *
     * @param options
     * @return list of failed directories
     */
    @VisibleForTesting
    synchronized List<String> importNewSSTables(Options options)
    {
        logger.info("Loading new SSTables for {}/{}: {}",
                    cfs.keyspace.getName(), cfs.getTableName(), options);

        List<Pair<Directories.SSTableLister, String>> listers = getSSTableListers(options.srcPaths);

        Set<Descriptor> currentDescriptors = new HashSet<>();
        for (SSTableReader sstable : cfs.getSSTables(SSTableSet.CANONICAL))
            currentDescriptors.add(sstable.descriptor);
        List<String> failedDirectories = new ArrayList<>();

        // verify first to avoid starting to copy sstables to the data directories and then have to abort.
        if (options.verifySSTables || options.verifyTokens)
        {
            for (Pair<Directories.SSTableLister, String> listerPair : listers)
            {
                Directories.SSTableLister lister = listerPair.left;
                String dir = listerPair.right;
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
                            if (dir != null)
                            {
                                logger.error("Failed verifying sstable {} in directory {}", descriptor, dir, t);
                                failedDirectories.add(dir);
                            }
                            else
                            {
                                logger.error("Failed verifying sstable {}", descriptor, t);
                                throw new RuntimeException("Failed verifying sstable "+descriptor, t);
                            }
                            break;
                        }
                    }
                }
            }
        }

        Set<SSTableReader> newSSTables = new HashSet<>();
        for (Pair<Directories.SSTableLister, String> listerPair : listers)
        {
            Directories.SSTableLister lister = listerPair.left;
            String dir = listerPair.right;
            if (failedDirectories.contains(dir))
                continue;

            Set<MovedSSTable> movedSSTables = new HashSet<>();
            Set<SSTableReader> newSSTablesPerDirectory = new HashSet<>();
            for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet())
            {
                try
                {
                    Descriptor oldDescriptor = entry.getKey();
                    if (currentDescriptors.contains(oldDescriptor))
                        continue;
                    File targetDir = findBestDiskAndInvalidateCaches(oldDescriptor, dir, options.invalidateCaches, options.jbodCheck);
                    Descriptor newDescriptor = cfs.getUniqueDescriptorFor(entry.getKey(), targetDir);
                    maybeMutateMetadata(entry.getKey(), options);
                    movedSSTables.add(new MovedSSTable(newDescriptor, entry.getKey(), entry.getValue()));
                    SSTableReader sstable = moveAndOpenSSTable(entry.getKey(), newDescriptor, entry.getValue());
                    newSSTablesPerDirectory.add(sstable);
                }
                catch (Throwable t)
                {
                    newSSTablesPerDirectory.forEach(s -> s.selfRef().release());
                    if (dir != null)
                    {
                        logger.error("Failed importing sstables in directory {}", dir, t);
                        failedDirectories.add(dir);
                        moveSSTablesBack(movedSSTables);
                        movedSSTables.clear();
                        newSSTablesPerDirectory.clear();
                        break;
                    }
                    else
                    {
                        logger.error("Failed importing sstables from data directory - renamed sstables are: {}", movedSSTables);
                        throw new RuntimeException("Failed importing sstables", t);
                    }
                }
            }
            newSSTables.addAll(newSSTablesPerDirectory);
        }

        if (newSSTables.isEmpty())
        {
            logger.info("No new SSTables were found for {}/{}", cfs.keyspace.getName(), cfs.getTableName());
            return failedDirectories;
        }

        logger.info("Loading new SSTables and building secondary indexes for {}/{}: {}", cfs.keyspace.getName(), cfs.getTableName(), newSSTables);

        try (Refs<SSTableReader> refs = Refs.ref(newSSTables))
        {
            cfs.getTracker().addSSTables(newSSTables);
        }

        logger.info("Done loading load new SSTables for {}/{}", cfs.keyspace.getName(), cfs.getTableName());
        return failedDirectories;
    }

    /**
     * Create SSTableListers based on srcPaths
     *
     * If srcPaths is empty, we create a lister that lists sstables in the data directories (deprecated use)
     */
    private List<Pair<Directories.SSTableLister, String>> getSSTableListers(Set<String> srcPaths)
    {
        List<Pair<Directories.SSTableLister, String>> listers = new ArrayList<>();

        if (!srcPaths.isEmpty())
        {
            for (String path : srcPaths)
            {
                File dir = new File(path);
                if (!dir.exists())
                {
                    throw new RuntimeException(String.format("Directory %s does not exist", path));
                }
                if (!Directories.verifyFullPermissions(dir, path))
                {
                    throw new RuntimeException("Insufficient permissions on directory " + path);
                }
                listers.add(Pair.create(cfs.getDirectories().sstableLister(dir, Directories.OnTxnErr.IGNORE).skipTemporary(true), path));
            }
        }
        else
        {
            listers.add(Pair.create(cfs.getDirectories().sstableLister(Directories.OnTxnErr.IGNORE).skipTemporary(true), null));
        }

        return listers;
    }

    private static class MovedSSTable
    {
        private final Descriptor newDescriptor;
        private final Descriptor oldDescriptor;
        private final Set<Component> components;

        private MovedSSTable(Descriptor newDescriptor, Descriptor oldDescriptor, Set<Component> components)
        {
            this.newDescriptor = newDescriptor;
            this.oldDescriptor = oldDescriptor;
            this.components = components;
        }

        public String toString()
        {
            return String.format("%s moved to %s with components %s", oldDescriptor, newDescriptor, components);
        }
    }

    /**
     * If we fail when opening the sstable (if for example the user passes in --no-verify and there are corrupt sstables)
     * we might have started copying sstables to the data directory, these need to be moved back to the original name/directory
     */
    private void moveSSTablesBack(Set<MovedSSTable> movedSSTables)
    {
        for (MovedSSTable movedSSTable : movedSSTables)
        {
            if (new File(movedSSTable.newDescriptor.filenameFor(Component.DATA)).exists())
            {
                logger.debug("Moving sstable {} back to {}", movedSSTable.newDescriptor.filenameFor(Component.DATA)
                                                          , movedSSTable.oldDescriptor.filenameFor(Component.DATA));
                SSTableWriter.rename(movedSSTable.newDescriptor, movedSSTable.oldDescriptor, movedSSTable.components);
            }
        }
    }

    /**
     * Iterates over all keys in the sstable index and invalidates the row cache
     *
     * also counts the number of tokens that should be on each disk in JBOD-config to minimize the amount of data compaction
     * needs to move around
     */
    @VisibleForTesting
    File findBestDiskAndInvalidateCaches(Descriptor desc, String srcPath, boolean clearCaches, boolean jbodCheck) throws IOException
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
     * Verify an sstable for import, throws exception if there is a failure verifying.
     *
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

    /**
     * Depending on the options passed in, this might reset level on the sstable to 0 and/or remove the repair information
     * from the sstable
     */
    private void maybeMutateMetadata(Descriptor descriptor, Options options) throws IOException
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
    }

    /**
     * Moves the sstable in oldDescriptor to its new place (with generation etc) in newDescriptor.
     *
     * All components given will be moved/renamed
     */
    private SSTableReader moveAndOpenSSTable(Descriptor oldDescriptor, Descriptor newDescriptor, Set<Component> components)
    {
        if (!oldDescriptor.isCompatible())
            throw new RuntimeException(String.format("Can't open incompatible SSTable! Current version %s, found file: %s",
                                                     oldDescriptor.getFormat().getLatestVersion(),
                                                     oldDescriptor));

        logger.info("Renaming new SSTable {} to {}", oldDescriptor, newDescriptor);
        SSTableWriter.rename(oldDescriptor, newDescriptor, components);

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
        private final Set<String> srcPaths;
        private final boolean resetLevel;
        private final boolean clearRepaired;
        private final boolean verifySSTables;
        private final boolean verifyTokens;
        private final boolean invalidateCaches;
        private final boolean jbodCheck;
        private final boolean extendedVerify;

        public Options(Set<String> srcPaths, boolean resetLevel, boolean clearRepaired, boolean verifySSTables, boolean verifyTokens, boolean invalidateCaches, boolean jbodCheck, boolean extendedVerify)
        {
            this.srcPaths = srcPaths;
            this.resetLevel = resetLevel;
            this.clearRepaired = clearRepaired;
            this.verifySSTables = verifySSTables;
            this.verifyTokens = verifyTokens;
            this.invalidateCaches = invalidateCaches;
            this.jbodCheck = jbodCheck;
            this.extendedVerify = extendedVerify;
        }

        public static Builder options(String srcDir)
        {
            return new Builder(Collections.singleton(srcDir));
        }

        public static Builder options(Set<String> srcDirs)
        {
            return new Builder(srcDirs);
        }

        public static Builder options()
        {
            return options(Collections.emptySet());
        }

        @Override
        public String toString()
        {
            return "Options{" +
                   "srcPaths='" + srcPaths + '\'' +
                   ", resetLevel=" + resetLevel +
                   ", clearRepaired=" + clearRepaired +
                   ", verifySSTables=" + verifySSTables +
                   ", verifyTokens=" + verifyTokens +
                   ", invalidateCaches=" + invalidateCaches +
                   ", jbodCheck = "+ jbodCheck +
                   ", extendedVerify=" + extendedVerify +
                   '}';
        }

        static class Builder
        {
            private final Set<String> srcPaths;
            private boolean resetLevel = false;
            private boolean clearRepaired = false;
            private boolean verifySSTables = false;
            private boolean verifyTokens = false;
            private boolean invalidateCaches = false;
            private boolean jbodCheck = false;
            private boolean extendedVerify = false;

            private Builder(Set<String> srcPath)
            {
                assert srcPath != null;
                this.srcPaths = srcPath;
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
                return new Options(srcPaths, resetLevel, clearRepaired, verifySSTables, verifyTokens, invalidateCaches, jbodCheck, extendedVerify);
            }
        }
    }

}
