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
package org.apache.cassandra.db.compaction;

import com.google.common.base.Throwables;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.SSTableIdentityIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.DataIntegrityMetadata;
import org.apache.cassandra.io.util.DataIntegrityMetadata.FileDigestValidator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.UUIDGen;

import java.io.Closeable;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Predicate;

public class Verifier implements Closeable
{
    private final ColumnFamilyStore cfs;
    private final SSTableReader sstable;

    private final CompactionController controller;


    private final RandomAccessReader dataFile;
    private final RandomAccessReader indexFile;
    private final VerifyInfo verifyInfo;
    private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;
    private final OptionHolder options;

    private int goodRows;

    private final OutputHandler outputHandler;
    private FileDigestValidator validator;

    public Verifier(ColumnFamilyStore cfs, SSTableReader sstable, boolean isOffline, OptionHolder options)
    {
        this(cfs, sstable, new OutputHandler.LogOutput(), isOffline, options);
    }

    public Verifier(ColumnFamilyStore cfs, SSTableReader sstable, OutputHandler outputHandler, boolean isOffline, OptionHolder options)
    {
        this.cfs = cfs;
        this.sstable = sstable;
        this.outputHandler = outputHandler;
        this.rowIndexEntrySerializer = sstable.descriptor.version.getSSTableFormat().getIndexSerializer(sstable.metadata, sstable.descriptor.version, sstable.header);

        this.controller = new VerifyController(cfs);

        this.dataFile = isOffline
                        ? sstable.openDataReader()
                        : sstable.openDataReader(CompactionManager.instance.getRateLimiter());
        this.indexFile = RandomAccessReader.open(new File(sstable.descriptor.filenameFor(Component.PRIMARY_INDEX)));
        this.verifyInfo = new VerifyInfo(dataFile, sstable);
        this.options = options;
    }

    public void verify() throws IOException
    {
        boolean extended = options.extendedVerification;
        long rowStart = 0;

        outputHandler.output(String.format("Verifying %s (%s)", sstable, FBUtilities.prettyPrintMemory(dataFile.length())));

        if (options.checkVersion && !sstable.descriptor.version.isLatestVersion())
        {
            String msg = String.format("%s is not the latest version, run upgradesstables", sstable);
            outputHandler.output(msg);
            // don't use markAndThrow here because we don't want a CorruptSSTableException for this.
            throw new RuntimeException(msg);
        }

        outputHandler.output(String.format("Deserializing sstable metadata for %s ", sstable));
        try
        {
            EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);
            Map<MetadataType, MetadataComponent> sstableMetadata = sstable.descriptor.getMetadataSerializer().deserialize(sstable.descriptor, types);
            if (sstableMetadata.containsKey(MetadataType.VALIDATION) &&
                !((ValidationMetadata)sstableMetadata.get(MetadataType.VALIDATION)).partitioner.equals(sstable.getPartitioner().getClass().getCanonicalName()))
                throw new IOException("Partitioner does not match validation metadata");
        }
        catch (Throwable t)
        {
            outputHandler.debug(t.getMessage());
            markAndThrow(false);
        }
        outputHandler.output(String.format("Checking computed hash of %s ", sstable));


        // Verify will use the Digest files, which works for both compressed and uncompressed sstables
        try
        {
            validator = null;

            if (sstable.descriptor.digestComponent != null &&
                new File(sstable.descriptor.filenameFor(sstable.descriptor.digestComponent)).exists())
            {
                validator = DataIntegrityMetadata.fileDigestValidator(sstable.descriptor);
                validator.validate();
            }
            else
            {
                outputHandler.output("Data digest missing, assuming extended verification of disk values");
                extended = true;
            }
        }
        catch (IOException e)
        {
            outputHandler.debug(e.getMessage());
            markAndThrow();
        }
        finally
        {
            FileUtils.closeQuietly(validator);
        }

        if ( !extended )
            return;

        outputHandler.output("Extended Verify requested, proceeding to inspect values");


        try
        {
            ByteBuffer nextIndexKey = ByteBufferUtil.readWithShortLength(indexFile);
            {
                long firstRowPositionFromIndex = rowIndexEntrySerializer.deserializePositionAndSkip(indexFile);
                if (firstRowPositionFromIndex != 0)
                    markAndThrow();
            }

            DecoratedKey prevKey = null;

            while (!dataFile.isEOF())
            {

                if (verifyInfo.isStopRequested())
                    throw new CompactionInterruptedException(verifyInfo.getCompactionInfo());

                rowStart = dataFile.getFilePointer();
                outputHandler.debug("Reading row at " + rowStart);

                DecoratedKey key = null;
                try
                {
                    key = sstable.decorateKey(ByteBufferUtil.readWithShortLength(dataFile));
                }
                catch (Throwable th)
                {
                    throwIfFatal(th);
                    // check for null key below
                }

                ByteBuffer currentIndexKey = nextIndexKey;
                long nextRowPositionFromIndex = 0;
                try
                {
                    nextIndexKey = indexFile.isEOF() ? null : ByteBufferUtil.readWithShortLength(indexFile);
                    nextRowPositionFromIndex = indexFile.isEOF()
                                             ? dataFile.length()
                                             : rowIndexEntrySerializer.deserializePositionAndSkip(indexFile);
                }
                catch (Throwable th)
                {
                    markAndThrow();
                }

                long dataStart = dataFile.getFilePointer();
                long dataStartFromIndex = currentIndexKey == null
                                        ? -1
                                        : rowStart + 2 + currentIndexKey.remaining();

                long dataSize = nextRowPositionFromIndex - dataStartFromIndex;
                // avoid an NPE if key is null
                String keyName = key == null ? "(unreadable key)" : ByteBufferUtil.bytesToHex(key.getKey());
                outputHandler.debug(String.format("row %s is %s", keyName, FBUtilities.prettyPrintMemory(dataSize)));

                assert currentIndexKey != null || indexFile.isEOF();

                try
                {
                    if (key == null || dataSize > dataFile.length())
                        markAndThrow();

                    //mimic the scrub read path, intentionally unused
                    try (UnfilteredRowIterator iterator = SSTableIdentityIterator.create(sstable, dataFile, key))
                    {
                    }

                    if ( (prevKey != null && prevKey.compareTo(key) > 0) || !key.getKey().equals(currentIndexKey) || dataStart != dataStartFromIndex )
                        markAndThrow();
                    
                    goodRows++;
                    prevKey = key;


                    outputHandler.debug(String.format("Row %s at %s valid, moving to next row at %s ", goodRows, rowStart, nextRowPositionFromIndex));
                    dataFile.seek(nextRowPositionFromIndex);
                }
                catch (Throwable th)
                {
                    markAndThrow();
                }
            }
        }
        catch (Throwable t)
        {
            throw Throwables.propagate(t);
        }
        finally
        {
            controller.close();
        }

        outputHandler.output("Verify of " + sstable + " succeeded. All " + goodRows + " rows read successfully");
    }

    public void close()
    {
        FileUtils.closeQuietly(dataFile);
        FileUtils.closeQuietly(indexFile);
    }

    private void throwIfFatal(Throwable th)
    {
        if (th instanceof Error && !(th instanceof AssertionError || th instanceof IOError))
            throw (Error) th;
    }

    private void markAndThrow() throws IOException
    {
        markAndThrow(true);
    }

    private void markAndThrow(boolean mutateRepaired) throws IOException
    {
        if (mutateRepaired && options.mutateRepairStatus) // if we are able to mutate repaired flag, an incremental repair should be enough
        {
            sstable.descriptor.getMetadataSerializer().mutateRepairedAt(sstable.descriptor, ActiveRepairService.UNREPAIRED_SSTABLE);
            sstable.reloadSSTableMetadata();
            cfs.getTracker().notifySSTableRepairedStatusChanged(Collections.singleton(sstable));
        }
        Exception e = new Exception(String.format("Invalid SSTable %s, please force %srepair", sstable.getFilename(), (mutateRepaired && options.mutateRepairStatus) ? "" : "a full "));
        if (options.invokeDiskFailurePolicy)
            throw new CorruptSSTableException(e, sstable.getFilename());
        else
            throw new RuntimeException(e);
    }

    public CompactionInfo.Holder getVerifyInfo()
    {
        return verifyInfo;
    }

    private static class VerifyInfo extends CompactionInfo.Holder
    {
        private final RandomAccessReader dataFile;
        private final SSTableReader sstable;
        private final UUID verificationCompactionId;

        public VerifyInfo(RandomAccessReader dataFile, SSTableReader sstable)
        {
            this.dataFile = dataFile;
            this.sstable = sstable;
            verificationCompactionId = UUIDGen.getTimeUUID();
        }

        public CompactionInfo getCompactionInfo()
        {
            try
            {
                return new CompactionInfo(sstable.metadata,
                                          OperationType.VERIFY,
                                          dataFile.getFilePointer(),
                                          dataFile.length(),
                                          verificationCompactionId);
            }
            catch (Exception e)
            {
                throw new RuntimeException();
            }
        }
    }

    private static class VerifyController extends CompactionController
    {
        public VerifyController(ColumnFamilyStore cfs)
        {
            super(cfs, Integer.MAX_VALUE);
        }

        @Override
        public Predicate<Long> getPurgeEvaluator(DecoratedKey key)
        {
            return time -> false;
        }
    }

    public static class OptionHolder
    {
        public final boolean invokeDiskFailurePolicy;
        public final boolean extendedVerification;
        public final boolean checkVersion;
        private final boolean mutateRepairStatus;

        public OptionHolder(boolean invokeDiskFailurePolicy, boolean extendedVerification, boolean checkVersion, boolean mutateRepairStatus)
        {
            this.invokeDiskFailurePolicy = invokeDiskFailurePolicy;
            this.extendedVerification = extendedVerification;
            this.checkVersion = checkVersion;
            this.mutateRepairStatus = mutateRepairStatus;
        }

        @Override
        public String toString()
        {
            return "OptionHolder{" +
                   "invokeDiskFailurePolicy=" + invokeDiskFailurePolicy +
                   ", extendedVerification=" + extendedVerification +
                   ", checkVersion=" + checkVersion +
                   ", mutateRepairStatus=" + mutateRepairStatus +
                   '}';
        }
    }
}
