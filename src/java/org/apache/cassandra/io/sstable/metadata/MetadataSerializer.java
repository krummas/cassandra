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
package org.apache.cassandra.io.sstable.metadata;

import java.io.*;
import java.util.*;

import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.VersionedComponent;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.ChecksummedRandomAccessReader;
import org.apache.cassandra.io.util.ChecksummedSequentialWriter;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Metadata serializer for SSTables {@code version >= 'k'}.
 *
 * <pre>
 * File format := | number of components (4 bytes) | toc | component1 | component2 | ... |
 * toc         := | component type (4 bytes) | position of component |
 * </pre>
 *
 * IMetadataComponent.Type's ordinal() defines the order of serialization.
 */
public class MetadataSerializer implements IMetadataSerializer
{
    private static final Logger logger = LoggerFactory.getLogger(MetadataSerializer.class);
    private static final SequentialWriterOption writerOption = SequentialWriterOption.newBuilder()
                                                                              .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                                                              .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024)
                                                                              .build();
    public void serialize(Map<MetadataType, MetadataComponent> components, DataOutputPlus out, Version version) throws IOException
    {
        // sort components by type
        List<MetadataComponent> sortedComponents = Lists.newArrayList(components.values());
        Collections.sort(sortedComponents);

        // write number of component
        out.writeInt(components.size());
        // build and write toc
        int lastPosition = 4 + (8 * sortedComponents.size());
        for (MetadataComponent component : sortedComponents)
        {
            MetadataType type = component.getType();
            // serialize type
            out.writeInt(type.ordinal());
            // serialize position
            out.writeInt(lastPosition);
            lastPosition += type.serializer.serializedSize(version, component);
        }
        // serialize components
        for (MetadataComponent component : sortedComponents)
        {
            component.getType().serializer.serialize(version, component, out);
        }
    }

    public Map<MetadataType, MetadataComponent> deserialize( Descriptor descriptor, EnumSet<MetadataType> types, int fileVersion) throws IOException
    {
        Map<MetadataType, MetadataComponent> components;
        logger.trace("Load metadata for {}", descriptor);
        File statsFile = new File(descriptor.filenameFor(new VersionedComponent(Component.Type.STATS, fileVersion)));
        File crcFile = new File(descriptor.filenameFor(new VersionedComponent(Component.Type.STATS_CRC, fileVersion)));
        if (!statsFile.exists())
        {
            components = new EnumMap<>(MetadataType.class);
            components.put(MetadataType.STATS, MetadataCollector.defaultStatsMetadata());
        }
        else
        {
            try (RandomAccessReader r = crcFile.exists() ? ChecksummedRandomAccessReader.open(statsFile, crcFile) : RandomAccessReader.open(statsFile))
            {
                components = deserialize(descriptor, r, types);
            }
            catch (Exception e)
            {
                throw new CorruptSSTableException(e, statsFile);
            }
        }
        return components;
    }

    public MetadataComponent deserialize(Descriptor descriptor, MetadataType type, int fileVersion) throws IOException
    {
        return deserialize(descriptor, EnumSet.of(type), fileVersion).get(type);
    }

    public Map<MetadataType, MetadataComponent> deserialize(Descriptor descriptor, FileDataInput in, EnumSet<MetadataType> types) throws IOException
    {
        Map<MetadataType, MetadataComponent> components = new EnumMap<>(MetadataType.class);
        // read number of components
        int numComponents = in.readInt();
        // read toc
        Map<MetadataType, Integer> toc = new EnumMap<>(MetadataType.class);
        MetadataType[] values = MetadataType.values();
        for (int i = 0; i < numComponents; i++)
        {
            toc.put(values[in.readInt()], in.readInt());
        }
        for (MetadataType type : types)
        {
            Integer offset = toc.get(type);
            if (offset != null)
            {
                in.seek(offset);
                MetadataComponent component = type.serializer.deserialize(descriptor.version, in);
                components.put(type, component);
            }
        }
        return components;
    }

    public void mutateLevel(Descriptor descriptor, int fileVersion, int newLevel) throws IOException
    {
        logger.trace("Mutating {} to level {}", descriptor, newLevel);
        Map<MetadataType, MetadataComponent> currentComponents = deserialize(descriptor, EnumSet.allOf(MetadataType.class), fileVersion);
        StatsMetadata stats = (StatsMetadata) currentComponents.remove(MetadataType.STATS);
        // mutate level
        currentComponents.put(MetadataType.STATS, stats.mutateLevel(newLevel));
        rewriteSSTableMetadata(descriptor, fileVersion + 1, currentComponents);
    }

    public void mutateRepaired(Descriptor descriptor, int fileVersion, long newRepairedAt, UUID newPendingRepair) throws IOException
    {
        logger.trace("Mutating {} to repairedAt time {} and pendingRepair {}",
                     descriptor, newRepairedAt, newPendingRepair);
        Map<MetadataType, MetadataComponent> currentComponents = deserialize(descriptor, EnumSet.allOf(MetadataType.class), fileVersion);
        StatsMetadata stats = (StatsMetadata) currentComponents.remove(MetadataType.STATS);
        // mutate time & id
        currentComponents.put(MetadataType.STATS, stats.mutateRepairedAt(newRepairedAt).mutatePendingRepair(newPendingRepair));
        rewriteSSTableMetadata(descriptor, fileVersion + 1, currentComponents);
    }

    private void rewriteSSTableMetadata(Descriptor descriptor, int fileVersion, Map<MetadataType, MetadataComponent> currentComponents) throws IOException
    {
        File file = new File(descriptor.filenameFor(new VersionedComponent(Component.Type.STATS, fileVersion)));
        File crcFile = new File(descriptor.filenameFor(new VersionedComponent(Component.Type.STATS_CRC, fileVersion)));
        assert !file.exists() && !crcFile.exists() : file + " : " + crcFile;
        serializeWithChecksum(descriptor, currentComponents, file, crcFile);
    }

    /**
     * Writes the given metadata components to file with a checksum in crcFile
     * @throws IOException
     */
    public void serializeWithChecksum(Descriptor descriptor, Map<MetadataType, MetadataComponent> components, File file, File crcFile) throws IOException
    {
        try (SequentialWriter out = new ChecksummedSequentialWriter(file, crcFile, null, writerOption))
        {
            serialize(components, out, descriptor.version);
            out.finish();
        }
    }
}
