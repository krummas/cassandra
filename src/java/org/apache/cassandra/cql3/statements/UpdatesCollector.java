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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;

/**
 * Utility class to collect updates.
 *
 * <p>In a batch statement we don't want to recreate mutations every time as this is particularly inefficient when
 * applying multiple batch to the same partition (see #6737). </p>
 *
 */
final class UpdatesCollector
{
    /**
     * The columns that will be updated for each table (keyed by the table ID).
     */
    private final Map<UUID, PartitionColumns> updatedColumns;

    /**
     * The estimated number of updated row.
     */
    private final int updatedRows;

    /**
     * The mutations per keyspace.
     */
    private final Map<String, Map<ByteBuffer, IMutationBuilder>> mutationBuilders = new HashMap<>();

    public UpdatesCollector(Map<UUID, PartitionColumns> updatedColumns, int updatedRows)
    {
        super();
        this.updatedColumns = updatedColumns;
        this.updatedRows = updatedRows;
    }

    /**
     * Gets the <code>PartitionUpdate.Builder</code> for the specified column family and key. If the builder does not
     * exist it will be created.
     *
     * @param cfm the column family meta data
     * @param dk the partition key
     * @param consistency the consistency level
     * @return the <code>PartitionUpdate.Builder</code> for the specified column family and key
     */
    public PartitionUpdate.Builder getPartitionUpdateBuilder(CFMetaData cfm, DecoratedKey dk, ConsistencyLevel consistency)
    {
        IMutationBuilder mut = getMutationBuilder(cfm, dk, consistency);
        PartitionUpdate.Builder upd = mut.get(cfm.cfId);
        if (upd == null)
        {
            PartitionColumns columns = updatedColumns.get(cfm.cfId);
            assert columns != null;
            upd = new PartitionUpdate.Builder(cfm, dk, columns, updatedRows);
            mut.add(upd);
        }
        return upd;
    }

    private IMutationBuilder getMutationBuilder(CFMetaData cfm, DecoratedKey dk, ConsistencyLevel consistency)
    {
        String ksName = cfm.ksName;
        IMutationBuilder mutationBuilder = keyspaceMap(ksName).get(dk.getKey());
        if (mutationBuilder == null)
        {
            Mutation.Builder builder = new Mutation.Builder(ksName, dk);
            mutationBuilder = cfm.isCounter() ? new CounterMutation.Builder(builder, consistency) : builder;
            keyspaceMap(ksName).put(dk.getKey(), mutationBuilder);
        }
        return mutationBuilder;
    }

    /**
     * Returns a collection containing all the mutations.
     * @return a collection containing all the mutations.
     */
    public Collection<IMutation> toMutations()
    {
        //TODO: The case where all statement where on the same keyspace is pretty common, optimize for that?
        List<IMutation> ms = new ArrayList<>();
        for (Map<ByteBuffer, IMutationBuilder> ksMap : mutationBuilders.values())
            ms.addAll(ksMap.values().stream().map(IMutationBuilder::build).collect(Collectors.toList()));
        return ms;
    }

    /**
     * Returns the key-mutation mappings for the specified keyspace.
     *
     * @param ksName the keyspace name
     * @return the key-mutation mappings for the specified keyspace.
     */
    private Map<ByteBuffer, IMutationBuilder> keyspaceMap(String ksName)
    {
        return mutationBuilders.computeIfAbsent(ksName, k -> new HashMap<>());
    }

    public static void validateIndexedColumns(Collection<? extends IMutation> mutations)
    {
        mutations.forEach(mutation -> mutation.getPartitionUpdates().forEach(update -> Keyspace.openAndGetStore(update.metadata()).indexManager.validate(update)));
    }
}
