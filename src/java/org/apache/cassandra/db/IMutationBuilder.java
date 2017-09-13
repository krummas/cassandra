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

import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.TableId;

public interface IMutationBuilder
{
    /**
     * Add a new PartitionUpdate builder to this mutation builder
     * @param builder the builder to add
     * @return this
     */
    IMutationBuilder add(PartitionUpdate.Builder builder);

    /**
     * Build the immutable mutation
     * @return
     */
    IMutation build();

    /**
     * Get the builder for the given cfId
     * @param cfId
     * @return
     */
    PartitionUpdate.Builder get(TableId tableId);
}
