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

package org.apache.cassandra.audit;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.wire.ValueOut;
import net.openhft.chronicle.wire.WireOut;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.binlog.BinLog;

/**
 * A logger that logs entire query contents after the query finishes (or times out).
 */
public class FullQueryLogger extends BinLogAuditLogger implements IAuditLogger
{
    public static final String BATCH = "batch";
    public static final String SINGLE = "single";
    public static final String QUERY = "query";
    public static final String BATCH_TYPE = "batch-type";
    public static final String QUERIES = "queries";
    public static final String VALUES = "values";

    @Override
    public void log(AuditLogEntry entry)
    {
        logQuery(entry.getOperation(), entry.getOptions(), entry.getState(), entry.getTimestamp());
    }

    /**
     * Log an invocation of a batch of queries
     * @param type The type of the batch
     * @param queries CQL text of the queries
     * @param values Values to bind to as parameters for the queries
     * @param queryOptions Options associated with the query invocation
     * @param queryState Timestamp state associated with the query invocation
     * @param batchTimeMillis Approximate time in milliseconds since the epoch since the batch was invoked
     */
    void logBatch(BatchStatement.Type type,
                  List<String> queries,
                  List<List<ByteBuffer>> values,
                  QueryOptions queryOptions,
                  QueryState queryState,
                  long batchTimeMillis)
    {
        Preconditions.checkNotNull(type, "type was null");
        Preconditions.checkNotNull(queries, "queries was null");
        Preconditions.checkNotNull(values, "value was null");
        Preconditions.checkNotNull(queryOptions, "queryOptions was null");
        Preconditions.checkNotNull(queryState, "queryState was null");
        Preconditions.checkArgument(batchTimeMillis > 0, "batchTimeMillis must be > 0");

        //Don't construct the wrapper if the log is disabled
        BinLog binLog = this.binLog;
        if (binLog == null)
        {
            return;
        }

        Batch wrappedBatch = new Batch(type, queries, values, queryOptions, queryState, batchTimeMillis);
        logRecord(wrappedBatch, binLog);
    }

    /**
     * Log a single CQL query
     * @param query CQL query text
     * @param queryOptions Options associated with the query invocation
     * @param queryState Timestamp state associated with the query invocation
     * @param queryTimeMillis Approximate time in milliseconds since the epoch since the batch was invoked
     */
    void logQuery(String query, QueryOptions queryOptions, QueryState queryState, long queryTimeMillis)
    {
        Preconditions.checkNotNull(query, "query was null");
        Preconditions.checkNotNull(queryOptions, "queryOptions was null");
        Preconditions.checkNotNull(queryState, "queryState was null");
        Preconditions.checkArgument(queryTimeMillis > 0, "queryTimeMillis must be > 0");

        //Don't construct the wrapper if the log is disabled
        BinLog binLog = this.binLog;
        if (binLog == null)
        {
            return;
        }

        Query wrappedQuery = new Query(query, queryOptions, queryState, queryTimeMillis);
        logRecord(wrappedQuery, binLog);
    }

    static class Batch extends AbstractLogEntry
    {
        private final int weight;
        private final BatchStatement.Type batchType;
        private final List<String> queries;
        private final List<List<ByteBuffer>> values;

        public Batch(BatchStatement.Type batchType,
                     List<String> queries,
                     List<List<ByteBuffer>> values,
                     QueryOptions queryOptions,
                     QueryState queryState,
                     long batchTimeMillis)
        {
            super(queryOptions, queryState, batchTimeMillis);

            this.queries = queries;
            this.values = values;
            this.batchType = batchType;

            int weight = super.weight();

            weight += 4 +                       // weight
                      2 * EMPTY_LIST_SIZE +     // two lists, queries & values
                      8;                        // enum size (batch type)

            for (String query : queries)
                weight += ObjectSizes.sizeOf(query);

            for (List<ByteBuffer> subValues : values)
            {
                weight += EMPTY_LIST_SIZE;
                for (ByteBuffer value : subValues)
                    weight += EMPTY_BYTEBUFFER_SIZE + value.capacity();
            }

            this.weight = weight;
        }

        @Override
        protected String type()
        {
            return BATCH;
        }

        @Override
        public void writeMarshallable(WireOut wire)
        {
            super.writeMarshallable(wire);
            wire.write(BATCH_TYPE).text(batchType.name());
            ValueOut valueOut = wire.write(QUERIES);
            valueOut.int32(queries.size());
            for (String query : queries)
            {
                valueOut.text(query);
            }
            valueOut = wire.write(VALUES);
            valueOut.int32(values.size());
            for (List<ByteBuffer> subValues : values)
            {
                valueOut.int32(subValues.size());
                for (ByteBuffer value : subValues)
                {
                    valueOut.bytes(BytesStore.wrap(value));
                }
            }
        }

        @Override
        public int weight()
        {
            return weight;
        }
    }

    static class Query extends AbstractLogEntry
    {
        private final String query;

        public Query(String query, QueryOptions queryOptions, QueryState queryState, long queryStartTime)
        {
            super(queryOptions, queryState, queryStartTime);
            this.query = query;
        }

        @Override
        protected String type()
        {
            return SINGLE;
        }

        @Override
        public void writeMarshallable(WireOut wire)
        {
            super.writeMarshallable(wire);
            wire.write(QUERY).text(query);
        }

        @Override
        public int weight()
        {
            return Ints.checkedCast(ObjectSizes.sizeOf(query)) + super.weight();
        }
    }
}
