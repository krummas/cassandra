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
package org.apache.cassandra.streaming.messages;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.cassandra.io.util.DataOutputStreamAndChannel;
import org.apache.cassandra.streaming.StreamRequest;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamSummary;

public class IncrementalPrepareMessage extends StreamMessage
{
    public static Serializer<IncrementalPrepareMessage> serializer = new Serializer<IncrementalPrepareMessage>()
    {
        public IncrementalPrepareMessage deserialize(ReadableByteChannel in, int version, StreamSession session) throws IOException
        {
            DataInput input = new DataInputStream(Channels.newInputStream(in));
            IncrementalPrepareMessage message = new IncrementalPrepareMessage();
            // requests
            int numRequests = input.readInt();
            for (int i = 0; i < numRequests; i++)
                message.requests.add(StreamRequest.serializer.deserialize(input, version));
            // summaries
            int numSummaries = input.readInt();
            for (int i = 0; i < numSummaries; i++)
                message.summaries.add(StreamSummary.serializer.deserialize(input, version));
            return message;
        }

        public void serialize(IncrementalPrepareMessage message, DataOutputStreamAndChannel out, int version, StreamSession session) throws IOException
        {
            // requests
            out.writeInt(message.requests.size());
            for (StreamRequest request : message.requests)
                StreamRequest.serializer.serialize(request, out, version);
            // summaries
            out.writeInt(message.summaries.size());
            for (StreamSummary summary : message.summaries)
                StreamSummary.serializer.serialize(summary, out, version);
        }
    };

    /**
     * Streaming requests
     */
    public final Collection<StreamRequest> requests = new ArrayList<>();

    /**
     * Summaries of streaming out
     */
    public final Collection<StreamSummary> summaries = new ArrayList<>();

    public IncrementalPrepareMessage()
    {
        super(Type.PREPARE_INCREMENTAL);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("IncrementalPrepare (");
        sb.append(requests.size()).append(" requests, ");
        int totalFile = 0;
        for (StreamSummary summary : summaries)
            totalFile += summary.files;
        sb.append(" ").append(totalFile).append(" files");
        sb.append('}');
        return sb.toString();
    }
}
