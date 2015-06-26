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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LivenessInfoArrayTest
{
    @Test
    public void basicTest()
    {
        LivenessInfoArray arr = new LivenessInfoArray(200);
        for (int i = 0; i < 200; i++)
            arr.set(i, i*10, i*20, i*30, i % 2 == 0);
        for (int i = 0; i < 200; i++)
        {
            assertEquals(i % 2 == 0, arr.isRepaired(i));
            assertEquals(i*10, arr.timestamp(i));
            assertEquals(i*20, arr.ttl(i));
            assertEquals(i*30, arr.localDeletionTime(i));
        }
    }

    @Test
    public void resizeTest()
    {
        LivenessInfoArray arr = new LivenessInfoArray(2000);
        for (int i = 0; i < 2000; i++)
            arr.set(i, i * 10, i * 20, i * 30, i % 2 == 0);

        arr.resize(4000);
        for (int i = 0; i < 2000; i++)
        {
            assertEquals(i % 2 == 0,arr.isRepaired(i));
            assertEquals(i * 10, arr.timestamp(i));
            assertEquals(i * 20, arr.ttl(i));
            assertEquals(i * 30, arr.localDeletionTime(i));
        }
    }

    @Test
    public void swapTest()
    {
        LivenessInfoArray arr = new LivenessInfoArray(200);
        for (int i = 0; i < 200; i++)
            arr.set(i, i * 10, i * 20, i * 30, i % 2 == 1);

        arr.swap(10, 151);

        for (int i = 0; i < 200; i++)
        {
            int x = i;
            if (i == 10)
                x = 151;
            if (i == 151)
                x = 10;
            assertEquals(x % 2 == 1, arr.isRepaired(i));
            assertEquals(x * 10, arr.timestamp(i));
            assertEquals(x * 20, arr.ttl(i));
            assertEquals(x * 30, arr.localDeletionTime(i));
        }

    }
}
