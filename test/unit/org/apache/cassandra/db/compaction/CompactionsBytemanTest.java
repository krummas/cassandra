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

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.cql3.CQLTester;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(BMUnitRunner.class)
public class CompactionsBytemanTest extends CQLTester
{

    @Test
    @BMRules(rules = { @BMRule(name = "No disk space test",
                               targetClass = "Directories",
                               targetMethod = "hasAvailableDiskSpace",
                               condition = "not flagged(\"done\")",
                               action = "flag(\"done\"); return false;") } ) // only return false once
    public void testDiskspace() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, val text) with compaction = {'class':'SizeTieredCompactionStrategy', 'enabled': 'false'}");
        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (id, val) values ("+i+", 'xyz')");
            getCurrentColumnFamilyStore().forceBlockingFlush();
        }
        assertEquals(10, getCurrentColumnFamilyStore().getLiveSSTables().size());
        getCurrentColumnFamilyStore().forceMajorCompaction(true);
        assertEquals(2, getCurrentColumnFamilyStore().getLiveSSTables().size());
    }

}
