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

package org.apache.cassandra.tools.nodetool;

import static com.google.common.base.Preconditions.checkArgument;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

import io.airlift.airline.Option;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "import", description = "Import new SSTables to the system")
public class Import extends NodeToolCmd
{
    @Arguments(usage = "<keyspace> <table>", description = "The keyspace and table name")
    private List<String> args = new ArrayList<>();

    @Option(title = "external_directory",
            name = {"-d", "--directory"},
            required = true,
            description = "Directory to load new SSTables from. SSTables are moved, so original copy is gone after refresh")
    private String directory = null;

    @Option(title = "keep_level",
            name = {"-l", "--keep-level"},
            description = "Keep the level on the new sstables")
    private boolean keepLevel = false;

    @Option(title = "keep_repaired",
            name = {"-r", "--keep-repaired"},
            description = "Keep any repaired information from the sstables")
    private boolean keepRepaired = false;

    @Option(title = "no_verify_sstables",
            name = {"-v", "--no-verify"},
            description = "Don't verify new sstables")
    private boolean noVerify = false;

    @Option(title = "no_verify_tokens",
            name = {"-t", "--no-tokens"},
            description = "Don't verify that all tokens in the new sstable are owned by the current node")
    private boolean noVerifyTokens = false;

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 2, "import requires keyspace and table name");
        if (directory == null)
        {
            System.out.println("Directory (-d) is required");
            System.exit(1);
        }

        if (!noVerifyTokens && noVerify)
        {
            System.out.println("Verify tokens requires verify sstables");
            System.exit(1);
        }
        probe.importNewSSTables(args.get(0), args.get(1), directory, !keepLevel, !keepRepaired, !noVerify, !noVerifyTokens);
    }
}