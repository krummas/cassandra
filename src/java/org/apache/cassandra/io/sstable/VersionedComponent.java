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

package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.utils.Pair;

public class VersionedComponent extends Component
{
    private static final Logger logger = LoggerFactory.getLogger(VersionedComponent.class);
    public final int version;

    private VersionedComponent(Type type, String name, int version)
    {
        super(type, name);
        this.version = version;
    }
    public VersionedComponent(Type type, String name)
    {
        this(type, name, getVersionFromFilename(type, new File(name)));
    }

    public VersionedComponent(Type stats)
    {
        this(stats, String.format(stats.fileFormat, 0), 0);
    }

    public VersionedComponent(Type stats, int fileVersion)
    {
        this(stats, fileVersion < 0 ? stats.legacyName : String.format(stats.fileFormat, fileVersion), fileVersion);
    }

    public static VersionedComponent getLatestVersion(Descriptor desc, Type componentType)
    {
        int maxVersion = 0;
        VersionedComponent maxComponent = null;
        for (VersionedComponent component : getAllVersions(desc, componentType))
        {
            if (maxComponent == null && component.version < 0)
                maxComponent = component; // handle legacy Statistics files
            if (component.version >= maxVersion)
            {
                maxVersion = component.version;
                maxComponent = component;
            }
        }
        if (maxComponent == null)
            maxComponent = new VersionedComponent(componentType, String.format(componentType.fileFormat, 0), 0);
        logger.trace("got latest version {} for {} (type = {}, component = {})", maxVersion, desc, componentType, maxComponent);
        return maxComponent;
    }

    private static int getVersionFromFilename(Type type, File f)
    {
        Pattern p = Pattern.compile(type.repr);
        Matcher m = p.matcher(f.toString());
        if (m.matches())
        {
            if (m.group(1) == null)
                return -1;
            return Integer.parseInt(m.group(1));
        }
        throw new UnsupportedOperationException("Could not get version from filename: " + f.toString());
    }

    public static Collection<VersionedComponent> getAllVersions(Descriptor desc, Type componentType)
    {
        List<VersionedComponent> allVersions = new ArrayList<>();
        desc.directory.listFiles(file ->
                                 {
                                     Pair<Descriptor, Component> pair = SSTable.tryComponentFromFilename(file);
                                     if (pair != null && pair.left.generation == desc.generation && pair.right.type == componentType)
                                         allVersions.add((VersionedComponent) pair.right);
                                     return false;
                                 });
        return allVersions;
    }
}
