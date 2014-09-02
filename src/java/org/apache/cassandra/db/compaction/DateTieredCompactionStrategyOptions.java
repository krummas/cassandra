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

import java.util.Map;

import org.apache.cassandra.exceptions.ConfigurationException;

public final class DateTieredCompactionStrategyOptions
{
    protected static final long DEFAULT_MAX_SSTABLE_AGE = 0; //5 * 365L * 24L * 60L * 60L * 1000L * 1000L; // 5 years
    protected static final long DEFAULT_TIME_UNIT = 60L * 60L * 1000L * 1000L; // One hour
    protected static final String MAX_SSTABLE_AGE_KEY = "max_sstable_age";
    protected static final String TIME_UNIT_KEY = "time_unit";

    protected long maxSSTableAge;
    protected long timeUnit;

    public DateTieredCompactionStrategyOptions(Map<String, String> options)
    {
        String optionValue = options.get(MAX_SSTABLE_AGE_KEY);
        maxSSTableAge = optionValue == null ? DEFAULT_MAX_SSTABLE_AGE : Long.parseLong(optionValue);
        optionValue = options.get(TIME_UNIT_KEY);
        timeUnit = optionValue == null ? DEFAULT_TIME_UNIT : Long.parseLong(optionValue);
    }

    public DateTieredCompactionStrategyOptions()
    {
        maxSSTableAge = DEFAULT_MAX_SSTABLE_AGE;
        timeUnit = DEFAULT_TIME_UNIT;
    }

    public static Map<String, String> validateOptions(Map<String, String> options, Map<String, String> uncheckedOptions) throws  ConfigurationException
    {
        String optionValue = options.get(MAX_SSTABLE_AGE_KEY);
        try
        {
            long maxSStableAge = optionValue == null ? DEFAULT_MAX_SSTABLE_AGE : Long.parseLong(optionValue);
            if (maxSStableAge < 0)
            {
                throw new ConfigurationException(String.format("%s must be non-negative: %d", MAX_SSTABLE_AGE_KEY, maxSStableAge));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, MAX_SSTABLE_AGE_KEY), e);
        }

        optionValue = options.get(TIME_UNIT_KEY);
        try
        {
            long timeUnit = optionValue == null ? DEFAULT_TIME_UNIT : Long.parseLong(optionValue);
            if (timeUnit <= 0)
            {
                throw new ConfigurationException(String.format("%s must be greater than 0, but was %d", TIME_UNIT_KEY, timeUnit));
            }
        }
        catch (NumberFormatException e)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", optionValue, TIME_UNIT_KEY), e);
        }

        uncheckedOptions.remove(MAX_SSTABLE_AGE_KEY);
        uncheckedOptions.remove(TIME_UNIT_KEY);

        return uncheckedOptions;
    }
}
