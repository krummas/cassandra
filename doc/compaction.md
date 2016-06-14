# Compaction

## Types of compaction

The concept of compaction is used for different kinds of operations in Cassandra, the common thing about these operations is that it takes one or more sstables and output new sstables. The types of compactions are;

* Minor compaction - triggered automatically in Cassandra.
* Major compaction - a user executes a compaction over all sstables on the node.
* User defined compaction - a user triggers a compaction on a given set of sstables.
* Scrub - try to fix any broken sstables. This can actually remove valid data if that data is corrupted, if that happens you will need to run a full repair on the node.
* Upgradesstables - upgrade sstables to the latest version. Run this after upgrading to a new major version.
* Cleanup - remove any ranges this node does not own anymore, typically triggered on neighbouring nodes after a node has been bootstrapped since that node will take ownership of some ranges from those nodes.
* Secondary index rebuild - rebuild the secondary indexes on the node.
* Anticompaction - after repair the ranges that were actually repaired are split out of the sstables that existed when repair started.

## When is a minor compaction triggered?

* When an sstable is added to the node through flushing/streaming etc.
* When autocompaction is enabled after being disabled (nodetool enableautocompaction)
* When compaction adds new sstables.
* A check for new minor compactions every 5 minutes.

## Merging sstables

Compaction is about merging sstables, since partitions in sstables are sorted based on the hash of the partition key it is possible to efficiently merge separate sstables. Content of each partition is also sorted so each partition can be merged efficiently.

## Tombstones and gc_grace

When a delete is issued in Cassandra what happens is that a tombstone is written, this tombstone shadows the data it deletes. This means that the tombstone can live in one sstable and the data it covers is in another sstable. To be able to remove the actual data, a compaction where both the sstable containing the tombstone and the sstable containing the data is included the same compaction is needed.

gc\_grace\_seconds is the minimum time tombstones are kept around. If you generally run repair once a week, then gc\_grace\_seconds needs to be atleast 1 week (you probably want some margin as well), otherwise you might drop a tombstone that has not been propagated to all replicas and that could cause deleted data to become live again.

To be able to drop an actual tombstone the following needs to be true;

* The tombstone must be older than `gc_grace_seconds`
* If partition X contains the tombstone, the sstable containing the partition plus all sstables containing data older than the tombstone containing X must be included in the same compaction. We don't need to care if the partition is in an sstable if we can guarantee that all data in that sstable is newer than the tombstone. If the tombstone is older than the data it cannot shadow that data.
* If the option `only_purge_repaired_tombstones` is enabled, tombstones are only removed if the data has also been repaired.

## TTL

Data in Cassandra can have an additional property called time to live - this is used to automatically drop data that has expired once the time is reached. Once the TTL has expired the data is converted to a tombstone which stays around for at least gc\_grace\_seconds. Note that if you mix data with TTL and data without TTL (or just different length of the TTL) Cassandra will have a hard time dropping the tombstones created since the partition might span many sstables and not all are compacted at once.

## Fully expired sstables

If an sstable contains only tombstones and it is guaranteed that that sstable is not shadowing data in any other sstable compaction can drop that sstable. If you see sstables with only tombstones (note that TTL:ed data is considered tombstones once the time to live has expired) but it is not being dropped by compaction, it is likely that other sstables contain older data. There is a tool called `sstableexpiredblockers` that will list which sstables are droppable and which are blocking them from being dropped. This is especially useful for time series compaction with `TimeWindowCompactionStrategy` (and the deprecated `DateTieredCompactionStrategy`).

## Repaired/unrepaired data

With incremental repairs Cassandra must keep track of what data is repaired and what data is unrepaired. With anticompaction repaired data is split out into repaired and unrepaired sstables. To avoid mixing up the data again separate compaction strategy instances are run on the two sets of data, each instance only knowing about either the repaired or the unrepaired sstables. This means that if you only run incremental repair once and then never again, you might have very old data in the repaired sstables that block compaction from dropping tombstones in the unrepaired (probably newer) sstables.

## Data directories

Since tombstones and data can live in different sstables it is important to realize that losing an sstable might lead to data becoming live again - the most common way of losing sstables is to have a hard drive break down. To avoid making data live tombstones and actual data are always in the same data directory. This way, if a disk is lost, all versions of a partition are lost and no data can get undeleted. To achieve this a compaction strategy instance per data directory is run in addition to the compaction strategy instances containing repaired/unrepaired data, this means that if you have 4 data directories there will be 8 compaction strategy instances running. This has a few more benefits than just avoiding data getting undeleted:

* It is possible to run more compactions in parallel - leveled compaction will have several totally separate levelings and each one can run compactions independently from the others.
* Users can backup and restore a single data directory.
* Note though that currently all data directories are considered equal, so if you have a tiny disk and a big disk backing two data directories, the big one will be limited the by the small one. One work around to this is to create more data directories backed by the big disk.

## Single sstable tombstone compaction

When an sstable is written a histogram with the tombstone expiry times is created and this is used to try to find sstables with very many tombstones and run single sstable compaction on that sstable in hope of being able to drop tombstones in that sstable. Before starting this it is also checked how likely it is that any tombstones will actually will be able to be dropped how much this sstable overlaps with other sstables. To avoid most of these checks the compaction option `unchecked_tombstone_compaction` can be enabled.

## Common options

There is a bunch of common options for all the compaction strategies;

* `enabled`: Whether minor compactions should run. Note that you can have 'enabled': true as a compaction option and then do 'nodetool enableautocompaction' to start running compactions. Default true.
* `tombstone_threshold`: How much of the sstable should be tombstones for us to consider doing a single sstable compaction of that sstable, defaults to 0.2
* `tombstone_compaction_interval`: Since it might not be possible to drop any tombstones when doing a single sstable compaction we need to make sure that one sstable is not constantly getting recompacted - this option states how often we should try for a given sstable. Default 86400s (1 day).
* `log_all`: New detailed compaction logging, read below. Default false.
* `unchecked_tombstone_compaction`: The single sstable compaction has quite strict checks for whether it should be started, this option disables those checks and for some usecases this might be needed. Note that this does not change anything for the actual compaction, tombstones are only dropped if it is safe to do so - it might just rewrite an sstable without being able to drop any tombstones. Default false.
* `only_purge_repaired_tombstone`: Option to enable the extra safety of making sure that tombstones are only dropped if the data has been repaired. Default false.
* `min_threshold`: Lower limit of number of sstables before a compaction is triggered. Default is 4. Not used for LeveledCompactionStrategy.
* `max_threshold`: Upper limit of number of sstables before a compaction is triggered. Default is 32. Not used for LeveledCompactionStrategy.

## compaction nodetool commands

* `enableautocompaction`, `disableautocompaction` - enable/disable compaction.
* `setcompactionthroughput` - how fast compaction should run at most - defaults to 16MB/s, but note that it is likely that it is not possible to reach this througput.
* `compactionstats` - statistics about current and pending compactions.
* `compactionhistory` - list details about the last compactions.
* `setcompactionthreshold` - set the min/max sstable count for when to trigger compaction, defaults to 4/32.

## Switching compction strategies and options using JMX

It is possible to switch compaction strategies and options on just a single node using JMX, this is a great way to experiment with settings without affecting the whole cluster. The mbean is org.apache.cassandra.db:type=ColumnFamilies,keyspace=<keyspace>,columnfamily=<tablename> and the attribute you should change is CompactionParameters or CompactionParametersJson if you use jconsole or jmc. The syntax for the json version is the same as you would use in an `ALTER TABLE` - for example: `{'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': 123}`. The setting is kept until someone executes an `ALTER TABLE that touches the compaction settings or restarts the node.

## More detailed compaction logging

Enable with the compaction option `log_all` and a more detailed compaction log file will be produced in your log directory.

## SizeTieredCompactionStrategy

Basic idea of SizeTieredCompactionStrategy (STCS) is to merge sstables of approximately the same size. All sstables are put in different buckets depending on their size. An sstable is added to the bucket if size of the sstable is within bucket\_low and bucket\_high of the current average size of the sstables already in the bucket. This will create several buckets and the most interesting of those buckets will be compacted. The most interesting one is decided by figuring out which bucket's sstables takes the most reads.

### Major compaction

When running a major compaction with STCS you will end up with two sstables per data directory (one for repaired data and one for unrepaired data). There is also an option (-s) to do a major compaction that splits the output into several sstables. The sizes of the sstables are approximately 50%, 25%, 12.5%... of the total size.

### STCS options

* `min_sstable_size`: sstables smaller than this are put in the same bucket. Default is 50MB.
* `bucket_low`, `bucket_high`: limit of the size of an sstable to add to a bucket, if `bucket_low * avg_bucket_size > sstable_size` and `bucket_high * avg_bucket_size < sstable_size` holds, it is added to the bucket. Defaults are 0.5 and 1.5.

### Defragmentation

Defragmentation is done when many sstables are touched during a read. The result of the read is put in to the memtable so that the next read will not have to touch as many sstables. This can cause writes on a read-only-cluster.

## LeveledCompactionStrategy

The idea of LeveledCompactionStrategy (LCS) is that all sstables are put into different levels where we guarantee that no overlapping sstables are in the same level. By overlapping we mean that the first/last token of a single sstable are never overlapping with other sstables. This means that for a SELECT we will only have to look for the partition key in a single sstable per level. Each level is 10x the size of the previous one and each sstable is 160MB by default. L0 is where sstables are streamed/flushed - no overlap guarantees are given here.

When picking compaction candidates we have to make sure that the compaction does not create overlap in the target level. This is done by always including all overlapping sstables in the next level. For example if we select an sstable in L3, we need to guarantee that we pick all overlapping sstables in L4 and make sure that no currently ongoing compactions will create overlap if we start that compaction. We can start many parallel compactions in a level if we guarantee that we wont create overlap. For L0 -> L1 compactions we almost always need to include all L1 sstables since most L0 sstables cover the full range. We also can't compact all L0 sstables with all L1 sstables in a single compaction since that can use too much memory.

When deciding which level to compact LCS checks the higher levels first (with LCS, a "higher" level is one with a higher number, L0 being the lowest one) and if the level is behind a compaction will be started in that level.

### Major compaction

It is possible to do a major compaction with LCS - it will currently start by filling out L1 and then once L1 is full, it continues with L2 etc. This is sub optimal and will change to create all the sstables in a high level instead, CASSANDRA-11817.

### Bootstrapping

During bootstrap sstables are streamed from other nodes. The level of the remote sstable is kept to avoid many compactions after the bootstrap is done. During bootstrap the new node also takes writes while it is streaming the data from a remote node - these writes are flushed to L0 like all other writes and to avoid those sstables blocking the remote sstables from going to the correct level, we only do STCS in L0 until the bootstrap is done.

### STCS in L0

If LCS gets very many L0 sstables reads are going to hit all (or most) of the L0 sstables since they are likely to be overlapping. To more quickly remedy this LCS does STCS compactions in L0 if there are more than 32 sstables there. This should improve read performance more quickly compared to letting LCS do its L0 -> L1 compactions. If you keep getting too many sstables in L0 it is likely that LCS is not the best fit for your workload and STCS could work out better.

### Starved sstables

If a node ends up with a leveling where there are a few very high level sstables that are not getting compacted they might make it impossible for lower levels to drop tombstones etc. For example, if there are sstables in L6 but there is only enough data to actually get a L4 on the node the left over sstables in L6 will get starved and not compacted. This can happen if a user changes sstable_size_in_mb from 5MB to 160MB for example. To avoid this LCS tries to include those starved high level sstables in other compactions if there has been 25 compaction rounds where the highest level has not been involved.

### LCS options

* `sstable_size_in_mb`: the target compressed (if using compression) sstable size - the sstables can end up being larger if there are very large partitions on the node. Default is 160MB
* `cassandra.disable_stcs_in_l0`: a startup option (`-Dcassandra.disable_stcs_in_l0=true`)to avoid doing STCS in L0.

## TimeWindowCompactionStrategy

TimeWindowCompactionStrategy is designed specifically for workloads where it's beneficial to have data on disk grouped by the timestamp of the data, a common goal when the workload is time-series in nature or when all data is written with a TTL. In an expiring/TTL workload, the contents of an entire SSTable likely expire at approximately the same time, allowing them to be dropped completely, and space reclaimed much more reliably than when using SizeTieredCompactionStrategy or LeveledCompactionStrategy. The basic concept is that TimeWindowCompactionStrategy will create 1 sstable per file for a given window, where a window is simply calculated as the combination of two primary options:

* `compaction_window_unit`: A Java TimeUnit (MINUTES, HOURS, or DAYS). The default value is DAYS
* `compaction_window_size`: The number of units that make up a window. The default value is 1

Taken together, the operator can specify windows of virtually any size, and TimeWindowCompactionStrategy will work to create a single sstable for writes within that window. For efficiency during writing, the newest window will be compacted using SizeTieredCompactionStrategy.

Ideally, operators should select a `compaction_window_unit` and `compaction_window_size` pair that produces approximately 20-30 windows - if writing with a 90 day TTL, for example, a 3 Day window would be a reasonable choice (`'compaction_window_unit':'DAYS','compaction_window_size':3`).

On tables running TWCS it is adviced to disable read repair as this can mix old and new data in the same memtable and this creates sstables with long time spans. On the same note any clients to a TWCS table should avoid doing backfill of data or executing `USING TIMESTAMP` queries (or client equivalent) as this will also mix old and new data in the same memtable.

### Changing TimeWindowCompactionStrategy Options

Operators wishing to enable TimeWindowCompactionStrategy on existing data should consider running a major compaction first, placing all existing data into a single (old) window. Subsequent newer writes will then create typical SSTables as expected.

Operators wishing to change `compaction_window_unit` or `compaction_window_size` can do so, but may trigger additional compactions as adjacent windows are joined together. If the window size is decrease
d (for example, from 24 hours to 12 hours), then the existing SSTables will not be modified - TimeWindowCompactionStrategy can not split existing SSTables into multiple windows.
