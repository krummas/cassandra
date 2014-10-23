package org.apache.cassandra.repair;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Keeps track of the progress of an ongoing @code RepairJob and
 * writes this to a distributed system table
 */

public class RepairResultPersister
{
    private static Logger logger = LoggerFactory.getLogger(RepairResultPersister.class);

    public static final String REPAIRS_CF = "repair_history";
    private final UUID id;
    private final String keyspace;
    private final String[] cfnames;
    private final Set<InetAddress> endpoints;
    private final Range<Token> range;

    public RepairResultPersister(UUID id, String keyspace, String[] cfnames, Set<InetAddress> endpoints, Range<Token> range)
    {
        this.id = id;
        this.keyspace = keyspace;
        this.cfnames = cfnames;
        this.endpoints = endpoints;
        this.range = range;
    }

    public void failure(Throwable t)
    {
        String query = "UPDATE %s.%s SET status = '%s', finished_at = dateOf(now()), exception_message='%s', exception_stacktrace='%s' WHERE keyspace_name = '%s' AND columnfamily_name = '%s' AND id = %s";
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        for (String cf : cfnames)
        {
            String fmtQry = String.format(query,
                    Keyspace.DISTRIBUTED_SYSTEM_KS,
                    REPAIRS_CF,
                    State.SUCCESS.toString(),
                    t.getMessage(),
                    sw.toString(),
                    keyspace,
                    cf,
                    id.toString()
            );
            execute(fmtQry);
        }
    }

    public void success()
    {
        String query = "UPDATE %s.%s SET status = '%s', finished_at = dateOf(now()) WHERE keyspace_name = '%s' AND columnfamily_name = '%s' AND id = %s";
        for (String cf : cfnames)
        {
            String fmtQry = String.format(query,
                    Keyspace.DISTRIBUTED_SYSTEM_KS,
                    REPAIRS_CF,
                    State.SUCCESS.toString(),
                    keyspace,
                    cf,
                    id.toString()
            );
            execute(fmtQry);
        }
    }

    private enum State
    {
        STARTED, SUCCESS, FAILURE
    }

    public void start()
    {
        String coordinator = FBUtilities.getBroadcastAddress().getHostAddress();
        Set<String> participants = Sets.newHashSet(coordinator);
        for (InetAddress endpoint : endpoints)
        {
            participants.add(endpoint.getHostAddress());
        }

        String query =
                "INSERT INTO %s.%s (keyspace_name, columnfamily_name, id, range_begin, range_end, coordinator, participants, status, started_at) " +
                "VALUES ('%s', '%s', %s, '%s', '%s', '%s', { '%s' }, '%s', dateOf(now()))";

        for (String cf : cfnames)
        {
            String fmtQry = String.format(query,
                    Keyspace.DISTRIBUTED_SYSTEM_KS,
                    REPAIRS_CF,
                    keyspace,
                    cf,
                    id.toString(),
                    range.left.toString(),
                    range.right.toString(),
                    coordinator,
                    Joiner.on("', '").join(participants),
                    State.STARTED.toString()
            );
            execute(fmtQry);
        }
    }

    private static void execute(String fmtQry)
    {
        try
        {
            QueryProcessor.process(fmtQry, ConsistencyLevel.ANY);
        }
        catch (Throwable ree)
        {
            logger.warn("Unable to store info about repair job.", ree);
        }
    }
}
