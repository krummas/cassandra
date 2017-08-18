package org.apache.cassandra.metrics;


import com.codahale.metrics.Meter;

/**
 * Metrics about Tombstone Failures and Warnings
 */
public class TombstoneMetrics
{

    public static final TombstoneMetrics instance = new TombstoneMetrics();

    public static void init()
    {
        // no-op, just used to force instance creation
    }

    /** Number and rate of tombstone failures */
    protected final Meter failure;
    protected final Meter warning;

    private TombstoneMetrics()
    {

        failure = ClientMetrics.instance.addMeter("TombstoneFailures");
        warning = ClientMetrics.instance.addMeter("TombstoneWarnings");
    }

    public void markFailure()
    {
        failure.mark();
    }

    public void markWarning()
    {
        warning.mark();
    }
}
