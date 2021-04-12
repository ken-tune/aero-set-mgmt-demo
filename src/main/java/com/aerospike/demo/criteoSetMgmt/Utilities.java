package com.aerospike.demo.criteoSetMgmt;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Language;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Utilities {

    private static final long CITRUSLEAF_EPOCH = getCitrusLeafEpoch();

    /**
     * Throw this error if we have parsing exceptions
     */
    public static class ParseException extends Exception{
        ParseException(String message){
            super(message);
        }
    }

    /**
     * Convert Aerospike TTL to a datetime
     * @param expiration
     * @return
     */
    public static Date expirationToDateTime(long expiration){
        return new Date(1000 * expiration + CITRUSLEAF_EPOCH);
    }

    /***
     * Work out the citrusleaf epoch in terms of the Unix epoch
     * @return
     */
    private static final long getCitrusLeafEpoch(){
        long citrusLeafEpoch = 0;
        try {
            citrusLeafEpoch = new SimpleDateFormat("yyyy-MM-dd").parse("2010-01-01").getTime();
        }
        catch (java.text.ParseException e){
            // Can't throw error
        }
        return citrusLeafEpoch;
    }
}
