package com.aerospike.demo.criteoSetMgmt;

public class Constants {
    public static final int AEROSPIKE_SERVER_PORT = 3000;

    /*
     *   Command line flags
     */
    public static final String HOST_FLAG = "h";
    public static final String NAMESPACE_FLAG = "n";
    public static final String SET_FLAG = "s";
    public static final String TLS_NAME_FLAG = "t";
    public static final String PORT_FLAG = "p";
    public static final String RECORD_COUNT_FLAG = "c";
    public static final String MODULUS_FLAG = "m";
    public static final String HARD_OBJECT_LIMIT_FLAG = "hl";
    public static final String SOFT_OBJECT_LIMIT_FLAG = "sl";
    public static final String IS_ENTERPRISE_FLAG = "e";
    public static final String DEBUG_FLAG = "d";


    // Defaults
    public static final int DEFAULT_PORT = AEROSPIKE_SERVER_PORT;

    // Bin name for test application
    public static final String TTL_BIN_NAME = "ttlInDays";

    // nsup histogram period
    public static final int NSUP_HIST_PERIOD = 1;

    // Info request timeout
    static final int INFO_REQUEST_TIMEOUT = 1000;
}
