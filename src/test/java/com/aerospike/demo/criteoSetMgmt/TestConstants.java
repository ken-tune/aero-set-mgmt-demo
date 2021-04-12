package com.aerospike.demo.criteoSetMgmt;

public class TestConstants {
    public static String HOST = "54.147.37.34";
    public static String NAMESPACE = "test";
    public static String SET = "testSet";
    public static int TLS_PORT = 4333;
    public static String TLS_NAME = "tls_aero";

    public static int ITERATIONS = 20;
    public static int MODULUS = 9;
    public static int HARD_OBJECT_SET_LIMIT = 2 * ITERATIONS / 3;
    public static int SOFT_OBJECT_SET_LIMIT = 3 * ITERATIONS / 5;
}
