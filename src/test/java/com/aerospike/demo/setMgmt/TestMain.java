package com.aerospike.demo.setMgmt;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class TestMain {
    @Test
    public void checkNonTLSHostSetCorrectly() {
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%d -%s %%d -%s %%d -%s %%d",
                Constants.HOST_FLAG, Constants.NAMESPACE_FLAG,Constants.SET_FLAG,Constants.RECORD_COUNT_FLAG,Constants.MODULUS_FLAG,
                Constants.HARD_OBJECT_LIMIT_FLAG,Constants.SOFT_OBJECT_LIMIT_FLAG);

        String commandLineArguments =
                String.format(formatString,
                        TestConstants.HOST, TestConstants.NAMESPACE,TestConstants.SET,TestConstants.ITERATIONS,TestConstants.MODULUS,
                        TestConstants.HARD_OBJECT_SET_LIMIT,TestConstants.SOFT_OBJECT_SET_LIMIT);

        CommandLine cmdLine = commandLineObjectFromString(commandLineArguments);
        Host host = null;
        try {
            host = AeroHelper.getASHostFromOptions(cmdLine);
        } catch (Utilities.ParseException e) {
            Assert.fail("Parse exception should not happen");
        }
        assertEquals(TestConstants.HOST, host.name);
        assertEquals(Constants.DEFAULT_PORT, host.port);

    }

    @Test
    public void checkTLSHostSetCorrectly() {
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%d  -%s %%d  -%s %%d -%s %%d -%s %%s -%s %%d",
                Constants.HOST_FLAG, Constants.NAMESPACE_FLAG, Constants.SET_FLAG, Constants.RECORD_COUNT_FLAG,Constants.MODULUS_FLAG,
                Constants.HARD_OBJECT_LIMIT_FLAG,Constants.SOFT_OBJECT_LIMIT_FLAG,Constants.TLS_NAME_FLAG, Constants.PORT_FLAG);

        String commandLineArguments =
                String.format(formatString,
                        TestConstants.HOST, TestConstants.NAMESPACE, TestConstants.SET,TestConstants.ITERATIONS,TestConstants.MODULUS,
                        TestConstants.HARD_OBJECT_SET_LIMIT,TestConstants.SOFT_OBJECT_SET_LIMIT,TestConstants.TLS_NAME, TestConstants.TLS_PORT);

        CommandLine cmdLine = commandLineObjectFromString(commandLineArguments);

        Host host = null;
        try {
            host = AeroHelper.getASHostFromOptions(cmdLine);
        } catch (Utilities.ParseException e) {
            Assert.fail("Parse exception should not happen");
        }

        assertEquals(TestConstants.HOST, host.name);
        assertEquals(TestConstants.TLS_PORT, host.port);
        assertEquals(TestConstants.TLS_NAME, host.tlsName);
    }

    @Test
    public void confirmBehaviour(){
        // If hardLimit is more than number of records, don't truncate
        checkTruncateArithmetic(20,7,21,20,20);
        // If hardLimit = number of records, don't truncate
        checkTruncateArithmetic(20,7,21,20,20);

        // If we are over limit, truncate, shouldn't go under soft limit
        checkTruncateArithmetic(20,7,19,19,20);
        checkTruncateArithmetic(20,7,19,18,20);
        checkTruncateArithmetic(20,7,19,17,17);
        checkTruncateArithmetic(20,7,19,16,17);
    }

    /**
     * 1) Test that actual remaining count is the expected count
     * 2) Check that soft limit is respected
     * @param recordCount
     * @param modulus
     * @param hardLimit
     * @param softLimit
     * @param expectedCount
     */
    public void checkTruncateArithmetic(int recordCount, int modulus,int hardLimit,int softLimit,int expectedCount){
        String formatString = String.format("-%s %%s -%s %%s -%s %%s -%s %%d -%s %%d -%s %%d -%s %%d",
                Constants.HOST_FLAG, Constants.NAMESPACE_FLAG,Constants.SET_FLAG,Constants.RECORD_COUNT_FLAG,Constants.MODULUS_FLAG,
                Constants.HARD_OBJECT_LIMIT_FLAG,Constants.SOFT_OBJECT_LIMIT_FLAG);

        String commandLineArguments =
                String.format(formatString,
                        TestConstants.HOST, TestConstants.NAMESPACE,TestConstants.SET,recordCount,modulus, hardLimit,softLimit);

        CommandLine cmdLine = commandLineObjectFromString(commandLineArguments);

        try {
            Main main  = new Main(cmdLine);
            main.run();
        }
        catch(Utilities.ParseException e){
            System.out.println(e.getMessage());
            Assert.fail("Parse exception - shouldn't happen");
        }
        int remainingRecordCount = countRecordsForSet(getAerospikeClient(cmdLine),TestConstants.NAMESPACE,TestConstants.SET);
        Assert.assertEquals(expectedCount,remainingRecordCount);
        Assert.assertTrue(recordCount >= softLimit);
    }

    /**
     * Utility testing method - return a CommandLine object based on command line arguments
     *
     * @param argsString
     * @return CommandLine object
     */
    private static CommandLine commandLineObjectFromString(String argsString) {
        String[] args = argsString.split(" ");
        CommandLine cmdLine = null;
        try {
            cmdLine = OptionsHelper.getArguments(args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            Assert.fail("Shouldn't throw a parse exception");
        }
        return cmdLine;
    }

    /**
     * Utility method for testing - return an Aerospike client based on test constants
     *
     * @return AerospikeClient
     */
    private static AerospikeClient getAerospikeClient(CommandLine cmd) {

        AerospikeClient client = null;
        try {
            client = AeroHelper.getASClientFromOptions(cmd);
        } catch (Utilities.ParseException e) {
            System.out.println(e.getMessage());
            Assert.fail("This shouldn't happen");
        }
        return client;
    }

    private static int countRecordsForSet(AerospikeClient client, String namespace, String set){
        Statement countStmt = new Statement();
        countStmt.setNamespace(namespace);
        countStmt.setSetName(set);
        RecordSet recordSet = client.query(null,countStmt);
        int remainingRecordCount = 0;
        Iterator i = recordSet.iterator();
        while(i.hasNext()) {
            remainingRecordCount++;
            i.next();
        }
        return remainingRecordCount;
    }
}
