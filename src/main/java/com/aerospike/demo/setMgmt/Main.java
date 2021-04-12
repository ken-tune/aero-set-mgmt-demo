package com.aerospike.demo.setMgmt;

import com.aerospike.client.*;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.*;
import com.aerospike.client.query.Statement;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.util.*;

/**
 * Given hard and soft limits, if we are over the hard limit for a given set (master objects)
 * then remove the records with the lowest time to live
 *
 * Class also performs setup - inserts recordCount records into specified namespace/set
 * with ttl set to recordIndex mod countModulusForTTL (in days)
 */
public class Main {
    // Default client policy
    private static final ClientPolicy DEFAULT_CLIENT_POLICY = new ClientPolicy();

    // CommandLine options
    CommandLine options;

    // Member variables - package level visibility
    String namespace;
    String set;
    int hardObjectLimit;
    int softObjectLimit;
    int recordCount;
    int countModulusForTTL;
    boolean isEnterprise = false;

    private boolean debug = true; // enables debug

    public static void main(String[] args) throws Utilities.ParseException, IOException {
        try {
            CommandLine cmd = OptionsHelper.getArguments(args);
            Main mainObject = new Main(cmd);
            mainObject.run();
        }
        catch(ParseException e){
            System.out.println(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Main",OptionsHelper.cmdLineOptions());
        }
    }

    /**
     * Constructor - takes options in CommandLine form
     * Do setup
     * @param cmd
     */
    Main(CommandLine cmd) throws Utilities.ParseException{
        options = cmd;
        namespace = OptionsHelper.getOptionUsingDefaults(options,Constants.NAMESPACE_FLAG);
        set = OptionsHelper.getOptionUsingDefaults(options,Constants.SET_FLAG);
        hardObjectLimit = Integer.parseInt(OptionsHelper.getOptionUsingDefaults(options,Constants.HARD_OBJECT_LIMIT_FLAG));
        softObjectLimit = Integer.parseInt(OptionsHelper.getOptionUsingDefaults(options,Constants.SOFT_OBJECT_LIMIT_FLAG));
        recordCount = Integer.parseInt(OptionsHelper.getOptionUsingDefaults(options,Constants.RECORD_COUNT_FLAG));
        countModulusForTTL = Integer.parseInt(OptionsHelper.getOptionUsingDefaults(options,Constants.MODULUS_FLAG));
        debug = options.hasOption(Constants.DEBUG_FLAG);
        isEnterprise = options.hasOption(Constants.IS_ENTERPRISE_FLAG);
        setup();
    }

    /**
     * Perform the record expiration
     * @throws Utilities.ParseException
     * @throws IOException
     */
    void run() throws Utilities.ParseException{
        AerospikeClient client = AeroHelper.getASClientFromOptions(options);
        Node[] nodes = client.getNodes();
        // For each node
        for(int nodeIndex = 0;nodeIndex<nodes.length;nodeIndex++) {
            Node node = nodes[nodeIndex];
            debug("Node",node.toString());
            // Get set info
            String[] setInfoArray = getSetInfo(node);
            for (int setIndex = 0; setIndex < setInfoArray.length; setIndex++) {
                // Extract namespace, set, object count, replication factor from setInfo
                String setInfo = setInfoArray[setIndex];
                String namespace = parameterValueFromInfo(setInfo,"ns",":");
                debug("Namespace",namespace);
                String setName = parameterValueFromInfo(setInfo,"set",":");
                debug("Set name",setName);
                int replicationFactor = getReplicationFactor(node,namespace);
                debug("Replication factor",replicationFactor);
                int setSize = Integer.parseInt(parameterValueFromInfo(setInfoArray[setIndex],"objects",":"));
                debug("Set Size",setSize);
                // Get ttl histogram
                String histogramReq = String.format("histogram:namespace=%s;set=%s;type=ttl", namespace, setName);
                String ttlHistogram = Info.request(nodes[0].getConnection(Constants.INFO_REQUEST_TIMEOUT), histogramReq);
                int bucketWidth = Integer.parseInt(parameterValueFromInfo(ttlHistogram,"bucket-width",":"));
                debug("Bucket width",bucketWidth);
                String[] buckets = parameterValueFromInfo(ttlHistogram,"buckets",":").split(",");
                // Work out node hard limit and soft limit
                int nodeHardLimit = hardObjectLimit * replicationFactor / nodes.length;
                debug("Node hard limit",nodeHardLimit);
                int nodeSoftLimit = softObjectLimit * replicationFactor / nodes.length;
                debug("Node soft limit",nodeSoftLimit);
                int truncateTTL = 0;

                // If we are over the hard limit
                if(setSize > nodeHardLimit){
                    int remainingSetSize = setSize;
                    // For each bucket
                    for(int bucketIndex = 0;bucketIndex < buckets.length;bucketIndex++){
                        int bucketCount = Integer.parseInt(buckets[bucketIndex]);
                        // If we can expire these records without hitting the soft limit, do it
                        if(remainingSetSize - bucketCount >= nodeSoftLimit){
                            truncateTTL += bucketWidth;
                            remainingSetSize-= bucketCount;
                            debug("Remaining set size",remainingSetSize);
                        }
                        else{
                            debug("Not reducing set size to ",remainingSetSize - bucketCount);
                            break;
                        }
                    }
                    // Output truncate info
                    System.out.println(String.format("Node %s, Set %s",node.getAddress().toString(),setName));
                    System.out.println(String.format("Truncating with TTL %d",truncateTTL));
                    System.out.println(String.format("Truncate timestamp %s",new Date(System.currentTimeMillis() + 1000 * truncateTTL)));
                    System.out.println(String.format("This will leave %d records",remainingSetSize));
                    System.out.println(String.format("Node hard Limit : %d",nodeHardLimit));
                    System.out.println(String.format("Node soft Limit : %d",nodeSoftLimit));

                    // Execute truncate
                    WritePolicy writePolicy = new WritePolicy();
                    Expression e = Exp.build(Exp.lt(Exp.ttl(),Exp.val(truncateTTL)));
                    writePolicy.filterExp = e;
                    // Use durable deletes if using Enterprise
                    if(isEnterprise) {
                        writePolicy.durableDelete = true;
                    }
                    Statement deleteStmt = new Statement();
                    deleteStmt.setNamespace(namespace);
                    deleteStmt.setSetName(set);
                    client.execute(writePolicy,deleteStmt,Operation.delete());
                }
            }
        }

    }

    /**
     * Insert recordCount records
     * TTL is record index mod countModulusForTTL
     */
    private void setup() throws Utilities.ParseException{
        AerospikeClient client = AeroHelper.getASClientFromOptions(options);
        client.truncate(null,namespace,set,null);
        // Insert 'recordCount' records
        // TTL = recordCount mod countModulusForTTL + 1 (days)
        for(int i = 0; i< recordCount; i++){
            WritePolicy p = new WritePolicy();
            p.expiration = 86400 * (1 + i % countModulusForTTL);
            client.put(p,aeroKeyFromInteger(i),new Bin(Constants.TTL_BIN_NAME,1 + i % countModulusForTTL));
        }
        // Set nsup-hist-period to a low value and wait to allow histogram to be made available
        Node[] nodes = client.getNodes();
        for(int nodeIndex = 0;nodeIndex<nodes.length;nodeIndex++) {
            Node node = nodes[nodeIndex];
            setNsupHistPeriod(node,OptionsHelper.getOptionUsingDefaults(options,Constants.NAMESPACE_FLAG),Constants.NSUP_HIST_PERIOD);
            debug(String.format("nsup-hist-period set to %d and waited for",Constants.NSUP_HIST_PERIOD));

        }
    }

    /**
     * asinfo related
     */

    /**
     * Get set info for node
     * @param node
     * @return
     */
    private static String[] getSetInfo(Node node) {
        return Info.request(node.getConnection(Constants.INFO_REQUEST_TIMEOUT), "sets").split(";");
    }

    /**
     * Set nsup period for node and wait
     * @param node
     * @param namespace
     * @param period
     */
    private static void setNsupHistPeriod(Node node,String namespace, int period){
        String command = String.format("set-config:context=namespace;id=%s;nsup-hist-period=%d",namespace,period);
        Info.request(node,command);
        long currentTime = System.currentTimeMillis();
        while(System.currentTimeMillis() < currentTime + period * 1000){};
    }

    /**
     * Get replication factor for namespace for this node
     * If not found will equal zero
     * @param node
     * @param namespace
     * @return
     */
    private static int getReplicationFactor(Node node,String namespace){
        String infoRequestString = String.format("namespace/%s",namespace);
        String namespaceInfo = Info.request(node.getConnection(Constants.INFO_REQUEST_TIMEOUT),infoRequestString);
        return Integer.parseInt(parameterValueFromInfo(namespaceInfo,"effective_replication_factor",";"));
    }

    /**
     * Get parameter value from aeroInfo - supply delimiter needed to split the string
     * @param aeroInfo
     * @param parameterName
     * @param delimiter
     * @return
     */
    private static String parameterValueFromInfo(String aeroInfo,String parameterName,String delimiter){
        String parameterValue = null;
        String[] infoArray = aeroInfo.split(delimiter);
        for(int i=0;i<infoArray.length;i++){
            String[] infoParts = infoArray[i].split("=");
            if(infoParts[0].equals(parameterName)) parameterValue = infoParts[1];
        }
        return parameterValue;
    }

    /**
     * Setup related
     */

    /**
     * From the supplied integer, construct a key in the form required for this application
     * @param i
     * @return
     */
    Key aeroKeyFromInteger(int i){
        Key aeroKey = null;
        try {
            aeroKey =  new Key(
                    OptionsHelper.getOptionUsingDefaults(options, Constants.NAMESPACE_FLAG),
                    OptionsHelper.getOptionUsingDefaults(options, Constants.SET_FLAG),
                    i
            );
        }
        catch (Utilities.ParseException e){
            System.err.println("Unexpected parse exception");
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return aeroKey;
    }

    /**
     * Client Policy
     * @return
     */
    static ClientPolicy requiredClientPolicy(){
        return DEFAULT_CLIENT_POLICY;
    }

    /**
     * Utility functions for debug purposes
     * @param message
     */
    private void debug(String message){
        if(debug) System.out.println(message);
    }

    private void debug(String parameterName,String value){
        debug(String.format("%s: %s",parameterName,value));
    }

    private void debug(String parameterName, int value){
        debug(parameterName, Integer.toString(value));
    }


}
