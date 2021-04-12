package com.aerospike.demo.setMgmt;

import com.aerospike.client.Host;
import com.aerospike.client.AerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.TlsPolicy;
import org.apache.commons.cli.CommandLine;

public class AeroHelper {
    /**
     * Get an AerospikeClient object based on a parsed command line
     * @param cmd - CommandLine object
     * @return AerospikeClient
     * @throws Utilities.ParseException
     */
    static AerospikeClient getASClientFromOptions(CommandLine cmd) throws Utilities.ParseException{
        ClientPolicy clientPolicy = Main.requiredClientPolicy();
        if (cmd.hasOption(Constants.TLS_NAME_FLAG)) {
            clientPolicy.tlsPolicy = new TlsPolicy();
        }
        return new AerospikeClient(clientPolicy, getASHostFromOptions(cmd));
    }

    /**
     * Get an Aerospike Host object based on a parsed command line
     * @param cmd - CommandLine object
     * @return Aerospike Host object
     * @throws Utilities.ParseException
     */
    static Host getASHostFromOptions(CommandLine cmd) throws Utilities.ParseException{
        int port = Integer.parseInt(OptionsHelper.getOptionUsingDefaults(cmd, Constants.PORT_FLAG));
        String hostName = OptionsHelper.getOptionUsingDefaults(cmd, Constants.HOST_FLAG);
        Host host;
        if (cmd.hasOption(Constants.TLS_NAME_FLAG)) {
            host = new Host(hostName, cmd.getOptionValue(Constants.TLS_NAME_FLAG), port);
        } else {
            host = new Host(hostName, port);
        }
        return host;
    }

}
