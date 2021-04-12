package com.aerospike.demo.criteoSetMgmt;

import org.apache.commons.cli.*;

public class OptionsHelper {
    /**
     * Command line options for Main class
     * @return cmdLineOptions
     */
    static Options cmdLineOptions() {
        Options cmdLineOptions = new Options();

        Option hostOption = new Option(Constants.HOST_FLAG, "host", true, "Aerospike seed host - no default");
        Option namespaceOption = new Option(Constants.NAMESPACE_FLAG, "namespace", true, "Namespace - no default");
        Option setOption = new Option(Constants.SET_FLAG, "set", true, "Set name - no default");
        Option tlsNameOption = new Option(Constants.TLS_NAME_FLAG, "tlsName", true, "TLS Name - no default");
        Option portOption = new Option(Constants.PORT_FLAG, "port", true,
                "Aerospike port. Default = " + getDefaultValue(Constants.PORT_FLAG));
        Option iterationsOption = new Option(Constants.RECORD_COUNT_FLAG,"recordCount",true,"No of recordCount to run for");
        Option modulusOption = new Option(Constants.MODULUS_FLAG,"countModulusForTTL",true,"Modulus to use for TTL vs iteration");
        Option hardObjectLimitOption = new Option(Constants.HARD_OBJECT_LIMIT_FLAG,"hardObjectSetLimit",true,"Hard object limit for sets");
        Option softObjectLimitOption = new Option(Constants.SOFT_OBJECT_LIMIT_FLAG,"softObjectLimit",true,"Soft object limit for sets");
        Option isEnterpriseOption = new Option(Constants.IS_ENTERPRISE_FLAG,"isEnterprise",false,"Using Aerospike Enterprise?");
        Option debugOption = new Option(Constants.DEBUG_FLAG,"debug",false,"Run in debug mode");

        hostOption.setRequired(true);
        namespaceOption.setRequired(true);
        setOption.setRequired(true);
        tlsNameOption.setRequired(false);
        portOption.setRequired(false);
        iterationsOption.setRequired(true);
        modulusOption.setRequired(true);
        hardObjectLimitOption.setRequired(true);
        softObjectLimitOption.setRequired(true);
        isEnterpriseOption.setRequired(false);
        debugOption.setRequired(false);

        cmdLineOptions.addOption(hostOption);
        cmdLineOptions.addOption(namespaceOption);
        cmdLineOptions.addOption(setOption);
        cmdLineOptions.addOption(tlsNameOption);
        cmdLineOptions.addOption(portOption);
        cmdLineOptions.addOption(iterationsOption);
        cmdLineOptions.addOption(modulusOption);
        cmdLineOptions.addOption(hardObjectLimitOption);
        cmdLineOptions.addOption(softObjectLimitOption);
        cmdLineOptions.addOption(isEnterpriseOption);
        cmdLineOptions.addOption(debugOption);

        return cmdLineOptions;
    }


    /**
     * Get default value for command line flags
     * @param flag
     * @return default value for flag
     */
    private static String getDefaultValue(String flag) {
        switch (flag) {
            case Constants.PORT_FLAG:
                return Integer.toString(Constants.AEROSPIKE_SERVER_PORT);
            default:
                return null;
        }
    }

    /**
     * Check type of supplied command line values
     * Throw an exception if there is a problem
     * @param flag
     * @param value
     * @throws Utilities.ParseException
     */
    private static void checkCommandLineArgumentType(String flag,String value) throws Utilities.ParseException{
        switch(flag){
            case Constants.RECORD_COUNT_FLAG:
            case Constants.MODULUS_FLAG:
            case Constants.HARD_OBJECT_LIMIT_FLAG:
            case Constants.SOFT_OBJECT_LIMIT_FLAG:
            case Constants.PORT_FLAG:
                try{
                    Integer.parseInt(value);
                }
                catch(NumberFormatException e){
                    throw new Utilities.ParseException(String.format("-%s flag should have an integer argument. Argument supplied is  %s",flag,value));
                }
                break;
        }
    }

    /**
     * Get option for optionFlag from a command line object, returning the default value if applicable
     * @param cmd
     * @param optionFlag
     * @return value for option flag
     */
    static String getOptionUsingDefaults(CommandLine cmd, String optionFlag) throws Utilities.ParseException{
        String value = cmd.getOptionValue(optionFlag, getDefaultValue(optionFlag));
        checkCommandLineArgumentType(optionFlag,value);
        return value;
    }

    /**
     * Return a CommandLine object given String[] args
     * @param args
     * @return CommandLine object
     * @throws ParseException
     */
    static CommandLine getArguments(String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        return parser.parse(cmdLineOptions(), args);
    }

}
