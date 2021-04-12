# Set management Demo

This excecutable implements the following vs Aerospike

1. Insert recordCount records. TTL in days  = recordCount mod countModulusForTTL

2. If recordCount > hardObjectLimit

   i)	Identify records with lowest TTL & remove if remaining record count >= softObjectLimit

   ii)	Repeat step 1 until not possible to satisfy remaining record count >= softObjectLimit
   
## Usage

```bash
java -jar ./target/set-mgmt-demo-1.0-SNAPSHOT-jar-with-dependencies.jar
 -c,--recordCount <arg>           No of recordCount to run for
 -d,--debug                       Run in debug mode
 -e,--isEnterprise                Using Aerospike Enterprise?
 -h,--host <arg>                  Aerospike seed host - no default
 -hl,--hardObjectSetLimit <arg>   Hard object limit for sets
 -m,--countModulusForTTL <arg>    Modulus to use for TTL vs iteration
 -n,--namespace <arg>             Namespace - no default
 -p,--port <arg>                  Aerospike port. Default = 3000
 -s,--set <arg>                   Set name - no default
 -sl,--softObjectLimit <arg>      Soft object limit for sets
 -t,--tlsName <arg>               TLS Name - no default

```

## Build Instructions

```bash
mvn clean compile assembly:single
```

## Unit Tests

Change 

```java
TestConstants.Host
TestConstants.Namespace
TestConstants.Set
```

as needed to run unit tests

