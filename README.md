# Aerospike set management demo

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

## Sample usage and output
```
java -jar set-mgmt-demo-1.0-SNAPSHOT-jar-with-dependencies.jar -c 1000 -m 23 -hl 500 -sl 400 -h localhost -n test -s testSet
Node localhost/127.0.0.1:3000, Set testSet
Truncating with TTL 1192320
Truncate timestamp Mon Apr 26 10:36:41 UTC 2021
This will leave 273 records
Node hard Limit : 333
Node soft Limit : 266
Node /10.0.1.66:3000, Set testSet
Truncating with TTL 854496
Truncate timestamp Thu Apr 22 12:46:17 UTC 2021
This will leave 284 records
Node hard Limit : 333
Node soft Limit : 266
Node /10.0.0.229:3000, Set testSet
Truncating with TTL 337824
Truncate timestamp Fri Apr 16 13:15:05 UTC 2021
This will leave 293 records
Node hard Limit : 333
Node soft Limit : 266
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

