COMPILING
====================================
# Requirements
* Java 8 or above

Akka libs and dependencies:
* akka-actor_2.11-2.4.4.jar
* config-1.3.0.jar
* scala-java8-compat_2.11-0.7.0.jar
* scala-library-2.11.8.jar

## Automatic Build
The application can be build by executing:

    build.sh

This will download the required libraries and build the application jar

## Manual Build
All those libraries can be downloaded from:
http://downloads.typesafe.com/akka/akka_2.11-2.4.4.zip

This application includes an ant script that will take care of compiling and packaging.
To generate the executable jar file run the ant command (without parameters) in the main project directory (where the build.xml file is).

RUNNING
====================================
The program expect the path to a simulation configuration file:

    java -jar cyclon.jar <configFile>

To enable log messages printing set the JVM flag "it.unitn.zozin.da.cyclon.debug" to ON:

    java -Dit.unitn.zozin.da.cyclon.debug=ON -jar cyclon.jar <configFile>

CONFIGURATION
====================================
A configuration file is a key-value text file with the following format:
```
#Number of nodes to simulate
nodes=

#Number of rounds the simulation has to run
rounds=

#Boot topology: chain, star or random
topology=

#Cache size used by Cyclon
cyclonCache=

#Number of entries to use in Cyclon shuffling messages
cyclonShuffle=

#Which property to measure: CLUSTERING, PATH_LEN or IN_DEGREE
measureType=

#When to measure the property: round or final
measureMode=
```
