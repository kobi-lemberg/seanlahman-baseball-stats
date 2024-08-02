# seanlahman-baseball-stats
Simple Spark ETL code over Sean Lahman's baseball database

## Infra dependencies to run locally
 - Spark 3.5.0 compatible JAVA
 - Mysql or Docker 
   - There is a docker compose under mysql directory, see [instructions](mysql/MySQL_Docker.md)

## Instructions to run pre compiled code
1. unzip the attached ```seanlahman-baseball-stats-0.1.0-SNAPSHOT.zip``` file
2. run the following command
   ```shell
    cd seanlahman-baseball-stats-0.1.0-SNAPSHOT
    ./bin/seanlahman-baseball-stats -Dconfig.file=conf/application.conf -Dlemberg.kobi.slbs.stats.output-dir="/tmp/kuku"
    ```
3. /tmp/kuku is the output directory, you can change it to any directory you want
4. conf/application.conf is the configuration file
   1. you can modify parameters within the file or via JAVA OPTS (-D) (i.e spark master can be changed) 

## Instructions to run the code via IDEA/Recompile
1. you need SBT to compile the code
2. to compile the code, simply run
    ```shell
    sbt universal:dist
   ``` 
3. new ZIP file will be created under target/universal

## Run tests
Keep in mind that the tests are using Docker
```shell
sbt test
``` 