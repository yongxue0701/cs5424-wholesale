# cs5424-wholesale
## Setup Development Environment
- Install IntelliJ IDEA
- Install Yugabyte

# cs5424-wholesale - ysql
## Run Project on Server

- Scripts to create schema & load data: `./output/scripts/ysql/*`
- Data: `./output/scripts/data/*`
- Demo: `./output/xact/demo.txt`

1. To Increase Server Timeout
  - `./bin/yugabyted stop`
  - `./bin/yugabyted start --tserver_flags="pg_yb_session_timeout_ms=60000000"`

2. To Drop Tables
  - `cd cs5424-wholesale/output`
  - `java -jar wholesale-ysql.jar drop`

3. To Create Tables
  - `cd cs5424-wholesale/output`
  - `java -jar wholesale-ysql.jar create`

4. To Load Data
  - `cd cs5424-wholesale/output`
  - `java -jar wholesale-ysql.jar load`

5. To Run Transactions
  - `cd cs5424-wholesale/output`
  - `java -jar wholesale-ysql.jar run`


# cs5424-wholesale - ycql
## Run Project on Server

- Scripts to create schema & load data: `./output/scripts/ycql/*`
- Data: `./output/scripts/data/*`
- Demo: `./output/xact/demo.txt`

1. To Increase Server Timeout
  - `./bin/yugabyted stop`
  - `./bin/yugabyted start --tserver_flags="pg_yb_session_timeout_ms=60000000"`

2. To Drop Tables
  - `cd cs5424-wholesale/output`
  - `java -jar wholesale-ycql.jar drop`

3. To Create Tables
  - `cd cs5424-wholesale/output`
  - `java -jar wholesale-ycql.jar create`

4. To Load Data
  - `cd cs5424-wholesale/output`
  - `java -jar wholesale-ycql.jar load`

5. To Run Transactions
  - `cd cs5424-wholesale/output`
  - `java -jar wholesale-ycql.jar run`
