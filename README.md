# cs5424-wholesale
## Setup Development Environment
- Install IntelliJ IDEA
- Install Yugabyte

# cs5424-wholesale - ysql
## Run Project on Server

0. To Increase Server Timeout
  - `./bin/yugabyted stop`
  - `./bin/yugabyted start --tserver_flags="pg_yb_session_timeout_ms=60000000"`

1. To Drop Tables
  - `cd cs5424-wholesale/output`
  - `java -jar wholesale-ysql.jar drop`

2. To Create Tables
  - `cd cs5424-wholesale/output`
  - `java -jar wholesale-ysql.jar create`

3. To Load Data
  - `cd cs5424-wholesale/output`
  - `java -jar wholesale-ysql.jar load`

4. To Run Transactions
  - `cd cs5424-wholesale/output`
  - `java -jar wholesale-ysql.jar run`
  - 

# cs5424-wholesale - ycql
## Run Project on Server

0. To Increase Server Timeout
  - `./bin/yugabyted stop`
  - `./bin/yugabyted start --tserver_flags="pg_yb_session_timeout_ms=60000000"`

1. To Drop Tables
  - `cd cs5424-wholesale/output`
  - `java -jar wholesale-ycql.jar drop`

2. To Create Tables
  - `cd cs5424-wholesale/output`
  - `java -jar wholesale-ycql.jar create`

3. To Load Data
  - `cd cs5424-wholesale/output`
  - `java -jar wholesale-ycql.jar load`

4. To Run Transactions
  - `cd cs5424-wholesale/output`
  - `java -jar wholesale-ycql.jar run`
