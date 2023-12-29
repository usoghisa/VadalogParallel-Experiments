# Vadalog Parallel Experiments

This repository contains materials and instructions to reproduce the experiments presented in the VLDB paper on the Vadalog Parallel System: Distributed Reasoning with Datalog+/-.

## System Requirements

To replicate the experiments, ensure your system meets the following requirements:
- Ubuntu machine v20
- Java 11
- Python 3
- Spark 3.4.0
- Flink 1.17.1
- Maven
- PostgreSQL 16 (with parallelism enabled)

## Compilation and Running Experiments

#### 1. Compilation
```bash
cd VadalogParallel-Experiments/Code
mvn -f pom.xml clean package -DskipTests
mv target/Code-1.0.0-jar-with-dependencies.jar target/Code.jar
```

#### 2. Display available commands
Navigate to the project base directory:
```bash
cd ../
```
Display available launcher scripts for running experiments with different systems:
```bash
cat Common-scripts/command.txt
```
Execute the first launcher script to get available commands for experiments:

```bash
python3 Common-scripts/launcher.py --platforms_file Common-scripts/platforms.conf --jobs_file Common-scripts/jobs.conf --log_file Common-scripts/log.txt --to_run False
```
This command will display available commands for running experiments with different systems.

#### 3. Execution

For example, to run the "Close Links" program with the system JAVA STREAM PARALLEL on the company graph with a size of 50k, use the following command:
```bash
java -Xmx500g -cp Code/target/Code.jar prometheuxresearch.benchmark.vldb.experiment.javastream.closeLink.CloseLinkJavaStream --parallelism 96 --input Code/src/test/resources/programs_data/companygraph/company_graph_50k.csv
```
