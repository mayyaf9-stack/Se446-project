# SE446 Milestone 2:

## 1. Team Members

| Name | ID |
|---|---:|
| Mishari Al Mogren | 230142 |
| Ibrahim Alhagbani | 230597 |
| Nawaf Alshuaibi | 230146 |
| Abdulaziz Alnemer | 230380 |
| Abdulaziz Albaz | 230631 |

## 2. Executive Summary

This project upgrades the Milestone 1 Hadoop MapReduce crime analytics workflow into a Spark-based analytics and machine learning pipeline. Spark was used for data aggregation, SQL analysis, feature engineering, and MLlib model training to predict whether a crime results in an arrest. The final pipeline was tested locally, in YARN client mode, and with spark-submit in YARN cluster mode.

## 3. M1 vs M2 Comparison

Milestone 1 used Hadoop Streaming MapReduce with separate mapper and reducer scripts. Milestone 2 used Spark DataFrames and Spark SQL, which made the same analytics shorter, easier to read, and easier to connect with MLlib.

| Task | M1 MapReduce Result | M2 Spark Result | Same Numbers? | Faster / Easier? |
|---|---|---|---|---|
| Task 1: Crime Type Distribution | THEFT 162688, BATTERY 151930, CRIMINAL DAMAGE 91241, NARCOTICS 74127, ASSAULT 54070 | THEFT 162688, BATTERY 151930, CRIMINAL DAMAGE 91241, NARCOTICS 74127, ASSAULT 54070 | Yes | Spark was easier because it used groupBy().count().orderBy() instead of separate mapper and reducer files. |
| Task 2: Location Hotspots | STREET 245437, RESIDENCE 136238, APARTMENT 60925, SIDEWALK 47407, OTHER 29213 | STREET 248326, RESIDENCE 136393, APARTMENT 61235, SIDEWALK 47506, OTHER 29671 | Mostly close, but not exactly | Spark SQL was easier because the query used normal GROUP BY syntax. The numbers are close because both runs used the Chicago crimes dataset, but the available cluster file during M2 had slightly different counts. |
| Task 3: Crime Trend | 2001 had the highest count, followed by 2002 | 2001 = 467301, 2002 = 205266, 2023 = 81461, 2025 = 12710 | Mostly yes | Spark was easier because yearly grouping was done directly with DataFrame aggregation and sorting. |
| Task 4: Arrest Rate | MapReduce counted arrest values and categories manually | Total rows = 793073, Arrest rows = 221932, Overall arrest rate = 0.2798 | Yes in logic | Spark was easier because arrest rate could be calculated directly as a proportion using DataFrame operations. |

## 4. ML Results Summary

The ML pipeline used Spark MLlib to predict whether a crime record results in an arrest. The feature pipeline encoded categorical columns and combined multiple features into one features vector.

Features used:

text
District
Primary Type
Hour
Domestic


Task 11 cluster-mode spark-submit model results:

| Model | AUC | Accuracy | F1 | Precision | Recall | Training Time |
|---|---:|---:|---:|---:|---:|---:|
| Logistic Regression | 0.624 | 0.722 | 0.627 | 0.692 | 0.722 | 15.17s |
| Random Forest | 0.820 | 0.811 | 0.775 | 0.849 | 0.811 | 19.38s |
| GBT | 0.833 | 0.853 | 0.839 | 0.865 | 0.853 | 76.52s |

The best model was *GBT* because it had the highest AUC, accuracy, and F1 score.

text
Best model: GBT
AUC: 0.833
Accuracy: 0.853
F1: 0.839


Interpretation: GBT performed best because tree-based models can capture nonlinear patterns between crime type, district, time, and domestic status. Logistic Regression was faster, but weaker because indexed categorical values are not naturally ordered numbers. Random Forest also performed well and was faster than GBT, but GBT gave the best predictive performance overall.
## 5. Deployment Evidence

All deployment evidence files are included in the `output/` folder.

The `output/` folder contains evidence for:

| Task | Execution Mode | Evidence Location |
|---|---|---|
| Task 9 | Local execution using `local[*]` | `output/task9-mishari-colab-output.pdf` |
| Task 10 | Cluster execution using YARN client mode | `output/cluster_client/` |
| Task 11 | `spark-submit` using YARN cluster mode | `output/spark_submit/` |

Task 9 evidence shows local Spark execution with the generated 10,000-row dataset.

Task 10 evidence shows cluster execution with:

```text
Master: yarn
Data path: hdfs:///data/chicago_crimes.csv
Cluster row count: 793073
```

Task 11 evidence shows successful `spark-submit` execution with:

```text
Application ID: application_1778738889964_0067
ApplicationMaster host: worker-node-1
final status: SUCCEEDED
```

The full Task 11 terminal output and YARN logs are saved in:

```text
output/spark_submit/task11_submit_terminal.log
output/spark_submit/run.log
```

## 6. Member Contributions

| Member | Contribution |
|---|---|
| Mishari Al Mogren | Task 9 local execution, Task 10 cluster client execution, Task 11 spark-submit cluster execution, deployment evidence, final integration |
| Abdulaziz Albaz | Task 1 crime type distribution, Task 6 model training and evaluation |
| Abdulaziz Alnemer | Task 2 location hotspot analysis using Spark SQL |
| Ibrahim Alhagbani | Task 3 crime trend analysis, Task 5 feature engineering pipeline |
| Nawaf Alshuaibi | Task 4 arrest rate analysis, Task 7 feature importance interpretation |

## 7. spark-submit Terminal Output

Full Task 11 terminal output:

```text
mbsalmogren@master-node:~/SE446_Milestone2_Cluster_Tasks$ mkdir -p output/spark_submit

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 465m \
  --num-executors 1 \
  --executor-memory 768m \
  --executor-cores 1 \
  --conf spark.driver.memoryOverhead=32m \
  --conf spark.yarn.am.memory=465m \
  --conf spark.yarn.am.memoryOverhead=32m \
  --conf spark.driver.maxResultSize=128m \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3.12 \
  --conf spark.executorEnv.PYSPARK_PYTHON=python3.12 \
  m2_spark_ml.py hdfs:///data/chicago_crimes.csv \
  2>&1 | tee output/spark_submit/task11_submit_terminal.log

26/05/21 19:08:44 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
26/05/21 19:08:45 INFO DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
26/05/21 19:08:46 INFO Configuration: resource-types.xml not found
26/05/21 19:08:46 INFO ResourceUtils: Unable to find 'resource-types.xml'.
26/05/21 19:08:46 INFO Client: Verifying our application has not requested more than the maximum memory capability of the cluster (1536 MB per container)
26/05/21 19:08:46 INFO Client: Will allocate AM container, with 497 MB memory including 32 MB overhead
26/05/21 19:08:46 INFO Client: Setting up container launch context for our AM
26/05/21 19:08:46 INFO Client: Setting up the launch environment for our AM
26/05/21 19:08:46 INFO Client: Preparing resources for our AM container
26/05/21 19:08:50 INFO Client: Uploading resource file:/home/mbsalmogren/SE446_Milestone2_Cluster_Tasks/m2_spark_ml.py -> hdfs://master-node:9000/user/mbsalmogren/.sparkStaging/application_1778738889964_0067/m2_spark_ml.py
26/05/21 19:08:53 INFO Client: Submitting application application_1778738889964_0067 to ResourceManager
26/05/21 19:08:53 INFO YarnClientImpl: Submitted application application_1778738889964_0067
26/05/21 19:08:54 INFO Client: Application report for application_1778738889964_0067 (state: ACCEPTED)
26/05/21 19:08:54 INFO Client:
     diagnostics: AM container is launched, waiting for AM container to Register with RM
     final status: UNDEFINED
     tracking URL: http://master-node:8088/proxy/application_1778738889964_0067/
     user: mbsalmogren
26/05/21 19:09:14 INFO Client: Application report for application_1778738889964_0067 (state: RUNNING)
26/05/21 19:09:14 INFO Client:
     diagnostics: N/A
     ApplicationMaster host: worker-node-1
     ApplicationMaster RPC port: 34943
     final status: UNDEFINED
     tracking URL: http://master-node:8088/proxy/application_1778738889964_0067/
     user: mbsalmogren
26/05/21 19:09:44 INFO Client: Application report for application_1778738889964_0067 (state: RUNNING)
26/05/21 19:10:14 INFO Client: Application report for application_1778738889964_0067 (state: RUNNING)
26/05/21 19:10:44 INFO Client: Application report for application_1778738889964_0067 (state: RUNNING)
26/05/21 19:11:14 INFO Client: Application report for application_1778738889964_0067 (state: RUNNING)
26/05/21 19:11:44 INFO Client: Application report for application_1778738889964_0067 (state: RUNNING)
26/05/21 19:12:14 INFO Client: Application report for application_1778738889964_0067 (state: RUNNING)
26/05/21 19:12:45 INFO Client: Application report for application_1778738889964_0067 (state: RUNNING)
26/05/21 19:13:04 INFO Client: Application report for application_1778738889964_0067 (state: FINISHED)
26/05/21 19:13:04 INFO Client:
     diagnostics: N/A
     ApplicationMaster host: worker-node-1
     ApplicationMaster RPC port: 34943
     final status: SUCCEEDED
     tracking URL: http://master-node:8088/proxy/application_1778738889964_0067/
     user: mbsalmogren
26/05/21 19:13:04 INFO ShutdownHookManager: Shutdown hook called
```

The detailed Task 11 YARN output is saved in:

```text
output/spark_submit/run.log
```

Important lines from `run.log`:

```text
Tasks 5-7 and Task 11: Spark ML Pipeline
Task 5: Feature vector sample
Training Logistic Regression
Training Random Forest
Training GBT
Task 6: Model comparison
Model                      AUC     Acc      F1    Prec  Recall     Time   TN   FP   FN   TP
Logistic Regression      0.624   0.722   0.627   0.692   0.722    15.17 5514   78 2092  121
Random Forest            0.820   0.811   0.775   0.849   0.811    19.38 5589    3 1472  741
GBT                      0.833   0.853   0.839   0.865   0.853    76.52 5505   87 1057 1156
Task 7: Random Forest feature importance
Task 11 complete
```
