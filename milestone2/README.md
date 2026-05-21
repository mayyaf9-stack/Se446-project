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
