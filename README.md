  Se446-project
 Project Milestone 1

---

 Team Members
Mishari Al Mogren,
Ibrahim Alhagbani,
Nawaf Alshuaibi,
Abdulaziz Alnemer,

---

 Executive Summary

Our team built a MapReduce pipeline on the department's Hadoop Cluster to analyze the Chicago Crime dataset (2001–present) containing over 793,000 records. Each task uses a dedicated Python mapper script that parses the CSV dataset and emits key-value pairs, combined with a shared reducer that aggregates counts per key. Jobs were executed using Hadoop. The pipeline answers four critical questions: what crimes are most common, where they occur, how crime volume changes over time, and how often arrests are made.

---

 Task 2 - Crime Type Distribution

Research Question: What are the most common types of crimes in Chicago?

 Instructions
```bash
mapred streaming \
  -files mapper_task2.py,reducer_sum.py \
  -mapper "python3 mapper_task2.py" \
  -reducer "python3 reducer_sum.py" \
  -input /data/chicago_crimes.csv \
  -output /user/mbsalmogren/project/m1/task2
```

 Sample Results (Top 5)
```
THEFT             162688
BATTERY           151930
CRIMINAL DAMAGE    91241
NARCOTICS          74127
ASSAULT            54070
```

 Interpretation
Theft is the most prevalent crime in Chicago, accounting for over 20% of all reported incidents, followed closely by battery, indicating that property and violent crimes dominate the city's crime landscape.

 Execution Logs
```
mbsalmogren@master-node:~$ mapred streaming \
  -files mapper_task2.py,reducer_sum.py \
  -mapper "python3 mapper_task2.py" \
  -reducer "python3 reducer_sum.py" \
  -input /data/chicago_crimes.csv \
  -output /user/mbsalmogren/project/m1/task2
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob2466354219308657753.jar tmpDir=null
2026-03-27 13:28:14,425 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-03-27 13:28:14,801 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-03-27 13:28:15,430 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/mbsalmogren/.staging/job_1771402826595_0258
2026-03-27 13:28:17,322 INFO mapred.FileInputFormat: Total input files to process : 1
2026-03-27 13:28:17,361 INFO net.NetworkTopology: Adding a new node: /default-rack/164.92.103.148:9866
2026-03-27 13:28:17,363 INFO net.NetworkTopology: Adding a new node: /default-rack/146.190.147.119:9866
2026-03-27 13:28:18,031 INFO mapreduce.JobSubmitter: number of splits:2
2026-03-27 13:28:19,145 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0258
2026-03-27 13:28:19,145 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-03-27 13:28:19,541 INFO conf.Configuration: resource-types.xml not found
2026-03-27 13:28:19,542 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-03-27 13:28:19,690 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0258
2026-03-27 13:28:19,755 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0258/
2026-03-27 13:28:19,762 INFO mapreduce.Job: Running job: job_1771402826595_0258
2026-03-27 13:28:36,471 INFO mapreduce.Job: Job job_1771402826595_0258 running in uber mode : false
2026-03-27 13:28:36,473 INFO mapreduce.Job:  map 0% reduce 0%
2026-03-27 13:29:03,887 INFO mapreduce.Job:  map 100% reduce 0%
2026-03-27 13:29:17,269 INFO mapreduce.Job:  map 100% reduce 100%
2026-03-27 13:29:21,330 INFO mapreduce.Job: Job job_1771402826595_0258 completed successfully
2026-03-27 13:29:21,623 INFO mapreduce.Job: Counters: 54
        File System Counters
                FILE: Number of bytes read=11798790
                FILE: Number of bytes written=24540827
                HDFS: Number of bytes read=181964998
                HDFS: Number of bytes written=690
                HDFS: Number of read operations=11
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=2
                Launched reduce tasks=1
                Data-local map tasks=2
        Map-Reduce Framework
                Map input records=793074
                Map output records=793072
                Reduce input groups=34
                Reduce output records=34
                Failed Shuffles=0
        File Input Format Counters
                Bytes Read=181964800
        File Output Format Counters
                Bytes Written=690
2026-03-27 13:29:21,628 INFO streaming.StreamJob: Output directory: /user/mbsalmogren/project/m1/task2
```

---

 Task 3 - Location Hotspots

Research Question: Where do most crimes occur?

 Instructions
``` bash
mapred streaming \
  -files mapper_task3.py,reducer_sum.py \
  -mapper "python3 mapper_task3.py" \
  -reducer "python3 reducer_sum.py" \
  -input /data/chicago_crimes.csv \
  -output /user/mbsalmogren/project/m1/task3
```

 Sample Results (Top 5)
```
STREET       245437
RESIDENCE    136238
APARTMENT     60925
SIDEWALK      47407
OTHER         29213
```

 Interpretation
Streets and residences are the most dangerous locations in Chicago, together accounting for nearly half of all reported crimes, indicating that patrol units should prioritize public streets and residential areas.

 Execution Logs
```
mbsalmogren@master-node:~$ mapred streaming \
  -files mapper_task3.py,reducer_sum.py \
  -mapper "python3 mapper_task3.py" \
  -reducer "python3 reducer_sum.py" \
  -input /data/chicago_crimes.csv \
  -output /user/mbsalmogren/project/m1/task3
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob11185273739708174112.jar tmpDir=null
2026-03-27 13:29:41,242 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-03-27 13:29:41,601 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-03-27 13:29:42,137 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/mbsalmogren/.staging/job_1771402826595_0259
2026-03-27 13:29:43,857 INFO mapred.FileInputFormat: Total input files to process : 1
2026-03-27 13:29:43,896 INFO net.NetworkTopology: Adding a new node: /default-rack/164.92.103.148:9866
2026-03-27 13:29:43,897 INFO net.NetworkTopology: Adding a new node: /default-rack/146.190.147.119:9866
2026-03-27 13:29:44,539 INFO mapreduce.JobSubmitter: number of splits:2
2026-03-27 13:29:45,610 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0259
2026-03-27 13:29:45,610 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-03-27 13:29:45,924 INFO conf.Configuration: resource-types.xml not found
2026-03-27 13:29:45,924 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-03-27 13:29:46,047 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0259
2026-03-27 13:29:46,102 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0259/
2026-03-27 13:29:46,105 INFO mapreduce.Job: Running job: job_1771402826595_0259
2026-03-27 13:30:06,080 INFO mapreduce.Job: Job job_1771402826595_0259 running in uber mode : false
2026-03-27 13:30:06,082 INFO mapreduce.Job:  map 0% reduce 0%
2026-03-27 13:30:35,912 INFO mapreduce.Job:  map 100% reduce 0%
2026-03-27 13:30:51,776 INFO mapreduce.Job:  map 100% reduce 100%
2026-03-27 13:30:54,632 INFO mapreduce.Job: Job job_1771402826595_0259 completed successfully
2026-03-27 13:30:54,869 INFO mapreduce.Job: Counters: 54
        File System Counters
                FILE: Number of bytes read=12341707
                FILE: Number of bytes written=25626664
                HDFS: Number of bytes read=181964998
                HDFS: Number of bytes written=4749
                HDFS: Number of read operations=11
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=2
                Launched reduce tasks=1
                Data-local map tasks=2
        Map-Reduce Framework
                Map input records=793074
                Map output records=793072
                Reduce input groups=217
                Reduce output records=216
                Failed Shuffles=0
        File Input Format Counters
                Bytes Read=181964800
        File Output Format Counters
                Bytes Written=4749
2026-03-27 13:30:54,870 INFO streaming.StreamJob: Output directory: /user/mbsalmogren/project/m1/task3
```

---

 Task 4 - Crime Trend by Year

Research Question: How has the total number of crimes changed over the years?

 Instructions
```bash
mapred streaming \
  -files mapper_task4.py,reducer_sum.py \
  -mapper "python3 mapper_task4.py" \
  -reducer "python3 reducer_sum.py" \
  -input /data/chicago_crimes.csv \
  -output /user/mbsalmogren/project/m1/task4
```

 Sample Results (Top 5)
```
2001    467301
2002    205267
2023     81461
2025     12710
2022      4678
```

 Interpretation
The dataset shows a large concentration of records in 2001 and 2002 due to bulk historical data imports during digitization, while more recent years show consistent crime counts with a gradual increase from 2020 onward.

 Execution Logs
```
mbsalmogren@master-node:~$ mapred streaming \
  -files mapper_task4.py,reducer_sum.py \
  -mapper "python3 mapper_task4.py" \
  -reducer "python3 reducer_sum.py" \
  -input /data/chicago_crimes.csv \
  -output /user/mbsalmogren/project/m1/task4
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob13022455992928888442.jar tmpDir=null
2026-03-27 13:31:04,574 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-03-27 13:31:04,975 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-03-27 13:31:05,477 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/mbsalmogren/.staging/job_1771402826595_0260
2026-03-27 13:31:07,342 INFO mapred.FileInputFormat: Total input files to process : 1
2026-03-27 13:31:07,379 INFO net.NetworkTopology: Adding a new node: /default-rack/164.92.103.148:9866
2026-03-27 13:31:07,381 INFO net.NetworkTopology: Adding a new node: /default-rack/146.190.147.119:9866
2026-03-27 13:31:08,106 INFO mapreduce.JobSubmitter: number of splits:2
2026-03-27 13:31:08,980 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0260
2026-03-27 13:31:08,980 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-03-27 13:31:09,301 INFO conf.Configuration: resource-types.xml not found
2026-03-27 13:31:09,302 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-03-27 13:31:09,418 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0260
2026-03-27 13:31:09,478 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0260/
2026-03-27 13:31:09,481 INFO mapreduce.Job: Running job: job_1771402826595_0260
2026-03-27 13:31:28,068 INFO mapreduce.Job: Job job_1771402826595_0260 running in uber mode : false
2026-03-27 13:31:28,071 INFO mapreduce.Job:  map 0% reduce 0%
2026-03-27 13:31:53,236 INFO mapreduce.Job:  map 100% reduce 0%
2026-03-27 13:32:08,078 INFO mapreduce.Job:  map 100% reduce 100%
2026-03-27 13:32:10,998 INFO mapreduce.Job: Job job_1771402826595_0260 completed successfully
2026-03-27 13:32:11,273 INFO mapreduce.Job: Counters: 54
        File System Counters
                FILE: Number of bytes read=7137663
                FILE: Number of bytes written=15218576
                HDFS: Number of bytes read=181964998
                HDFS: Number of bytes written=245
                HDFS: Number of read operations=11
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=2
                Launched reduce tasks=1
                Data-local map tasks=2
        Map-Reduce Framework
                Map input records=793074
                Map output records=793073
                Reduce input groups=25
                Reduce output records=25
                Failed Shuffles=0
        File Input Format Counters
                Bytes Read=181964800
        File Output Format Counters
                Bytes Written=245
2026-03-27 13:32:11,274 INFO streaming.StreamJob: Output directory: /user/mbsalmogren/project/m1/task4
```

---

 Task 5 - Arrest Rate Analysis

Research Question: What percentage of crimes result in an arrest?

 Instructions
```bash
mapred streaming \
  -files mapper_task5.py,reducer_sum.py \
  -mapper "python3 mapper_task5.py" \
  -reducer "python3 reducer_sum.py" \
  -input /data/chicago_crimes.csv \
  -output /user/mbsalmogren/project/m1/task5
```

 Sample Results
```
false    571140
true     221932
```

 Interpretation
Only 28% of crimes in Chicago result in an arrest, meaning that the vast majority of criminals are not caught, suggesting a need for improved law enforcement efficiency and resource allocation.

 Execution Logs
```
mbsalmogren@master-node:~$ mapred streaming \
  -files mapper_task5.py,reducer_sum.py \
  -mapper "python3 mapper_task5.py" \
  -reducer "python3 reducer_sum.py" \
  -input /data/chicago_crimes.csv \
  -output /user/mbsalmogren/project/m1/task5
packageJobJar: [] [/opt/hadoop-3.4.1/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar] /tmp/streamjob872011263419280130.jar tmpDir=null
2026-03-27 13:32:58,544 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-03-27 13:32:58,851 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at master-node/134.209.172.50:8032
2026-03-27 13:32:59,290 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/mbsalmogren/.staging/job_1771402826595_0261
2026-03-27 13:33:01,092 INFO mapred.FileInputFormat: Total input files to process : 1
2026-03-27 13:33:01,122 INFO net.NetworkTopology: Adding a new node: /default-rack/164.92.103.148:9866
2026-03-27 13:33:01,123 INFO net.NetworkTopology: Adding a new node: /default-rack/146.190.147.119:9866
2026-03-27 13:33:01,797 INFO mapreduce.JobSubmitter: number of splits:2
2026-03-27 13:33:03,130 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1771402826595_0261
2026-03-27 13:33:03,130 INFO mapreduce.JobSubmitter: Executing with tokens: []
2026-03-27 13:33:03,481 INFO conf.Configuration: resource-types.xml not found
2026-03-27 13:33:03,481 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2026-03-27 13:33:03,612 INFO impl.YarnClientImpl: Submitted application application_1771402826595_0261
2026-03-27 13:33:03,669 INFO mapreduce.Job: The url to track the job: http://master-node:8088/proxy/application_1771402826595_0261/
2026-03-27 13:33:03,673 INFO mapreduce.Job: Running job: job_1771402826595_0261
2026-03-27 13:33:22,440 INFO mapreduce.Job: Job job_1771402826595_0261 running in uber mode : false
2026-03-27 13:33:22,442 INFO mapreduce.Job:  map 0% reduce 0%
2026-03-27 13:33:46,809 INFO mapreduce.Job:  map 100% reduce 0%
2026-03-27 13:34:00,160 INFO mapreduce.Job:  map 100% reduce 100%
2026-03-27 13:34:04,235 INFO mapreduce.Job: Job job_1771402826595_0261 completed successfully
2026-03-27 13:34:04,461 INFO mapreduce.Job: Counters: 54
        File System Counters
                FILE: Number of bytes read=7768020
                FILE: Number of bytes written=16479284
                HDFS: Number of bytes read=181964998
                HDFS: Number of bytes written=875
                HDFS: Number of read operations=11
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=2
                Launched reduce tasks=1
                Data-local map tasks=2
        Map-Reduce Framework
                Map input records=793074
                Map output records=793072
                Reduce input groups=44
                Reduce output records=43
                Failed Shuffles=0
        File Input Format Counters
                Bytes Read=181964800
        File Output Format Counters
                Bytes Written=875
2026-03-27 13:34:04,466 INFO streaming.StreamJob: Output directory: /user/mbsalmogren/project/m1/task5
```

---

Member Contributions

| Mishari Al Mogren | GitHub setup,Task 2  |
| Nawaf Alshuaibi | Task 3 |
| Abdulaziz Al Nemer | Task 4 |
| Ibrahim Alhugbani | Task 5|
