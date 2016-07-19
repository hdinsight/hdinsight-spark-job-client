# hdinsight-spark-job-client
A client for submitting Spark job to HDInsight cluster remotely.

**Submit Spark job (and exit):**

Usage: java -cp SparkJobClient.jar com.microsoft.spark.client.SparkClientMain
--submit --applicationName [applicationName] 
--application-jar [applicationJAR]
--application-class [applicationClass]
--application-arguments  [applicationArguments]
--jars-in-classpath [classPathJARS]
--files-in-executor-working-directory [executorFiles]
--executor-count [executorCount]
--per-executor-core-count [perExecutorCoreCount] 
--per-executor-memory-in-gb [perExecutorMemoryInGB]
--driver-memory-in-gb [driverMemoryInGB] 
--cluster-fqdn [clusterFQDN (without any prefix)] 
--cluster-username [clusterUsername]
--cluster-password [clusterPassword] 
[--batch-mode (default)|--interactive-mode]

**Run Spark job (and exit):**

Usage: java -cp SparkJobClient.jar com.microsoft.spark.client.SparkClientMain
--run --applicationName [applicationName] 
--application-jar [applicationJAR]
--application-class [applicationClass]
--application-arguments  [applicationArguments]
--jars-in-classpath [classPathJARS]
--files-in-executor-working-directory [executorFiles]
--executor-count [executorCount]
--per-executor-core-count [perExecutorCoreCount] 
--per-executor-memory-in-gb [perExecutorMemoryInGB]
--driver-memory-in-gb [driverMemoryInGB] 
--cluster-fqdn [clusterFQDN (without any prefix)] 
--cluster-username [clusterUsername]
--cluster-password [clusterPassword] 
[--batch-mode (default)|--interactive-mode]

**Monitor Spark job:**

Usage: java -cp SparkJobClient.jar com.microsoft.spark.client.SparkClientMain
--monitor|--kill --job-id [jobId] 
--cluster-fqdn [clusterFQDN (without any prefix)] 
--cluster-username [clusterUsername]
--cluster-password [clusterPassword] 
[--batch-mode (default)|--interactive-mode]

**List Spark jobs:**

Usage: java -cp SparkJobClient.jar com.microsoft.spark.client.SparkClientMain
--list --cluster-fqdn [clusterFQDN] 
--cluster-username [clusterUsername]
--cluster-password [clusterPassword] 
[--batch-mode (default)|--interactive-mode]