# hdinsight-spark-job-client
A client for submitting Spark job to HDInsight cluster remotely.

**For submitting or submitting and monitoring Spark job:**

Usage: java -cp SparkJobClient.jar com.microsoft.spark.job.client.LivyClient
--submit|--submit-and-monitor --applicationName [applicationName]
--application-jar [applicationJAR] --application-class [applicationClass]
--application-arguments  [applicationArguments] --jars-in-classpath [classPathJARS]
--files-in-executor-working-directory [executorFiles] --executor-count [executorCount]
--per-executor-core-count [perExecutorCoreCount] --per-executor-memory-in-gb [perExecutorMemoryInGB]
--driver-memory-in-gb [driverMemoryInGB] --cluster-fqdn [clusterFQDN] --cluster-username [clusterUsername]
--cluster-password [clusterPassword] --batch-mode|--interactive-mode

**For monitoring or terminating Spark job:**

Usage: java -cp SparkJobClient.jar com.microsoft.spark.job.client.LivyClient
--monitor|--kill --job-id [jobId] --cluster-fqdn [clusterFQDN] --cluster-username [clusterUsername]
--cluster-password [clusterPassword] --batch-mode|--interactive-mode

**For listing Spark jobs:**

Usage: java -cp SparkJobClient.jar com.microsoft.spark.job.client.LivyClient
--list --cluster-fqdn [clusterFQDN] --cluster-username [clusterUsername]
--cluster-password [clusterPassword] --batch-mode|--interactive-mode