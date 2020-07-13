// Databricks notebook source
val jdbcPassword = dbutils.secrets.get(scope = "SqlConnection", key = "cdpadmin")
val appPassword = dbutils.secrets.get(scope = "SqlConnection", key = "cdpdata-ue-app-sbpoc")

// COMMAND ----------


import java.util.Properties

import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("AzureDatabricksDemo")
  .getOrCreate()

val connectionProperties = new Properties()
connectionProperties.put("user", "cdpadmin")
connectionProperties.put("password", jdbcPassword) // Use Secrets

val sqlDb = "CDPData-sqldb-SBPOC"
val synapseDb = "cdpdatauw-sp-sbpoc"

val urlFmt = "jdbc:sqlserver://cdpdata-sql-sbpoc.database.windows.net:1433;database="
val sqlDbUrl = s"$urlFmt$sqlDb"
val synapseDbUrl = s"$urlFmt$synapseDb"

val jdbcDF = spark.read
  .jdbc(sqlDbUrl, "dbo.cars", connectionProperties)


// COMMAND ----------

jdbcDF.show(100)

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import spark.implicits._
import org.apache.spark.sql.functions._

//Rank by length
val wSpec = Window.partitionBy($"make", $"type", $"origin", $"DriveTrain").orderBy(desc("length"))

val rankedDf = jdbcDF.withColumn("rank",  rank over wSpec).withColumn("load_timestamp", current_timestamp())

// COMMAND ----------

rankedDf.show
rankedDf.count

// COMMAND ----------

// DBTITLE 1,Write result back to Sql DB
spark.sql("drop table if exists dbo.cars_ranked")
rankedDf.write.mode("overwrite").jdbc(sqlDbUrl, "dbo.cars_ranked", connectionProperties)


// COMMAND ----------

// DBTITLE 1,Write result to Synapse?
// spark.sql("drop table if exists dbo.cars_ranked_asa")
rankedDf.write.mode("overwrite").jdbc(synapseDbUrl, "dbo.cars_ranked_asa", connectionProperties)


// COMMAND ----------

spark.read
  .jdbc(sqlDbUrl, "dbo.cars", connectionProperties)

// COMMAND ----------

// ToDo:

// Integrate this notebook with ADF.
// Orchestrate via Automic.

// Upload this scala worksheet to GitHub.
// Intergate with Azure Devops such that. Upon commit to demo branch, Azure devops should build and deploy the worksheet to sandbox env, where ADF will trigger the pipelines -> blob - sql server - databricks - synapse

// COMMAND ----------

spark.read.option("header",true).csv("abfss://demo@dlscdpdatasbpoc.dfs.core.windows.net/cars.csv")


// COMMAND ----------

val appID = "574909aa-1a0c-4afd-ad55-4da6be09caab"
val password = appPassword // Use Secrets
val tenantID = "ddb8592f-ed74-4329-bfd2-571b7c34699e"
val fileSystemName = "demo";
var storageAccountName = "dlscdpdatasbpoc";

val configs =  Map("fs.azure.account.auth.type" -> "OAuth",
       "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id" -> appID,
       "fs.azure.account.oauth2.client.secret" -> password,
       "fs.azure.account.oauth2.client.endpoint" -> ("https://login.microsoftonline.com/" + tenantID + "/oauth2/token"),
       "fs.azure.createRemoteFileSystemDuringInitialization"-> "true")

dbutils.fs.mount(
source = "abfss://" + fileSystemName + "@" + storageAccountName + ".dfs.core.windows.net/",
mountPoint = "/mnt/data",
extraConfigs = configs)


// COMMAND ----------

val carsData = spark.read.option("header",true).csv("/mnt/data/cars.csv")

import org.apache.spark.sql.expressions.Window
import spark.implicits._
import org.apache.spark.sql.functions._

//Rank by length
val wSpec1 = Window.partitionBy($"make", $"type", $"origin", $"DriveTrain").orderBy(desc("length"))

val alteredCarsData = carsData.withColumn("rank",  rank over wSpec1).withColumn("load_timestamp", current_timestamp()).withColumn("comments", lit("ADFS-DBFS-ADLS"))

alteredCarsData.coalesce(1).write.option("header",true).mode("overwrite").csv("/mnt/data/adb-output/altered-cars-data.csv")


// COMMAND ----------

spark.read.option("header",true).csv("/mnt/data/adb-output/altered-cars-data.csv").show(100)


// COMMAND ----------

val initDbfsTest = spark.read.option("header",true).csv("dbfs:/mnt/data/adb-output/altered-cars-data.csv")
initDbfsTest.show(100)
initDbfsTest.write.option("header",true).mode("overwrite").csv("/mnt/data/adb-output/init-dbfs-test.csv")


// COMMAND ----------

