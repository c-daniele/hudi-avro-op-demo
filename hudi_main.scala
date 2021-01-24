
//import awscala.Region
//import awscala.dynamodbv2.DynamoDB
//import awscala.dynamodbv2.Table
// awscala.dynamodbv2.AttributeValue

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.spark.SparkConf
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql._;
import org.apache.hudi.common.model.HoodieTableType
import org.apache.spark.sql.functions.to_timestamp;
import org.apache.spark.sql.functions.unix_timestamp;
import org.apache.spark.sql.functions.regexp_replace;
import org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import scala.util.Failure
import scala.util.Success

// metadata sent by GoldenGate which contains the operational timestamp
val lastUpdateTsFieldName     	= "op_ts";

// metadata sent by GoldenGate which contains the operation type ("I": INSERT, "U": UPDATE, "D": DELETE)
val opTypeFieldName  	          = "op_type";

// metadata sent by GoldenGate which contains a sequence number
val lastPositionColumnName  	  = "pos";

// metadata sent by GoldenGate which contains the PK fields
val pkFieldName									= "primary_keys";

// glue/hive database name
val hiveDbName             		 	= "sales_db";

// source field to be used for partitioning
val partitionFieldName			 		= "DAT_ORDER";
val partitionFieldPattern				= "yyyy-MM-dd";

// Hudi field to be added for partition purposes
val hudiPartitionField					= "HUDI_PART_DATE";

val targetBucket 							 	= "my-s3-targetBucket";
val sourceBucket 							 	= "my-s3-sourcebucket";

val targetSubFolder 						= "HUDI_DATA";
val sourceSubFolder 						= "GG_DATA";

val inputTableName 							= "ORDERS";

val sourceSystem 								= "SALES_SYSTEM";

var admittedValues:ListBuffer[String] = ListBuffer[String]();

admittedValues += "I";
admittedValues += "U";

val ggDeltaFiles = "s3://" + sourceBucket + "/" + sourceSubFolder + "/" + sourceSystem + "/" + inputTableName + "/";
val hudiTablePath = "s3://" + targetBucket + "/" + targetSubFolder + "/" + sourceSystem + "/" + inputTableName + "/";
val hudiTableName = sourceSystem + "_" + inputTableName;

// spark backward compatibility
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY");

val hudiOptions = Map[String, String](

  DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> hudiPartitionField,
  DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY -> hudiPartitionField,
  DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> classOf[SlashEncodedDayPartitionValueExtractor].getName,

  DataSourceWriteOptions.HIVE_URL_OPT_KEY -> "jdbc:hive2://localhost:10000",

	DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY -> classOf[org.apache.hudi.keygen.ComplexKeyGenerator].getName,

	HoodieWriteConfig.TABLE_NAME -> hudiTableName,
	DataSourceWriteOptions.OPERATION_OPT_KEY -> DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,

	DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY -> HoodieTableType.COPY_ON_WRITE.name(),
	//DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY -> HoodieTableType.MERGE_ON_READ.name(),

	DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> lastUpdateTsFieldName,
	DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY -> "true",
	DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY -> hiveDbName,
	DataSourceWriteOptions.HIVE_TABLE_OPT_KEY -> hudiTableName,

	// keep last version only
	"hoodie.cleaner.policy" -> "KEEP_LATEST_FILE_VERSIONS",
	"hoodie.cleaner.fileversions.retained" -> "1"
)

// read source data
val rootDataframe:DataFrame = spark.read.format("avro").load(ggDeltaFiles);

// extract PK fields name from first line
val pkFields: Seq[String] = rootDataframe.select(pkFieldName).limit(1).collect()(0).getSeq(0);

// take into account the "after." fields only
val columnsPre:Array[String] = rootDataframe.select("after.*").columns;

// exclude "_isMissing" fields added by Oracle GoldenGate
// The second part of the expression will safely preserve all native "**_isMissing" fields
val columnsPost:Array[String] = columnsPre.filter { x => (!x.endsWith("_isMissing")) || (!x.endsWith("_isMissing_isMissing") && (columnsPre.filter(y => (y.equals(x + "_isMissing")) ).nonEmpty))};
val columnsFinal:ArrayBuffer[String] = new ArrayBuffer[String]();

columnsFinal += lastUpdateTsFieldName;
columnsFinal += lastPositionColumnName;

// add the "after." prefix
columnsPost.foreach(x => (columnsFinal += "after." + x));

// prepare the target dataframe with the partition additional column
val preparedDataframe = rootDataframe.select(opTypeFieldName, columnsFinal.toArray:_*).
  withColumn(hudiPartitionField, date_format(to_date(col(partitionFieldName), partitionFieldPattern),"yyyy/MM/dd")).
  filter(col(opTypeFieldName).isin(admittedValues.toList: _*));

var numRecords:Long = 0L;

numRecords = Try(preparedDataframe.count()) match {
  case Success(integer) => integer
  case Failure(ex) => {

  println(ex);

  0;

 }
}

println("loading " + numRecords + " RECORDS");


// write the data
preparedDataframe.write.format("org.apache.hudi").
  options(hudiOptions).
  option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, pkFields.mkString(",")).
  mode(SaveMode.Append).
  save(hudiTablePath);
