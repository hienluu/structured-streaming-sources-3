package org.structured_streaming_sources.twitter

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap


import scala.collection.JavaConverters._


/**
 * Entry point for the Twitter streaming data source
 */
class TwitterSourceProvider extends TableProvider with Logging {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    TwitterSourceV2.SCHEMA
  }

  override def getTable(schema: StructType, partitioning: Array[Transform],
                        properties: util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    log.warn(s"initializing getTable $properties")

    val consumerKey = options.getOrDefault(TwitterSourceV2.CONSUMER_KEY,"")
    val consumerSecret = options.getOrDefault(TwitterSourceV2.CONSUMER_SECRET,"")
    val accessToken = options.getOrDefault(TwitterSourceV2.ACCESS_TOKEN,"")
    val accessTokenSecret = options.getOrDefault(TwitterSourceV2.ACCESS_TOKEN_SECRET,"")
    val numPartitions = options.getInt(TwitterSourceV2.NUM_PARTITIONS,5)
    val queueSize = options.getInt(TwitterSourceV2.QUEUE_SIZE,512)
    val debugLevel = options.getOrDefault(TwitterSourceV2.DEBUG_LEVEL, "debug")
    val table = new TwitterSourceTable(consumerKey, consumerSecret, accessToken, accessTokenSecret,
                                        numPartitions, queueSize, debugLevel)
    table
  }
}

class TwitterSourceTable (consumerKey:String, consumerSecret: String,
                          accessToken:String, accessTokenSecret: String,
                          numPartitions: Int, queueSize: Int,
                          debugLevel : String) extends Table with SupportsRead {
  override def name(): String = s"twitter: numPartitions: $numPartitions, debugLevel: $debugLevel"

  override def schema(): StructType = TwitterSourceV2.SCHEMA

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.MICRO_BATCH_READ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new Scan {
    override def readSchema(): StructType = schema()

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
      new TwitterSourceMicroBatchStream(consumerKey, consumerSecret,
        accessToken, accessTokenSecret, numPartitions, queueSize, debugLevel)
    }

    override def toContinuousStream(checkpointLocation: String): ContinuousStream = {
      throw new UnsupportedOperationException(s"$name() doesn't support continous stream processing mode yet")
      null
    }
  }
}


object TwitterSourceV2 {

  val CONSUMER_KEY = "consumerKey"
  val CONSUMER_SECRET = "consumerSecret"
  val ACCESS_TOKEN = "accessToken"
  val ACCESS_TOKEN_SECRET = "accessTokenSecret"
  val DEBUG_LEVEL = "debugLevel"
  val NUM_PARTITIONS = "numPartitions"
  val QUEUE_SIZE = "queueSize"


  val SCHEMA =
    StructType(
      StructField("text", StringType) ::
        StructField("user", StringType) ::
        StructField("lang", StringType) ::
        StructField("createdDate", TimestampType) ::
        StructField("isRetweeted", BooleanType) ::
        Nil)
}
