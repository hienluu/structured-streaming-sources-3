package org.structured_streaming_sources.wikiedit

import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._


class WikiEditSourceProvider extends TableProvider with Logging {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    WikiEditSourceV2.SCHEMA
  }

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    log.warn(s"initializing getTable $properties")
    val table = new WikiEditTable(
      options.getOrDefault(WikiEditSourceV2.HOST, "irc.wikimedia.org"),
      options.getInt(WikiEditSourceV2.PORT, 6667),
      options.getOrDefault(WikiEditSourceV2.CHANNEL, "#en.wikipedia"),
      options.getInt(WikiEditSourceV2.QUEUE_SIZE, 128),
      options.getInt(WikiEditSourceV2.NUM_PARTITIONS, 5),
      options.getOrDefault(WikiEditSourceV2.DEBUG_LEVEL, "debug"))
    log.warn(s"table ${table.toString}")
    table
  }
}

class WikiEditTable (host:String, port: Int, channel:String, queueSize: Int,
                     numPartitions: Int, debugLevel : String) extends Table with SupportsRead {
  override def name(): String = s"WikiEdit[$host:$port:$channel]"

  override def schema(): StructType = WikiEditSourceV2.SCHEMA

  override def capabilities(): util.Set[TableCapability] = {
    Set(TableCapability.MICRO_BATCH_READ).asJava
  }


  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new Scan {
    override def readSchema(): StructType = schema()

    override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
      new WikiEditMicroBatchStream(host, port, queueSize, channel, numPartitions, debugLevel)
    }

    override def toContinuousStream(checkpointLocation: String): ContinuousStream = {
      throw new UnsupportedOperationException(s"$name() doesn't support continous stream processing mode yet")
      null
    }
  }
}

class WikiEditScan extends Scan {
  override def readSchema(): StructType = WikiEditSourceV2.SCHEMA
}

object WikiEditSourceV2 {
  val HOST = "host"
  val PORT = "port"
  val CHANNEL = "channel"
  val QUEUE_SIZE = "queueSize"
  val NUM_PARTITIONS = "numPartitions"
  val DEBUG_LEVEL = "debugLevel"

  val SCHEMA =
    StructType(StructField("timestamp", TimestampType) ::
      StructField("channel", StringType) ::
      StructField("title", StringType) ::
      StructField("diffUrl", StringType) ::
      StructField("user", StringType) ::
      StructField("byteDiff", IntegerType) ::
      StructField("summary", StringType) ::
      Nil)
}