package org.structured_streaming_sources.examples

import java.util.concurrent.TimeUnit

import org.structured_streaming_sources.wikiedit.WikiEditSourceProvider
import org.apache.spark.sql.SparkSession

/**
 * A simple example of using the WikiEditSourceProvider
 */
object WikiEditStreamingSourceExample {

 // private val SOURCE_PROVIDER_CLASS = org.structured_streaming_sources.wikiedit.WikiEditSourceProvider.getClass.getCanonicalName
  def main(args: Array[String]) : Unit = {
    println(s"Do you see this? $this.getClass.getCanonicalName")

    val spark = SparkSession
      .builder
      .appName("WikiEditSourceV2Example")
      .master("local[*]")
      .getOrCreate()

    println("Spark version: " + spark.version)

    val wikiEdit = spark.readStream.format("org.structured_streaming_sources.wikiedit.WikiEditSourceProvider")
                                   .option("channel", "#en.wikipedia").load()
    wikiEdit.printSchema()

    println(s"Starting the stream ... ")

    val wikiEditQuery = wikiEdit.writeStream.outputMode("append")
      .queryName("wikiedit").format("memory").start()

    var counter:Long = 0
    while (counter < 1) {
      println("**** Sleeping a little to wait for events to come in")
      Thread.sleep(TimeUnit.SECONDS.toMillis(5))
      counter = spark.sql("select * from wikiedit").count()
      println(s"There are total of $counter in memory table");
    }

    println("**** There is data now.  Showing them")
    spark.sql("select * from wikiedit").show(false)

    Thread.sleep(TimeUnit.SECONDS.toMillis(3))

    spark.sql("select * from wikiedit").show(false)

    val wikiEditCount = spark.sql("select * from wikiedit").count;
    println(s"There are total of $wikiEditCount in memory table");

    wikiEditQuery.stop()

    for(qs <- spark.streams.active) {
      println(s"Stop streaming query: ${qs.name} - active: ${qs.isActive}")
      if (qs.isActive) {
        qs.stop
      }
    }
    spark.stop()

  }
}
