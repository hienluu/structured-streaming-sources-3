package org.structured_streaming_sources.examples

import org.apache.spark.sql.SparkSession
import org.structured_streaming_sources.twitter.{TwitterSourceProvider, TwitterSourceV2}

/**
 * A simple example of using the TwitterSourceProvider
 */
object TwitterStreamingSourceExample {
  //private val SOURCE_PROVIDER_CLASS = TwitterSourceProvider.getClass.getCanonicalName
  private val TWITTER_SOURCE_NAME = "org.structured_streaming_sources.twitter.TwitterSourceProvider"
  def main(args: Array[String]): Unit = {
    println("TwitterStreamingSourceExample")

    if (args.length != 4) {
      println("Usage: <consumer key>, <consumer secret> <access token> <access token secret>")
      sys.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)

    val spark = SparkSession
      .builder
      .appName("TwitterSourceExample")
      .master("local[*]")
      .getOrCreate()

    val tweetDF = spark.readStream
      .format(TWITTER_SOURCE_NAME)
      .option(TwitterSourceV2.CONSUMER_KEY, consumerKey)
      .option(TwitterSourceV2.CONSUMER_SECRET, consumerSecret)
      .option(TwitterSourceV2.ACCESS_TOKEN, accessToken)
      .option(TwitterSourceV2.ACCESS_TOKEN_SECRET, accessTokenSecret)
      .load()

    tweetDF.printSchema()

    //val tweetQS = tweetDF.filter(tweetDF("lang") === "en")
    val tweetQS = tweetDF.writeStream.format("console").option("truncate", "false").start()

    Thread.sleep(1000 * 35)

    tweetQS.stop();
  }
}
