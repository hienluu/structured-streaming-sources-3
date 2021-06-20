package org.structured_streaming_sources.twitter

import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.unsafe.types.UTF8String
import org.structured_streaming_sources.wikiedit.WikiEditInputPartition
import twitter4j._
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

import scala.collection.mutable.ListBuffer

class TwitterSourceMicroBatchStream(consumerKey:String, consumerSecret: String,
                                    accessToken:String, accessTokenSecret: String,
                                    numPartitions: Int, queueSize: Int,
                                    debugLevel: String) extends MicroBatchStream with Logging {
  private val NO_DATA_OFFSET = LongOffset(-1)

  private var startOffset: LongOffset = new LongOffset(-1)
  private var endOffset: LongOffset = new LongOffset(-1)

  private var currentOffset: LongOffset = new LongOffset(-1)
  private var lastReturnedOffset: LongOffset = new LongOffset(-2)
  private var lastOffsetCommitted : LongOffset = new LongOffset(-1)

  private var incomingEventCounter = 0;
  private var stopped:Boolean = false

  private var twitterStream:TwitterStream = null
  private var worker:Thread = null

  private val tweetList:ListBuffer[Status] = new ListBuffer[Status]()
  private var tweetQueue:BlockingQueue[Status] = null

  private val initialized: AtomicBoolean = new AtomicBoolean(false)

  private def initialize(): Unit = synchronized {

    if (consumerKey == "" || consumerSecret == "" || accessToken == "" || accessTokenSecret == "") {
      throw new IllegalStateException("One or more pieces of required OAuth info. is missing." +
        s" Make sure the following are provided ${TwitterSourceV2.CONSUMER_KEY}," +
        s"${TwitterSourceV2.CONSUMER_SECRET} ${TwitterSourceV2.ACCESS_TOKEN} " +
        s"${TwitterSourceV2.ACCESS_TOKEN_SECRET}")
    }

    tweetQueue = new ArrayBlockingQueue(queueSize)

    val configBuilder:ConfigurationBuilder  = new ConfigurationBuilder()
    configBuilder.setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerSecret)
    configBuilder.setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)

    val twitterAuth = new OAuthAuthorization(configBuilder.build())
    twitterStream = new TwitterStreamFactory().getInstance(twitterAuth)

    twitterStream.addListener(new StatusListener {
      def onStatus(status: Status): Unit = {
        tweetQueue.add(status)
      }
      // Unimplemented
      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
      def onTrackLimitationNotice(i: Int) {}
      def onScrubGeo(l: Long, l1: Long) {}
      def onStallWarning(stallWarning: StallWarning) {}
      def onException(e: Exception) {
        /*if (!stopped) {
          restart("Error receiving tweets", e)
        }*/
      }
    })

    worker = new Thread("Tweet Worker") {
      setDaemon(true)
      override def run() {
        receive()
      }
    }
    worker.start()

    // start receiving tweets
    twitterStream.sample()
  }

  private def receive(): Unit = {

    while(!stopped) {
      // poll tweets from queue
      val tweet:Status = tweetQueue.poll(100, TimeUnit.MILLISECONDS)

      if (tweet != null) {

        tweetList.append(tweet);
        currentOffset = currentOffset + 1

        incomingEventCounter = incomingEventCounter + 1;
      }
    }
  }

  private def internalLog(msg:String): Unit = {
    debugLevel match {
      case "warn" => log.warn(msg)
      case "info" => log.info(msg)
      case "debug" => log.debug(msg)
      case _ =>
    }
  }

  override def latestOffset(): Offset = currentOffset

  override def planInputPartitions(start: Offset, `end`: Offset): Array[InputPartition] = {
    val startOrdinal = start.asInstanceOf[LongOffset].offset.toInt + 1
    val endOrdinal = end.asInstanceOf[LongOffset].offset.toInt + 1

    internalLog(s"createDataReaderFactories: sOrd: $startOrdinal, eOrd: $endOrdinal, " +
      s"lastOffsetCommitted: $lastOffsetCommitted")

    val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
    val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1

    val newBlocks = synchronized {
      if (initialized.compareAndSet(false, true)) {
        initialize()
      }

      assert(sliceStart <= sliceEnd, s"sliceStart: $sliceStart sliceEnd: $sliceEnd")
      tweetList.slice(sliceStart, sliceEnd)
    }

    val slices = Array.fill(numPartitions)(new ListBuffer[Status])
    newBlocks.zipWithIndex.foreach { case (r, idx) =>
      slices(idx % numPartitions).append(r)
    }

    slices.map(TwitterInputPartition)

  }

  override def createReaderFactory(): PartitionReaderFactory = {
    (partition: InputPartition) => {
      val slice = partition.asInstanceOf[TwitterInputPartition].slice
      new PartitionReader[InternalRow] {
        private var currentIdx = -1

        override def next(): Boolean = {
          currentIdx += 1
          currentIdx < slice.size
        }

        override def get(): InternalRow = {
          val tweet = slice(currentIdx)
          InternalRow(UTF8String.fromString(tweet.getText), UTF8String.fromString(tweet.getUser.getScreenName),
            UTF8String.fromString(tweet.getUser.getLang), tweet.getCreatedAt.getTime, tweet.isRetweet)
        }

        override def close(): Unit = {}
      }
    }
  }

  override def initialOffset(): Offset = startOffset

  override def deserializeOffset(json: String): Offset = {
    LongOffset(json.toLong)
  }

  override def commit(`end`: Offset): Unit = {
    internalLog(s"** commit($end) lastOffsetCommitted: $lastOffsetCommitted")

    val newOffset = end.asInstanceOf[LongOffset]

    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    tweetList.trimStart(offsetDiff)
    lastOffsetCommitted = newOffset
  }

  override def stop(): Unit = {
    log.warn(s"There is a total of $incomingEventCounter events that came in")
    stopped = true
    if (twitterStream != null) {
      try {
        twitterStream.shutdown()
      } catch {
        case e: IOException =>
      }
    }
    twitterStream = null
  }
}

case class TwitterInputPartition(slice: ListBuffer[Status]) extends InputPartition