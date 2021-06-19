package org.structured_streaming_sources.wikiedit

import java.io.IOException

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{BlockingQueue, TimeUnit}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.unsafe.types.UTF8String


import scala.collection.mutable.ListBuffer

/**
 * Reading wiki edits from wiki edit server, buffering the incoming wiki edits
 * in an internal list data structure.  Each row is an instance of {@code WikiEditEvent}.
 *
 * The interaction contract with Structured Streaming engine is through the {@code MicroBatchStream}
 * interface
 */
class WikiEditMicroBatchStream(host:String, port:Int, queueSize: Int, channel:String,
                               numPartitions:Int, debugLevel:String) extends MicroBatchStream with Logging {

  private var worker:Thread = null
  private var ircStream:WikiEditStream = null
  private var shouldStop:Boolean = false
  private val wikiEditList:ListBuffer[WikiEditEvent] = new ListBuffer[WikiEditEvent]()

  private var startOffset: LongOffset = new LongOffset(-1)
  private var endOffset: LongOffset = new LongOffset(-1)

  private var currentOffset: LongOffset = new LongOffset(-1)
  //private var lastReturnedOffset: LongOffset = new LongOffset(-2)
  private var lastOffsetCommitted : LongOffset = new LongOffset(-1)

  private val initialized: AtomicBoolean = new AtomicBoolean(false)
  private var incomingEventCounter = 0;

  private def initialize(): Unit = synchronized {
    logInputs()
    ircStream = WikiEditStream(host, port, queueSize)

    worker = new Thread("WikiEdit Worker") {
      setDaemon(true)
      override def run() {
        receive()
      }
    }
    worker.start()
    log.warn(s"done initializing WikiEditStream")
  }

  private def receive(): Unit = {
    ircStream.start();
    ircStream.join(channel);

    log.warn(s"joining channel $channel")

    while(!shouldStop) {
      // Query for the next edit event
      val edit: BlockingQueue[WikiEditEvent] = ircStream.getEdits();
      if (edit != null) {
        val wikiEdit: WikiEditEvent = edit.poll(100, TimeUnit.MILLISECONDS)

        if (wikiEdit != null) {
          wikiEditList.append(wikiEdit);
          currentOffset = currentOffset + 1
          incomingEventCounter = incomingEventCounter + 1;
        }
      }
    }
  }

  private def logInputs(): Unit = {
    log.warn(s"initializing WikiEditStream with host: $host, port: $port, channel: $channel, queueSize: $queueSize, " +
      s"numPartitions: $numPartitions, debugLevel: $debugLevel")
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

    internalLog(s"planInputPartitions: sOrd: $startOrdinal, eOrd: $endOrdinal, " +
      s"lastOffsetCommitted: $lastOffsetCommitted")

    val rawList = synchronized {
      // initialize the WikiEditStream if this is the very first time
      if (initialized.compareAndSet(false, true)) {
        initialize()
      }

      val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
      assert(sliceStart <= sliceEnd, s"sliceStart: $sliceStart sliceEnd: $sliceEnd")
      wikiEditList.slice(sliceStart, sliceEnd)
    }

    val slices = Array.fill(numPartitions)(new ListBuffer[WikiEditEvent])
    rawList.zipWithIndex.foreach { case (r, idx) =>
      slices(idx % numPartitions).append(r)
    }

    slices.map(WikiEditInputPartition)

  }

  override def createReaderFactory(): PartitionReaderFactory = {
    (partition: InputPartition) => {
      val slice = partition.asInstanceOf[WikiEditInputPartition].slice
      new PartitionReader[InternalRow] {
        private var currentIdx = -1

        override def next(): Boolean = {
          currentIdx += 1
          currentIdx < slice.size
        }

        override def get(): InternalRow = {
          val evt = slice(currentIdx)
          InternalRow(evt.timeStamp, UTF8String.fromString(evt.channel),
            UTF8String.fromString(evt.title), UTF8String.fromString(evt.diffUrl),
            UTF8String.fromString(evt.user), evt.byteDiff, UTF8String.fromString(evt.summary))
        }

        override def close(): Unit = {}
      }
    }
  }

  override def initialOffset(): Offset = startOffset

  override def deserializeOffset(json: String): Offset = {
    LongOffset(json.toLong)
  }

  override def commit(`end`: Offset): Unit = synchronized {
    internalLog(s"** commit($end) lastOffsetCommitted: $lastOffsetCommitted")

    val newOffset = end.asInstanceOf[LongOffset]

    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    wikiEditList.trimStart(offsetDiff)
    lastOffsetCommitted = newOffset
  }

  override def stop(): Unit = synchronized {
    log.warn(s"There is a total of $incomingEventCounter events that came in")
    shouldStop = true
    if (ircStream != null) {
      try {
        ircStream.leave(channel)
        ircStream.stop()
      } catch {
        case e: IOException =>
      }
    }
    ircStream = null
  }
}

/**
 * Class for holding a list of {@code WikiEditEvent}
 *
 * @param slice
 */
case class WikiEditInputPartition(slice: ListBuffer[WikiEditEvent]) extends InputPartition