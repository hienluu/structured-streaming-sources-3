package org.structured_streaming_sources.wikiedit

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue}

import org.schwering.irc.lib.IRCConnection
import org.slf4j.{Logger, LoggerFactory}

class WikiEditStream(host:String, port:Int, queueSize:Int = 128) {
  val logger:Logger = LoggerFactory.getLogger(this.getClass)

  val editQueue: BlockingQueue[WikiEditEvent]  = new ArrayBlockingQueue(queueSize);

  val nick:String  = "spark-bot-" + (Math.random() * 1000).toInt
  val conn:IRCConnection = new IRCConnection(host, Array(port) , "", nick, nick, nick);

  conn.addIRCEventListener(new WikiEditChannelListener(editQueue));
  conn.setEncoding("UTF-8");
  conn.setPong(true);
  conn.setColors(false);
  conn.setDaemon(true);
  conn.setName("WikipediaEditEventIrcStreamThread");

  def start()  {
    logger.info("starting the stream..")
    if (!conn.isConnected()) {
      conn.connect();
    }
  }

  def stop() {
    logger.info("stopping the stream..")
    if (conn.isConnected()) {
    }

    conn.interrupt();
    conn.join(5 * 1000);
  }

  def join(channel:String) {
    logger.info(s"joining channel $channel..")
    conn.send("JOIN " + channel);
  }

  def leave(channel:String) {
    logger.info(s"leaving channel $channel..")
    if (conn != null) {
      conn.send("PART " + channel);
    } else {
      logger.info("conn has not been initialized yet")
    }
  }

  def getEdits() : BlockingQueue[WikiEditEvent] = {
    return editQueue;
  }
}

object WikiEditStream {
  def apply(host:String, port:Int, queueSize:Int = 128) = {
    new WikiEditStream(host, port, queueSize)
  }
}

