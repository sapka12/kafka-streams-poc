package com.github.aseigneurin.kafka.streams.scala

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{KeyValueMapper, Produced}
import org.apache.kafka.streams.{Consumed, StreamsBuilder, StreamsConfig}

import scala.collection.JavaConverters._

object WordCountDemo {

  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val textLines = new StreamsBuilder().stream(
      "myStream",
      Consumed.`with`(Serdes.String, Serdes.String)
    )

    val wordCountsKTable = textLines
      .flatMapValues[String](_.toLowerCase.split(" ").toIterable.asJava)
      .groupBy[String]((k, v) => v)
      .count

    wordCountsKTable.toStream.to(
      "streams-wordcount-output",
      Produced.`with`(Serdes.String(), Serdes.Long())
    )
  }
}