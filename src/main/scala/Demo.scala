package lazyevaluators.streams

import collection.JavaConverters._
import java.util.Properties

import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.kstream.KStream


object Demo {

  val config = {
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-demo")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    properties
  }

  def tokenize(stream: KStream[String, String]) = stream
    .flatMapValues[String](_.split(" ").toList.asJava)
    .map[String, String]((_, v) => new KeyValue[String, String](v, v))

  def countWords(stream: KStream[String, String]): KTable[String, String] =
    stream
      .flatMap[String, String]((_: String, value: String) =>
      value
        .split(" ")
        .map(word => new KeyValue[String, String](word, word))
        .toList.asJava
    ).groupByKey().count().mapValues(_.toString)

  def processStream(source: String, sink: String, proc: KStream[String, String] => KStream[String, String]): Unit = {

    val builder = new StreamsBuilder()
    val sourceStream: KStream[String, String] = builder.stream[String, String](source)

    proc(sourceStream).to(sink)

    val streams = new KafkaStreams(builder.build(), config)
    streams.start()
  }

  def main(args: Array[String]): Unit = {
//    processStream("demo-source", "demo-sink", tokenize(_))
    processStream("demo-source", "demo-sink-table", stream => countWords(stream).toStream.map[String, String](
      (k, v) => new KeyValue[String, String](k, s"$k: $v")
    ))
  }

}