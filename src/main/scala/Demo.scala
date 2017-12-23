package lazyevaluators.streams

import java.util.Properties

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._

object Demo {

  def main(args: Array[String]): Unit = {
    val config = {
      val properties = new Properties()
      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-demo")
      properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      properties
    }


    val builder = new StreamsBuilder()
    val sourceStream = builder.stream[String, String]("demo-source")

    import collection.JavaConverters._

    sourceStream
      .flatMapValues[String](_.split(" ").toList.asJava)
      .map[String, String]((_, v) => new KeyValue[String, String](v, v))
//        .groupByKey().count().toStream
      .to("demo-sink")

    val streams = new KafkaStreams(builder.build(), config)
    streams.start()
  }
}