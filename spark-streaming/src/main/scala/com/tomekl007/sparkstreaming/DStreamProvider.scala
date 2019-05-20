package com.tomekl007.sparkstreaming

import kafka.serializer.DefaultDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.codehaus.jackson.map.ObjectMapper

object DStreamProvider {
  val objectMapper: ObjectMapper = new ObjectMapper()

  def providePageViews(ssc: StreamingContext): DStream[PageView] = {
    val properties = Map(
      "bootstrap.servers" -> "broker1:9092,broker2:9092", //set your prod env configuration
      "group.id" -> "bots-filtering",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    KafkaUtils.createStream[String, String, DefaultDecoder, DefaultDecoder](
      ssc,
      properties,
      Map("page_views" -> 1),
      StorageLevel.MEMORY_ONLY
    ).map(deserializeToPageView)


  }

  def deserializeToPageView(tuple: (String, String)): PageView = {
    objectMapper.readValue(tuple._2, classOf[PageView]) //in prod it should be binary format, for example avro
  }

}
