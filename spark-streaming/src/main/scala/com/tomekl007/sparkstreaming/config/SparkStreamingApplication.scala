package com.tomekl007.sparkstreaming.config

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.concurrent.duration.FiniteDuration

trait SparkStreamingApplication extends SparkApplication {

  def sparkStreamingConfig: SparkStreamingApplicationConfig

  def withSparkStreamingContext(conf: (SparkConf => SparkConf), f: (StreamingContext) => Unit): Unit = {
    withSparkContext(conf) { sc =>
      val ssc = new StreamingContext(sc, Seconds(sparkStreamingConfig.batchDuration.toSeconds))
      ssc.checkpoint(sparkStreamingConfig.checkpoint)
      f(ssc)

      ssc.start()
      ssc.awaitTermination()
    }
  }

  def withSparkStreamingContext(f: (StreamingContext) => Unit): Unit = withSparkStreamingContext(identity[SparkConf], f)

}

case class SparkStreamingApplicationConfig(batchDuration: FiniteDuration, checkpoint: String) extends Serializable