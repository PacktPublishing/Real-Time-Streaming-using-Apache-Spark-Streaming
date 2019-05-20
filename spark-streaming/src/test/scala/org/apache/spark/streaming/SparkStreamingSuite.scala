package org.apache.spark.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.Span
import org.scalatest.{BeforeAndAfter, FunSuite, Suite}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps


trait SparkStreamingSuite extends FunSuite with BeforeAndAfter with Eventually {
  this: Suite =>
  implicit var spark: SparkContext = _

  private val master = "local[*]"
  def appName: String
  private val batchDuration = Seconds(1)

  var clock: ClockWrapper = _

  var ssc: StreamingContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    ssc = new StreamingContext(conf, batchDuration)

    spark = ssc.sparkContext
    clock = new ClockWrapper(ssc)
  }

  after {
    if (ssc != null) {
      ssc.stop()
    }
    System.clearProperty("spark.driver.port")
  }


  def assertInputMatchExpected[T](result : mutable.ListBuffer[Array[T]],
                                  expected: Array[T]): Unit = {
    assertInputMatchExpected(result, expected, 5 seconds)
  }

  def assertInputMatchExpected[T](result : mutable.ListBuffer[Array[T]],
                                  expected: Array[T], waitTime: Span): Unit = {
    clock.advance(Seconds(1).milliseconds)
    eventually(timeout(waitTime)) {
      result.last should equal(expected)
    }
  }
}
