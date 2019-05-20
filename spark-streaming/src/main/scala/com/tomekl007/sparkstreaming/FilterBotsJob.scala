package com.tomekl007.sparkstreaming

import java.time.ZonedDateTime

import com.tomekl007.sparkstreaming.config._
import com.tomekl007.sparkstreaming.deduplication.DeduplicationService
import com.tomekl007.sparkstreaming.ordering.StreamOrderVerification
import com.tomekl007.sparkstreaming.pageviewcounter.PageViewCounterService
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.concurrent.duration._


class FilterBotsJob()
  extends SparkStreamingApplication {

  override def sparkAppName: String = "filter_bots_job"

  override def sparkStreamingConfig: SparkStreamingApplicationConfig =
    SparkStreamingApplicationConfig(Duration(2, SECONDS), "file://temporary-directory}")
  //when running on the cluster the Checkpointing dir should be on hdfs

  def start(): Unit = {
    withSparkStreamingContext { ssc =>
      val stream: DStream[PageView] = DStreamProvider.providePageViews(ssc)
      val sink: DStreamSink[PageViewWithViewCounter] = new DStreamSink()

      processStream(ssc, stream, sink)
    }
  }

  def processStream(ssc: StreamingContext, stream: DStream[PageView],
                    sink: DStreamSink[PageViewWithViewCounter]): Unit = {
    val streamWithTaggedRecords = processPageViews(stream)
    sink.write(ssc, streamWithTaggedRecords)
  }

  def processPageViews(stream: DStream[PageView]): DStream[PageViewWithViewCounter] = {
    stream
      .transform(FilterBotsJob.sort(_))
      .filter(FilterBotsJob.isNotDuplicated(_))
      .filter(record => {
        !record.url.contains("bot")
      })
      .map(pageView => FilterBotsJob.countOccurrences(pageView))
  }
}

object FilterBotsJob {

  val streamOrderVerification = new StreamOrderVerification()
  val deduplicationService = new DeduplicationService()
  val pageViewCounterService = new PageViewCounterService()

  def main(args: Array[String]): Unit = {
    val job = new FilterBotsJob()

    job.start()
  }

  def sort(rdd: RDD[PageView]): RDD[PageView] = {
    implicit val localDateOrdering: Ordering[ZonedDateTime] = Ordering.by(_.toInstant.toEpochMilli)
    rdd.sortBy(v => {
      v.eventTime
    }, ascending = true)
  }

  def isNotDuplicated(event: PageView): Boolean = !deduplicationService.isDuplicate(event)

  //if we want to have a strict order, we want to drop all our of order events
  def dropOutOfOrderEvents(rdd: RDD[PageView]): RDD[PageView] =
    rdd.filter(streamOrderVerification.isInOrder)

  def countOccurrences(event: PageView): PageViewWithViewCounter = {
    val count = pageViewCounterService.count(event)
    PageViewWithViewCounter.withVisitCount(event, count)
  }

}

