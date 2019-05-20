package com.tomekl007.sparkstreaming

import java.time.ZonedDateTime

case class PageView(pageViewId: Int, userId: String, url: String, eventTime: ZonedDateTime)

case class PageViewWithViewCounter(pageViewId: Int, userId: String, url: String,
                                   eventTime: ZonedDateTime, viewsCounter: Int)

object PageViewWithViewCounter {
  def withVisitCount(event: PageView, visitCount: Int): PageViewWithViewCounter = {
    PageViewWithViewCounter(event.pageViewId, event.userId, event.url, event.eventTime, visitCount)
  }
}