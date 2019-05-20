package com.tomekl007.sparkstreaming.deduplication

import com.tomekl007.sparkstreaming.PageView

class DeduplicationService {
  //in production applications it should be some external
  //database that keeps list of already processed ids
  var eventIds: Set[Int] = Set()

  def isDuplicate(event: PageView): Boolean = {
    val duplicate = eventIds.contains(event.pageViewId)
    eventIds += event.pageViewId
    duplicate
  }

}
