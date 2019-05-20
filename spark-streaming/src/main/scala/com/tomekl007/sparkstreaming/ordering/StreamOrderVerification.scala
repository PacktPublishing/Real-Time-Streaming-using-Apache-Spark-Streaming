package com.tomekl007.sparkstreaming.ordering

import java.time.ZonedDateTime

import com.tomekl007.sparkstreaming.PageView

class StreamOrderVerification() {

  //3 1 2 -> user1 1 user1 2 x 3 | (user1 -> 2 | x -> 3)
  //4 5 1 -> user1 1 x 4 y 5     |               x -> 4 | y -> 5
  private var lastEventsActionDatePerUserId: Map[String, ZonedDateTime] = Map()
  //on production it should be a cache with an expiring time to prevent OutOfMemory

  def isInOrder(newEvent: PageView): Boolean = {
    val userId = newEvent.userId
    val isInOrder = lastEventsActionDatePerUserId.get(userId)
      .forall(lastActionDate => inOrder(newEvent, lastActionDate))
    println(s"inOrder: $newEvent, result: $isInOrder, ${ZonedDateTime.now()}")
    if (isInOrder) {
      lastEventsActionDatePerUserId += (userId -> newEvent.eventTime)
    }

    isInOrder
  }

  private def inOrder(newEvent: PageView, lastEventActionDate: ZonedDateTime) =
    newEvent.eventTime.isAfter(lastEventActionDate)
}