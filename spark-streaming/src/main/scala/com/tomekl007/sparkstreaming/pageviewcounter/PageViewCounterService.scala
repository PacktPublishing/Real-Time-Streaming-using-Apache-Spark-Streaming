package com.tomekl007.sparkstreaming.pageviewcounter

import com.tomekl007.sparkstreaming.PageView


class PageViewCounterService {
  //in production want to store that for further analysis
  var countOfPageView: Map[String, Int] = Map()

  def count(event: PageView): Int = {
    val result = countOfPageView.getOrElse(event.url, 0) + 1
    countOfPageView += (event.url -> result)
    result
  }
}
