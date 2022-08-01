package com.victor.sc

/**
 * scala工具类
 */
object Utils {

  def hashCode(elements: Any*): Int = {
    if (elements == null) return 0
    var result = 1
    for (elem <- elements) {
      val hash = if (elem == null) 0 else elem.hashCode
      result = 31 * result + hash
    }
    result
  }

}
