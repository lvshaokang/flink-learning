package com.ruozedata.bigdata.base

import org.apache.flink.api.common.functions.RichFilterFunction

/**
 * author：若泽数据-PK哥   
 * 交流群：545916944
 */
class RuozedataFilter(traffics: Long) extends RichFilterFunction[Access] {
  override def filter(value: Access): Boolean = value.traffics > traffics
}
