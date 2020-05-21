package org.myorg.quickstart

import org.apache.flink.api.java.utils.ParameterTool

/**
 * author：若泽数据-PK哥   
 * 交流群：545916944
 */
object ParametersApp {

  def main(args: Array[String]): Unit = {
//    testFromArgs(args)
    testFromPropertiesFile()
  }

  def testFromPropertiesFile(): Unit ={
    val parameters = ParameterTool.fromPropertiesFile("conf/ruozedata-flink.properties")
    val groupId = parameters.get("group.id", "ruozedata-test-group")
    val topics = parameters.getRequired("topics")
    println(groupId + "...." + topics)
  }

  def testFromArgs(args: Array[String]): Unit = {
    val parameters = ParameterTool.fromArgs(args)
    val groupId = parameters.get("group.id", "ruozedata-test-group")
    val topics = parameters.getRequired("topics")
    println(groupId + "...." + topics)
  }

}
