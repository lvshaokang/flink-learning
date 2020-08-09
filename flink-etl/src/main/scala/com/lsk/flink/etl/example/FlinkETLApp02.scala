package com.lsk.flink.etl.example

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

/**
 * 去接口请求数据
 *
 * @author red
 * @class_name FlinkETLApp01
 * @date 2020-07-19
 */
object FlinkETLApp02 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("localhost", 9999)

    // TODO... 去接口请求数据

    stream.map(new RichMapFunction[String, Access02] {

      var client: CloseableHttpClient = _

      override def open(parameters: Configuration): Unit = {
        client = HttpClients.createDefault()
      }

      override def close(): Unit = {
        client.close()
      }

      override def map(value: String): Access02 = {
        val splits = value.split(",")
        val time = splits(0)
        val domain = splits(1)
        val traffic = splits(2).toLong
        val longitude = splits(3).toDouble
        val latitude = splits(4).toDouble

        val key = ""

        val url = s"https://restapi.amap.com/v3/geocode/regeo?output=json&key=${key}&location=$longitude,$latitude"
        var province = ""
        var city = ""
        var district = ""

        var response: CloseableHttpResponse = null

        try {
          val httpGet = new HttpGet(url)
          response = client.execute(httpGet)
          val status = response.getStatusLine.getStatusCode
          val entity = response.getEntity

          // ok
          if (200 == status) {
            val result = EntityUtils.toString(entity, "UTF-8")
            val json = JSON.parseObject(result)

            val regeocodes = json.getJSONObject("regeocode")

            if (null != regeocodes && !regeocodes.isEmpty) {
              val addressComponent = regeocodes.getJSONObject("addressComponent")
              province = addressComponent.getString("province")
              city = addressComponent.getString("city")
              district = addressComponent.getString("district")
            }
          }
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          response.close()
        }

        Access02(time, domain, traffic, latitude, longitude, province, city, district)
      }
    }).print()


    env.execute(this.getClass.getSimpleName)
  }

  case class Access02(time: String,
                      domain: String,
                      traffic: Long,
                      latitude: Double,
                      longitude: Double,
                      province: String = "",
                      city: String = "",
                      district: String = "")

}
