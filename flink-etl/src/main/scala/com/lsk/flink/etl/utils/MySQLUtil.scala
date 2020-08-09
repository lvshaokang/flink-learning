package com.lsk.flink.etl.utils

import java.sql.{Connection, DriverManager}

/**
 * TODO: 
 *
 * @author red
 * @class_name MySQLUtil
 * @date 2020-07-19
 */
object MySQLUtil {

  def getConnection() =  {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://ruozedata001:3306/ruozedata_flink","root","ruozedata")
  }

  def close(connection: Connection): Unit = {
    if (connection != null) {
      connection.close()
    }
  }



}
