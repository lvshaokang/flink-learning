package com.ruozedata.bigdata.base

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/**
 * author：若泽数据-PK哥   
 * 交流群：545916944
 */
class RuozedataMySQLScalikeJDBCSource extends RichSourceFunction[Student] {

  override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
    DBs.setupAll()

    DB.readOnly { implicit session => {
      SQL("select * from ruozedata_flink.student").map(rs => {
        val id = rs.int("id")
        val name = rs.string("name")
        val age = rs.int("age")
        ctx.collect(Student(id, name, age))
      }).list().apply()
    }
    }

  }

  override def cancel(): Unit = ???
}
