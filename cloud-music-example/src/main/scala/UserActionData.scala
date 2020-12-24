import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema

import scala.collection.JavaConverters._

/**
 * UserActionDeserializer
 *
 * @author red
 * @class_name UserActionData
 * @date 2020-11-29
 */
class UserActionDeserializer extends AbstractDeserializationSchema[UserActionData] {
  override def deserialize(message: Array[Byte]): UserActionData = {
    val jSONObject = JSON.parseObject(new String(message, "utf-8"))
    val userId = jSONObject.getLong("user_id")
    val action = jSONObject.getString("action")
    val logTime = jSONObject.getInteger("log_time")
    val props = jSONObject.getJSONObject("props")
    UserActionData(userId, action, logTime, props)
  }
}

class BinLogDeserializer extends AbstractDeserializationSchema[BinLogData] {
  override def deserialize(message: Array[Byte]): BinLogData = {
    val jSONObject = JSON.parseObject(new String(message, "utf-8"))
    val tableName = jSONObject.getString("tableName")
    val changeType = jSONObject.getString("changeType")
    val columnChanges = jSONObject.getJSONArray("columnChanges")
    val changes = columnChanges.toJavaList(classOf[ColumnChange])
    BinLogData(tableName, changeType, changes.asScala.toList)
  }
}

case class BinLogData(tableName: String, changeType: String, changeList: List[ColumnChange])

case class ColumnChange(columnName: String, columnType: String, oldValue: String, newValue: String)

case class UserActionData(userId: Long, action: String, logTime: Int, props: JSONObject)
