package com.lsk.flink.state.chk.savepoint

/**
 * SavePoint定位:
 *  由用户主动去控制,如停机运维的操作,更新代码,参数调整等
 *
 * Trigger
 * bin/flink savepoint :jobId [:targetDirectory]
 *
 * Trigger with yarn
 * bin/flink savepoint :jobId [:targetDirectory] -yid:yarnAppId
 */
object SavePointApp01 {
  
}
