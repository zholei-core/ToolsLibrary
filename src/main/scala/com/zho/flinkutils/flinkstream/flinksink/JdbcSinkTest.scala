package com.zho.flinkutils.flinkstream.flinksink

import com.zho.flinkutils.flinkstream.flinksource.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import java.sql.{Connection, DriverManager, PreparedStatement}

object JdbcSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 0.解析文件数据
    val inputData: DataStream[String] = env.readTextFile("/Users/zhoulei/Documents/workspaces/ToolsLibrary/src/main/resources/sensor.txt")
    val dataStream = inputData.map(dataElem => {
      val arr = dataElem.split(",")
      new SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    dataStream.addSink(new MyJdbcSinkFunc())

    env.execute("JDBC SINK TEST")
  }

}

class MyJdbcSinkFunc() extends RichSinkFunction[SensorReading] {

  var conn: Connection = _
  var insertPstm: PreparedStatement = _
  var updatePstm: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdc:mysql//localhost:3306/test", "root", "pwd")
    insertPstm = conn.prepareStatement("insert into sensor_temp (id,temp) values(?,?)")
    updatePstm = conn.prepareStatement("update sensor_temp set temp=? where id=?")
  }


  override def invoke(value: SensorReading): Unit = {
    // 先执行更新操作，如果查到就更新
    updatePstm.setDouble(1, value.temperature)
    updatePstm.setString(2, value.id)
    updatePstm.execute()
    // 如果没有查到数据 就插入数据
    if (updatePstm.getUpdateCount == 0) {
      insertPstm.setString(1, value.id)
      insertPstm.setDouble(2, value.temperature)
    }
  }

  override def close(): Unit = {
    insertPstm.close()
    updatePstm.close()
    conn.close()
  }
}