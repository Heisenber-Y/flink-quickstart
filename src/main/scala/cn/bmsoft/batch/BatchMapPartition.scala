package cn.bmsoft.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
object BatchMapPartition {


  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val dataSet: DataSet[String] = env.fromCollection(List("1,张三", "2,李四", "3,王五", "4,赵六"))

    // 4. 使用`mapPartition`操作执行转换
    val resultSet: DataSet[User] = dataSet.mapPartition(
      text =>
        text.map(
          ele => {
            val arr: Array[String] = ele.split(",")
            User(arr(0), arr(1))
          }

        )


    )
    resultSet.print()


  }
  // 3. 创建一个`User`样例类
  case class User(id:String, name:String)

}
