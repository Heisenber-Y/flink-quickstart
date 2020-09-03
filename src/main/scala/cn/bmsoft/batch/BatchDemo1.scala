package cn.bmsoft.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
object BatchDemo1 {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    case class User(id:String, name:String)
    val textDataSet: DataSet[String] = env.fromCollection(List("1,张三", "2,李四", "3,王五", "4,赵六"))

    val userDataSet: DataSet[User] = textDataSet.map {
      text => {
        val fieldArr: Array[String] = text.split(",")
        User(fieldArr(0), fieldArr(1))

      }

    }
    userDataSet.print()





  }

}
