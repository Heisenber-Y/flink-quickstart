package cn.bmsoft.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
object BatchFilter {

  def main(args: Array[String]): Unit = {



     val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val filterData: DataSet[String] = env.fromCollection(List("hadoop", "hive", "spark", "flink"))

    val resultData: DataSet[String] = filterData.filter(
      _.startsWith("h")
    )
    resultData.print()



  }



}
