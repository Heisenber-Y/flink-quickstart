package cn.bmsoft.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
object BatchLocal {
  def main(args: Array[String]): Unit = {
    //val env: ExecutionEnvironment = ExecutionEnvironment.createLocalEnvironment()

      val env: ExecutionEnvironment = ExecutionEnvironment.createCollectionsEnvironment
   // val ds1: DataSet[List[String]] = env.fromElements(List("1","2"))
    val ds1: DataSet[List[String]] = env.fromElements(List("1","2"))
    ds1.print()
  }

}
