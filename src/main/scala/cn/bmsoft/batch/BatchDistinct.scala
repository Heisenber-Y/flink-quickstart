package cn.bmsoft.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
object BatchDistinct {


  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1: DataSet[(String, Int)] = env.fromCollection(List(("java" , 1) , ("java", 1) ,("scala" , 1)  ))
    val resultDataSet: DataSet[(String, Int)] = ds1.distinct(0)
    resultDataSet.print()


  }
}
