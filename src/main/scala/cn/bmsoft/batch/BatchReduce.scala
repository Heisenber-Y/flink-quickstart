package cn.bmsoft.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
object BatchReduce {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val dataSet: DataSet[(String, Int)] = env.fromCollection(List(("a" , 1) , ("b", 2) ,("c" , 3) ))
    val result: DataSet[(String, Int)] = dataSet.reduce(
      (a1, a2) =>
        (a2._1, a2._2+a1._2)
    )
    result.print()
  }
}
