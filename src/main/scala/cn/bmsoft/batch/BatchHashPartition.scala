package cn.bmsoft.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
object BatchHashPartition {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 设置并行度
    env.setParallelism(2)
    val numSet: DataSet[Int] = env.fromCollection(List(10,1,13,1,51,1,1,662,2,82,2,2))

    val resultSet: DataSet[Int] = numSet.partitionByHash(_.toString)
    resultSet.writeAsText("./data/4")

  resultSet.print()

  }
}
