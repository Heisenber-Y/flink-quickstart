package cn.bmsoft.batch

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
object BatchSortPartition {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 2. 使用`fromCollection`构建测试数据集
    val wordDataSet = env.fromCollection(List("hadoop", "hadoop", "hadoop", "hive", "hive", "spark", "spark", "flink"))
  env.setParallelism(2)
      val sortDataSet: DataSet[String] = wordDataSet.sortPartition(_.toString,Order.DESCENDING)

  sortDataSet.writeAsText("./data/4")
    sortDataSet.print()

  }


}
