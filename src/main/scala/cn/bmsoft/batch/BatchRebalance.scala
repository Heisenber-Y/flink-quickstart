package cn.bmsoft.batch

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
object BatchRebalance {
  def main(args: Array[String]): Unit = {

      val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
      val numDataSet: DataSet[Long] = env.generateSequence(1, 100)
      val filterSet: DataSet[Long] = numDataSet.filter(_ > 8)

    val resultSet: DataSet[(Long, Long)] = filterSet.map(
      new RichMapFunction[Long, (Long, Long)] {
        override def map(in: Long): (Long, Long) = {
          (getRuntimeContext.getIndexOfThisSubtask, in)
        }
      }

    )
    resultSet.print()




  }

}
