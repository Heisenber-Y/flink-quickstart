package cn.bmsoft.batch

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
object BatchAggregate {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1: DataSet[(String, Int)] = env.fromCollection(List(("java" , 1) , ("java", 1) ,("scala" , 1)  ))

  val groupDataSet: GroupedDataSet[(String, Int)] = ds1.groupBy(0)
    val resultDataSet: AggregateDataSet[(String, Int)] = groupDataSet.aggregate(Aggregations.SUM,1)
    resultDataSet.print()


  }

}
