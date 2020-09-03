package cn.bmsoft.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
object ReduceGroup {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1: DataSet[(String, Int)] = env.fromCollection(List(("java" , 1) , ("java", 1) ,("scala" , 1)  ))

    val groupDataSet: GroupedDataSet[(String, Int)] = ds1.groupBy(_._1)
    val resultSet: DataSet[(String, Int)] = groupDataSet.reduceGroup(
      itet =>
        itet.reduce(
          (a1, a2) => (a1._1, a1._2 + a2._2)
        )
    )
    resultSet.print()




  }

}
