package cn.bmsoft.batch
import cn.bmsoft.batch.BatchJoin.{Score, Subject}
import org.apache.flink.api.scala._
object BatchUnion {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val scoreSet: DataSet[String] = env.readTextFile("./data/1.csv")
    val subjectSet: DataSet[String] = env.readTextFile("./data/2.txt")
    val resultSet: DataSet[String] = scoreSet.union(subjectSet)
    resultSet.print()


  }

}
