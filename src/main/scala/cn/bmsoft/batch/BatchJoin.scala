package cn.bmsoft.batch

import org.apache.flink.api.scala._
object BatchJoin {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val scoreSet: DataSet[Score] = env.readCsvFile("./data/1.csv")
    val subjectSet: DataSet[Subject] = env.readCsvFile("./data/2.txt")

  val joinSet: JoinDataSet[Score, Subject] = scoreSet.join(subjectSet).where(2).equalTo(0)
    joinSet.print()


  }
  case class Subject(id:Int,name:String)
  case class Score(id:Int,name:String,subjectId:Int,core:Double)

}
