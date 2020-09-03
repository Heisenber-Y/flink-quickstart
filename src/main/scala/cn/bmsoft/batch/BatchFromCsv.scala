package cn.bmsoft.batch

import org.apache.flink.api.scala._

object BatchFromCsv {
  def main(args: Array[String]): Unit = {

  val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    case class Student(id:Int,name:String)

    val ds1: DataSet[Student] = env.readCsvFile("./data/1.csv")
    ds1.print()

  }

}
