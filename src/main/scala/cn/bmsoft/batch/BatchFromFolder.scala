package cn.bmsoft.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration

object BatchFromFolder {


  def main(args: Array[String]): Unit = {

  val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val parameters = new Configuration()
    // recursive.file.enumeration 开启递归
  parameters.setBoolean("recursive.file.enumeration",true)
    val ds1: DataSet[String] = env.readTextFile("/Users/yml/Documents/Source/1").withParameters(parameters)
    ds1.print()


  }

}
