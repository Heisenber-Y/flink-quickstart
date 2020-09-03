package cn.bmsoft.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

object BatchFromCompressFile {
  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1: DataSet[String] = env.readTextFile("/Users/yml/Downloads/1ForestBlog.tar")
    ds1.print()

  }

}
