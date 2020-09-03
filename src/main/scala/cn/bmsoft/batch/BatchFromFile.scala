package cn.bmsoft.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

object BatchFromFile {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //使用readTextFile读取本地文件
    //初始化环境
    //加载数据
    //val datas: DataSet[String] = env.readTextFile("./data/1.csv")
    val datas: DataSet[String] = env.readTextFile("./data/2.txt")
    datas.print()
  }


}
