package cn.bmsoft.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
object BatchSinkCollection {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val stu: DataSet[(Int, String, Double)] = env.fromElements((19, "zhangsan", 178.8),
      (17, "lisi", 168.8),
      (18, "wangwu", 184.8),
      (21, "zhaoliu", 164.8))
    stu.print
    stu.printToErr()
    print(stu.collect())
    //env.execute()

  }

}
