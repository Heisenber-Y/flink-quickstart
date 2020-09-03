package cn.bmsoft.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
object BatchSinkFile {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val ds1: DataSet[Map[Int, String]] = env.fromElements(Map(1->"spark",3->"flink"))
    ds1.setParallelism(1).writeAsText("./5/1.txt",WriteMode.OVERWRITE)
    
    ds1.print()
    //env.execute()
  }
}
