package cn.bmsoft.batch

import java.io.File
import java.util

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.cache.DistributedCache
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object BatchDemoDisCache {
  def main(args: Array[String]): Unit = {

      val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    //注册文件
    env.registerCachedFile("./data/5/1.txt","2.txt")
    val data: DataSet[String] = env.fromElements("a","b","c","d")

    val result: DataSet[String] = data.map(new RichMapFunction[String, String] {


      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val cache: DistributedCache = getRuntimeContext.getDistributedCache
        val file: File = cache.getFile("2.txt")
        val strings: util.List[String] = FileUtils.readLines(file)
        val it: util.Iterator[String] = strings.iterator()
        while (it.hasNext) {
          val str: String = it.next()
          println("str:" + str)


        }


      }

      override def map(in: String): String = {
        in
      }
    })
    result.print()


  }
}
