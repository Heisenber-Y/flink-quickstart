package cn.bmsoft.batch

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
object BatchDemoCounter {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val data: DataSet[String] = env.fromElements("a","b","c","d")


    val res: DataSet[String] = data.map(new RichMapFunction[String, String] {
      val numLines = new IntCounter()

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        getRuntimeContext.addAccumulator("num-lines", this.numLines)
      }

      var sum = 0

      override def map(in: String): String = {
        sum += 1
        System.out.println("sum：" + sum)
        System.out.println("in：" + in)

        this.numLines.add(1)
        in

      }
    }).setParallelism(1)
    res.writeAsText("./data/5/1.txt")
    val jobResult: JobExecutionResult = env.execute("aaa")
    val num: Unit = jobResult.getAccumulatorResult("numLines")
    println("num:="+num)


  }

}
