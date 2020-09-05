package cn.bmsoft.batch

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
object BroadCast {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val studentDataSet: DataSet[(Int, String)] = env.fromCollection(List((1, "张三"), (2, "李四"), (3, "王五")))

    val scoreDataSet: DataSet[(Int, String, Int)] = env.fromCollection(List((1, "语文", 50), (2, "数学", 70), (3, "英文", 86)))

    // 1. 使用`RichMapFunction`对`成绩`数据集进行map转换
    // 将成绩数据(学生ID，学科，成绩) -> (学生姓名，学科，成绩)
    val resultDataSet: DataSet[(String, String, Int)] =scoreDataSet.map(new RichMapFunction[(Int,String,Int),(String,String,Int)] {
      var bc_studentList: List[(Int, String)] = null

      // - 重写`open`方法中，获取广播数据爱上
      override def open(parameters: Configuration): Unit = {
        import scala.collection.JavaConverters._
          bc_studentList = getRuntimeContext.getBroadcastVariable[(Int,String)]("bc_student").asScala.toList
      }


      override def map(value: (Int, String, Int)): (String, String, Int) = {
        val studentId: Int = value._1
        val tuples: List[(Int, String)] = bc_studentList.filter((x: (Int, String)) => x._1 == studentId)
        (tuples(0)._2,value._2,value._3)

      }
    }).withBroadcastSet(studentDataSet, "bc_student")

  resultDataSet.print()

  }

}
