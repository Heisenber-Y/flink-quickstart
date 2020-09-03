package cn.bmsoft.batch

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object BatchFromCollection {
  def main(args: Array[String]): Unit = {


    //获取flink执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.api.scala._

    //0.用element创建DataSet(fromElements)
    val ds0: DataSet[String] = env.fromElements("spark","flink")
    ds0.print()
    //1.用Tuple创建DataSet(fromElements)
    val ds1: DataSet[(Int, String)] = env.fromElements((1, "spark"), (2, "flink"))
    ds1.print()

    //2.用Array创建DataSet
    val ds2: DataSet[Array[String]] = env.fromElements(Array("spark","spark"))
    ds2.print()

    //3.用ArrayBuffer创建DataSet
    val ds3: DataSet[ArrayBuffer[String]] = env.fromElements(ArrayBuffer("spark","flink"))
    ds3.print()

    //4.用List创建DataSet
    val ds4: DataSet[List[String]] = env.fromElements(List("1","2"))
    ds4.print()

    //5.用ListBuffer创建DataSet
    env.fromElements(ListBuffer("spark","flink")).print()


    //6.用Vector创建DataSet
    env.fromElements(Vector("vector","flink")).print()

    //7.用Queue创建DataSet
    env.fromElements(mutable.Queue("queue","flink")).print()

    //8.用Stack创建DataSet
    env.fromElements(mutable.Stack("stack","flink")).print()

    //9.用Stream创建DataSet（Stream相当于lazy List，避免在中间过程中生成不必要的集合）
  env.fromElements(Stream("stream","flink")).print()
    //10.用Seq创建DataSet
    env.fromElements(Seq("Seq","flink")).print()

    //11.用Set创建DataSet
   // env.fromElements(Set("Set","spark")).print()
    env.fromCollection(Set("Set","spark")).print()

    //12.用Iterable创建DataSet
    env.fromCollection(Iterable("Iterable","spark")).print()

    //13.用ArraySeq创建DataSet
    env.fromElements(mutable.ArraySeq("ArraySeq","flink")).print()

    //14.用ArrayStack创建DataSet
    env.fromElements(mutable.ArrayStack("ArrayStack","spark")).print()

    //15.用Map创建DataSet
    env.fromElements(Map(1->"spark",2->"flink")).print()

    //16.用Range创建DataSet
    env.fromElements(Range(1,10)).print()

    //17.用fromElements创建DataSet
    val ds10: DataSet[Long] = env.generateSequence(1,9)
    ds10.print()



  }

}
