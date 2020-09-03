package cn.bmsoft.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

import scala.collection.mutable
object BatchFlatMap {

  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val userDataSet: DataSet[String] = env.fromCollection(List(
      "张三,中国,江西省,南昌市",
      "李四,中国,河北省,石家庄市",
      "Tom,America,NewYork,Manhattan"
    ))

    // 3. 使用`flatMap`将一条数据转换为三条数据


    val sultSet: DataSet[(String, String)] = userDataSet.flatMap {
      text =>
        val fieldArr: mutable.ArrayOps[String] = text.split(",")
        //   - 分别构建国家、国家省份、国家省份城市三个元组
        List(
          (fieldArr(0), fieldArr(1)),
          (fieldArr(0), fieldArr(1) +","+ fieldArr(2)),
          (fieldArr(0), fieldArr(1) +","+ fieldArr(2) +","+ fieldArr(3))

        )
    }
    sultSet.print()
  }



}
