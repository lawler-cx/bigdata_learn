import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordcount").setMaster("spark://10.6.3.11:7077").set("spark.executor.memory","512m").set("spark.cores.max","10")
    //val conf = new SparkConf().setAppName("wordcount").setMaster("local[2]")

    val sc = new SparkContext(conf)
    sc.textFile("hdfs://10.6.3.13:8020/works.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile("hdfs://10.6.3.13:8020/test/wc6")
    sc.stop()


  }

}
