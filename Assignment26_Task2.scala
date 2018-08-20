import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Offensive_Words_Count {

  def main(args: Array[String]): Unit = {

    println("This is the task2 of assignment of session 26")

    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkSteamingExample")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")
    println("Spark Context Created")

    val offensive_word_list: Set[String] = Set("idiot", "fool", "bad","nonsense")
    println(s"$offensive_word_list")

    val ssc = new StreamingContext(sc, Seconds(20))
    println("Spark Streaming Context Created !")

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" ")).map(x => x)
    val Offensive_Word_Count = words.filter(x => offensive_word_list.contains(x)).map(x => (x, 1)).reduceByKey(_ + _)

    Offensive_Word_Count.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
