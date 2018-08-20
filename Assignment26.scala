import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Even_Number_Line{

  def main(args: Array[String]): Unit = {
    def  Get_Lines_Sum(input : String) : Double ={

      val line = input.split("")
      var number : Double = 0.0
      for (x <- line)
      {
        try{
          val value = x.toDouble
          number = number + value
        }
        catch
          {
            case ex : Exception => {}
          }
      }
      return number
    }

    println("This is the task1 of assignment of session 26")

    val conf = new SparkConf().setMaster("local[2]").setAppName("EvenLines")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    println("Spark Context Created")

    val ssc = new StreamingContext(sc, Seconds(20))

    println("Spark Streaming Context Created ")

    val lines = ssc.socketTextStream("localhost", 9999)
    val lines_filter = lines.filter(x => Get_Lines_Sum(x)%2 == 0)
    val lines_sum = lines_filter.map(x => Get_Lines_Sum(x))

    println("Lines with even sum:")

    lines_filter.print()

    println("Sum of the numbers in even lines:")
    lines_sum.reduce(_+_).print()

    ssc.start()

    ssc.awaitTermination()
  }

}