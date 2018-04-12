import org.apache.spark.sql.SparkSession

object SparkWordCount {
  def main(args:Array[String]) : Unit = {

    val sc = SparkSession.builder
      .master("local[*]")
      .appName("Spark Word Count")
      .getOrCreate().sparkContext

    val lines = sc.parallelize(
      Seq("Spark Intellij Idea Scala test one",
        "Spark Intellij Idea Scala test two",
        "Spark Intellij Idea Scala test three"))

    val counts = lines
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println)

  }
}