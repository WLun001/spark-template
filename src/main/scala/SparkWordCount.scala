import org.apache.spark.sql.SparkSession

object SparkWordCount {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Word Count")
      .getOrCreate()

    val sc = spark.sparkContext
    //import to convert RDD to DataFrame
    import spark.implicits._
    // read file from local
    val videos = sc.textFile("gs://youtube-video-analysis/USvideos.txt")

    // eliminate \n, drop header
    val videosSplit = videos.map(_.split("\n"))
    val videoRemoveHead = videosSplit.mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it)

    // element(0) is a row of data
    val videosFlatten = videoRemoveHead.flatMap(x => x)

    // split by , which are not in " "
    val videosTabRemoved = videosFlatten.map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
    //val ids = videosTabRemoved.map(x => x(0)).collect

    // replace null description to "no description"
    // remove error tuple
    val videosTuple = videosTabRemoved.map(x => x match {
      case withDesc if (withDesc.size == 16) =>
        (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15));
      case withoutDesc if (withoutDesc.size == 15) =>
        (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), "no description");
      case error => ("dropThisTuple", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "")})
      .filter(_._1 != "dropThisTuple")

    //remove duplicate data
    //store as (video_id, (other atribute))
    val keyValue = videosTuple.map(x =>
      (x._1, (x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11, x._12, x._13, x._14, x._15, x._16)))

    val reduceRDD = keyValue.map(tup =>
      (tup._1, tup)).reduceByKey { case (a, b) => a }.map(_._2)

    val formattedRDD = reduceRDD.map(x => (x._1, x._2._1, x._2._2, x._2._3, x._2._4,
      x._2._5, x._2._6, x._2._7, x._2._8, x._2._9, x._2._10, x._2._11, x._2._12, x._2._13, x._2._14, x._2._15))

    def formatTitle(input: String): String = {
      var str = input.replaceAll("[,.?@(:\"!)]", "")
      str.toLowerCase();
    }

    val stopWord = sc.textFile("gs://youtube-video-analysis/englishST.txt")
      .map(_.split("\n")).flatMap(x => x)
      .sortBy(identity).collect.toList

    val categorytitle = formattedRDD.map(tup => (tup._5, tup._3.split(" ")))
      .flatMapValues(x => x).map(tup => (tup._1, formatTitle(tup._2), 1))
      .filter(x => !stopWord.contains(x._2))

    val df = categorytitle.toDF("category_id", "word", "count")

    df.write.format("parquet").save("gs://youtube-video-analysis/parquet")
  }
}