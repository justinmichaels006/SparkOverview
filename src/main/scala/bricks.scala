import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

class bricks {

  def main(spark:SparkSession): Unit = {
    val sparkContext = spark.sparkContext.setLogLevel("WARN")
    val sqlContext = spark.sqlContext

  val full_csv = sparkContext(Array(
    "col_1, col_2, col_3",
    "1, ABC, Foo1",
    "2, ABCD, Foo2",
    "3, ABCDE, Foo3",
    "4, ABCDEF, Foo4",
    "5, DEF, Foo5",
    "6, DEFGHI, Foo6",
    "7, GHI, Foo7",
    "8, GHIJKL, Foo8",
    "9, JKLMNO, Foo9",
    "10, MNO, Foo10"))//.saveAsTextFile("/tmp/csvfile.csv")

      val s1 = Array("Row-Key-001", "K1", "10", "A2", "20", "K3", "30", "B4", "42", "K5", "19", "C20", "20")
      val s2 = List("Row-Key-002 , X1, 20, Y6, 10, Z15, 35, X16, 42")
      val s3 = List("Row-Key-003 , L4, 30, M10, 5, N12, 38, O14, 41, P13, 8")

    // Construct a map using the list
    val newMap = s1.foreach( (c: String) => println(c) )
    val thek1 = s1(0)

    val babys = sqlContext.read.json("/tmp/rows.json")

    val peopleDF = spark.sparkContext
      .textFile("/tmp/rows.json")
      .map(_.split(","))
    val baby2 = spark.sparkContext.wholeTextFiles("/tmp/rows.json").collect()
    val baby3 = sqlContext.createDataset(baby2)

    val aSC1 = sqlContext.read.option("header", "true").json("/databricks-datasets/tpch/data-001/part/")
    val datafile = sqlContext.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/tmp/dataframe_sample.csv")
    datafile.printSchema()
    datafile.show()

    val string2kv = Map(s1 map {s => (s1(0), s)} : _*).toArray
    val strings2kv = spark.sparkContext.makeRDD(s1).map(i => (s1(0), i)).collect().drop(0)
    val df = sqlContext.read.format("csv").load("/tmp/dataframe_sample.csv")
    df.show()

    val result = s1.zipWithIndex // this adds an index to the elements
        .map({ case (arr, i) => s1(0) -> (arr).mkString(",")}) // concats the array elements
        .toMap // this transforms the Array of tuples into a map


    val x = ArrayBuffer(full_csv)
    x.remove(0)

    def removeFirst[A](xs: Iterable[A]) = xs.drop(1)
    val e = removeFirst(List(full_csv)) map println

  val csvfile = sqlContext.read.format("csv").option("inferSchema", "true").load("/tmp/csvfile.csv")
    val header = csvfile.first()
    val csvtable = csvfile.filter(row => row != header)
    csvtable.createOrReplaceTempView("table")
    csvtable.printSchema()
    csvtable.show()
    csvfile.show()
    csvfile.cache()

  }
}
