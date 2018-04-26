import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class bricks {

  def main(spark:SparkSession): Unit = {
    val sparkContext = spark.sparkContext.setLogLevel("WARN")
    val sqlContext = spark.sqlContext

  val full_csv = spark.sparkContext.parallelize(Array(
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
    "10, MNO, Foo10")) //.saveAsTextFile("/tmp/csvfile.csv")

    val x = spark.sparkContext.parallelize(List(full_csv)).collect().drop(1)
    println(x)


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


    def removeFirst[A](xs: Iterable[A]) = xs.drop(1)
    val e = removeFirst(List(full_csv)) map println

    import sqlContext.implicits._
  val csvfile = sqlContext.read.format("csv").option("inferSchema", "true").load("/tmp/csvfile.csv").orderBy($"_co1".desc)
    val header = csvfile.first()
    val csvtable = csvfile.filter(row => row != header)
    csvtable.createOrReplaceTempView("table")
    csvtable.printSchema()
    csvtable.show()
    csvfile.show()
    csvfile.cache()

    val name1 = header.get(0)
    val name2 = header.get(1)
    val name3 = header.get(2)
    case class header_row(name1: String, name2: String, name3: String)
    val dataWithSchema = new header_row(csvtable.col("_c0").toString(), csvtable.col("_c1").toString(), csvtable.col("_c2").toString())
    println(dataWithSchema)

    case class DeviceIoTData (battery_level: Long, c02_level: Long, cca2: String, cca3: String, cn: String, device_id: Long, device_name: String, humidity: Long, ip: String, latitude: Double, lcd: String, longitude: Double, scale:String, temp: Long, timestamp: Long)
    case class BabyData (data: ArrayType, meta: StructType)

    // Convenience function for turning JSON strings into DataFrames.
    def jsonToDataFrame(j: String, s: StructType = null): DataFrame = {
      // SparkSessions are available with Spark 2.0+
      val reader = spark.read
      Option(s).foreach(reader.schema)
      reader.json(j)
    }

    val path = "/tmp/rows.json"

    val ds = spark.read.json(path).as[DeviceIoTData]
    val aRDD = spark.sparkContext.textFile("dbfs:/databricks-datasets/tpch/data-001/part")
    val schemaString = "partkey name mfgr brand type size container retailprice comment"

    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // Convert records of the RDD to Rows and map the defined schema
    val rowRDD = aRDD.map(_.split("|"))
      .map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3),
        attributes(4), attributes(5), attributes(6), attributes(7), attributes(8)))

    // Apply the schema to the RDD
    val partDF = spark.createDataFrame(rowRDD, schema)
    partDF.show(5)

    // Creates a temporary view using the DataFrame
    partDF.createOrReplaceTempView("parts")

    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("SELECT * FROM parts").show(5)

    //Babyname parsing
    val aa = sqlContext.read.option("multiline", "true").json(path).select("meta.view.columns.name").map(attributes => Row(attributes(0), attributes(1),
      attributes(2), attributes(3), attributes(4), attributes(5), attributes(6), attributes(7),
      attributes(8), attributes(9), attributes(10), attributes(11), attributes(12))).toString()
    val BabyDF = sqlContext.read.option("multiline", "true").json(path).select("data").toDF(aa)
    BabyDF.printSchema()
    BabyDF.show(5)

    val mydf = sqlContext.read.json(path).toDF();
    mydf.printSchema();

    val app = mydf.select("meta.view.columns.name");
    app.printSchema();
    app.show();
    val appName = app.select("data");
    appName.printSchema();
    appName.show();

    // Delete Block
    //val fields2 = aa.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    //val schema2 = StructType(fields2)
    // Convert records of the RDD to Rows and map the defined schema
    //val rowRDD2 = bb.map(_.split(","))
    //  .map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4),
    //    attributes(5), attributes(6), attributes(7), attributes(8), attributes(9), attributes(10),
    //    attributes(11), attributes(12)))
    // Apply the schema to the RDD
    //val babyDF2 = spark.createDataFrame(rowRDD2, schema2)
    //babyDF2.show(5)

  }
}
