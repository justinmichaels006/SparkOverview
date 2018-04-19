import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class ml(spark:SparkSession) {

  val sc = spark.sparkContext.setLogLevel("WARN")
  val sqc = spark.sqlContext

  // Load training data
  val training = spark.read.format("libsvm").load("/tmp/ml.txt")
  spark.read.text("/tmp/ml.txt").show()

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)

  // Fit the model
  val lrModel = lr.fit(training)

  // Print the coefficients and intercept for logistic regression
  println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
  println(lrModel.coefficientMatrix)

  // We can also use the multinomial family for binary classification
  val mlr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)
    .setFamily("multinomial")

  val mlrModel = mlr.fit(training)

  // Print the coefficients and intercepts for logistic regression with multinomial family
  println(s"Multinomial coefficients: ${mlrModel.coefficientMatrix}")
  println(s"Multinomial intercepts: ${mlrModel.interceptVector}")

  case class DeviceIoTData (battery_level: Long, c02_level: Long, cca2: String, cca3: String, cn: String, device_id: Long, device_name: String, humidity: Long, ip: String, latitude: Double, lcd: String, longitude: Double, scale:String, temp: Long, timestamp: Long)

  val ds = spark.read.json(s"dbfs:/tmp/rows.json").as[DeviceIoTData]
  val ds2 = sqc.read.option("multiline", "true").json("/tmp/rows.json")

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

}
