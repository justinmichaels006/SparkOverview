package justin

import com.couchbase.spark.sql._
import org.apache.spark.sql.SparkSession

//case class DeviceModel(deviceid: String, mobilenumber: String)

object DeviceLookup {
  def main(spark:SparkSession): Unit = {
    val sparkContext = spark.sparkContext.setLogLevel("WARN")
    val sqlContext = spark.sqlContext

    import sqlContext.implicits._

    //Simple ingest of mobile numbers
    val tFile = "/tmp/testdeviceids_small.data"
    val mobileNumbers = spark.read.text(tFile).as("pnum")
    mobileNumbers.printSchema()
    mobileNumbers.createOrReplaceTempView("mobilenumbers")
    mobileNumbers.show()
    println(mobileNumbers)

    //Consider a simple query
    //... "SELECT deviceid, mobilenumber FROM `testload` WHERE (mobilenumber in [\"1088000019\", \"1088000004\", \"1088000008\", \"1088000005\", \"1088000020\", \"1095195141\"])"
    var startTime = System.currentTimeMillis()

    //Create a DataFrame from Couchbase and define the schema manually
    /*val schema = StructType(
      StructField("deviceid", StringType, false)::
        StructField("mobilenumber", StringType, true)::Nil
    )
    var cbDataFrame = sqlContext.read.couchbase(schema)
    cbDataFrame.printSchema()
    cbDataFrame.show(20)*/

    // Create a DataFrame from Couchbase but use SparkQL
    val df = sqlContext.read.couchbase() // no additional filter needed
    //val cbDataFrame2 = df.select("mobilenumber", "deviceid")
    //val listnumbers = mobileNumbers.select("value").rdd.map(r => r(0)).collect()
    //println("debug listnumbers" + listnumbers.toString)

    // Filer the DataFrame
    val numbers = Seq("1088000019", "1088000004", "1088000008", "1088000005", "1088000020", "1095195141")
    val filtered_df = df.where($"mobilenumber".isin(numbers:_*)) // add the where in clause with spark sql
    filtered_df.show()

    // Create a DataFrame directly with infered schema
    /*val cbDataFrame3 = sqlContext
      .read
      .format("com.couchbase.spark.sql")
      .load()
      .select("mobilenumber", "deviceid")
      .where("deviceid IS NOT MISSING")
    cbDataFrame3.show(10)*/
    
    var elapsed = (System.currentTimeMillis() - startTime).toInt
    println("Elapsed time %1d ms".format(elapsed))

    //TODO Partioning Queries

    //TODO Prtitioning k/v access


    //TODO spark streaming

    // Profile the processing time
    val totRecords = filtered_df.count()
      elapsed = (System.currentTimeMillis() - startTime).toInt
      println(f"Total devices retrieved: $totRecords%d")
      println("Time to execute query: %1d ms".format(elapsed))
  }
}
