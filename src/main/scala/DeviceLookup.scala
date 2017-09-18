package justin

import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.spark._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

//case class DeviceModel(deviceid: String, mobilenumber: String)

object DeviceLookup {
  def main(spark:SparkSession): Unit = {
    val sparkContext = spark.sparkContext
    val sqlContext = spark.sqlContext

    //region CB "deviceauxinfo_small" schema
    val schema = StructType(
      StructField("deviceid", StringType, false)::
        StructField("mobilenumber", StringType, true)::Nil
    )
    //val cbDataFrame = sqlContext.read.couchbase(schema).as("DeviceModel")
    //cbDataFrame.printSchema()
    //cbDataFrame.show(20)
    //endregion

    //region Querying CB "deviceauxinfo_small" via joining the local device id list
    var listOfMobileNumbers = new java.util.ArrayList[String]()

    /*val deviceids = sqlContext
      .read
      .textFile("/tmp/testdeviceids_small.data")
      .as[String]
      .toDF("mobilenumber")*/
    val tFile = "/tmp/testdeviceids_small.data"
    val deviceids = spark.read.format("json").load(tFile)
    deviceids.printSchema()
    deviceids.createOrReplaceTempView("deviceids")
    deviceids.show()

    //cbDataFrame
    //  .join(deviceids, Seq("mobilenumber"), "inner")
    //  .select(cbDataFrame.col("deviceid"), cbDataFrame.col("mobilenumber"))
    //  .show()
    //endregion

    //region CB querying with named parameter
    val query_string =
      "SELECT deviceid, mobilenumber FROM `testload` WHERE (mobilenumber in [\"1088000019\", \"1088000004\", \"1088000008\", \"1088000005\", \"1088000020\", \"1095195141\"])"

    var startTime = System.currentTimeMillis()
      //for (i <- 1088000001 to 1088100000) yield listOfMobileNumbers.add(i.toString)
    var elapsed = (System.currentTimeMillis() - startTime).toInt
      println("Elapsed time %1d ms".format(elapsed))

    val identifiers = JsonObject.create()
      .put("mobilenumber_list", JsonArray.from(deviceids.toString()))

    //val cbStatement = N1qlQuery.parameterized(query_string, identifiers)
    val cbStatement = N1qlQuery.simple(query_string)
      startTime = System.currentTimeMillis()
    val cbQueryRDD = sparkContext
      .couchbaseQuery(cbStatement)
      .map(
      r => Row(r.value.getString("deviceid"), r.value.getString("mobilenumber"))
      )

    cbQueryRDD.foreach(println)
    val totRecords = cbQueryRDD.count()
      elapsed = (System.currentTimeMillis() - startTime).toInt
      println(f"Total devices retrieved: $totRecords%d")
      println("Time to execute query: %1d ms".format(elapsed))
    //endregion
  }
}
