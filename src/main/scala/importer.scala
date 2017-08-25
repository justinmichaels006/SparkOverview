package justin

import com.couchbase.spark.sql._
import org.apache.spark.sql.SparkSession

object Main {

  // warehouseLocation points to the default location for managed databases and tables
  //val warehouseLocation = "spark-warehouse"

  def main(args: Array[String]): Unit = {
    val SS = SparkSession
      .builder()
      .appName("importer")
      //.master("local[*]") // use the JVM as the master, great for testing
      .master("spark://192.168.61.1:7077")
      .config("spark.couchbase.nodes", "192.168.61.101") // connect to couchbase
      .config("spark.couchbase.nodes", "192.168.61.102")
      .config("spark.couchbase.bucket.testload", "testload") // open the bucket with password
      //.config("spark.sql.warehouse.dir", "/vagrant/")
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      //.enableHiveSupport()
      .getOrCreate()
      //.sparkContext.addFile("/tmp/stocks.json")

    /*val conf = new SparkConf()
      //.setMaster("local[*]")
      .setMaster("spark://192.168.61.1:7077")
      .setAppName("StreamingExample")
      .set("com.couchbase.bucket.travel-sample", "")
    val ssc = new StreamingContext(conf, Seconds(5))*/

    // Load Airports from Couchbase ?var useage?
    //val airports = sConf.read.couchbase(schemaFilter = EqualTo("type", "airport"))
    //airports.printSchema()
    //val fileWorkerNode = sConf.sparkContext.addFile("/tmp/stocks.json")

    println("DEBUG:", SS.version)
    val tFile = "/tmp/stocks.json"
    val jFile = SS.read.format("json").load(tFile)
    jFile.createOrReplaceTempView("thestocks")
    jFile.printSchema()
    jFile.write.couchbase(Map{"idField" -> "Ticker"})

    shake.main(SS)
    println("DEBUG:")

  }
}