package justin

import org.apache.spark.sql.SparkSession

object Main {

  // warehouseLocation points to the default location for managed databases and tables
  //val warehouseLocation = "spark-warehouse"

  def main(args: Array[String]): Unit = {
    val SS = SparkSession
      .builder()
      .appName("importer")
      .master("local[*]") // use the JVM as the master, great for testing
      //.master("spark://192.168.61.1:7077")
      .config("spark.couchbase.nodes", "192.168.61.101") // connect to couchbase
      .config("spark.couchbase.nodes", "192.168.61.102")
      .config("spark.couchbase.bucket.travel-sample", "password") // open the bucket with password
      //.config("spark.couchbase.bucket.testload", "password") // open the bucket with password
      //.config("spark.sql.warehouse.dir", "/vagrant/")
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      //.enableHiveSupport()
      .getOrCreate()

    println("DEBUG:", SS.version)
    //val tFile = "/tmp/stocks.json"
    //val jFile = SS.read.format("json").load(tFile)
    //jFile.createOrReplaceTempView("thestocks")
    //jFile.printSchema()
    // Here we can load the data into Couchbase easily leveraging the connector
    // jFile.write.couchbase(Map{"idField" -> "Ticker"})

    //shake.main(SS)
    quickstart.main(SS)
    //subdoc.main(SS)
    //DeviceLookup.main(SS)
    println("DEBUG: complete")
    SS.close()
  }
}