package justin

import com.couchbase.client.java.document.JsonDocument
import org.apache.spark.sql.SparkSession
import com.couchbase.spark._

/**
  * Created by justin on 9/7/17.
  */
object subdoc {
  def main(sSess:SparkSession) = {

    val sc = sSess.sparkContext
    val result = sc.parallelize(Seq("airline_10"))
      .couchbaseSubdocLookup(Seq("name", "iata"), exists = Seq("type"))
      .collect()

    result.foreach(println)
    // Prints
    // SubdocLookupResult(
    //    airline_10,0,Map(name -> 40-Mile Air, iata -> Q5)
    // )

    val r3 = sc
      .parallelize(Seq("airline_10")) // Define Document IDs
      .couchbaseGet[JsonDocument]() // Load document from Couchbase
      .map(_.content()) // extract the content
      .collect() // collect all data
      .foreach(println) // print it out
    // Prints
    // airline_10 document
    //{
    //"id": 10,
    //"type": "airline",
    //"name": "40-Mile Air",
    //"iata": "Q5",
    //"icao": "MLA",
    //"callsign": "MILE-AIR",
    //"country": "United States"
    //}

  }
}