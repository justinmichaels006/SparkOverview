import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.spark._
import com.couchbase.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.EqualTo

/**
  * Created by justin on 9/7/17.
  */
object quickstart {

  def main(sSess:SparkSession) = {

    //Create RDD by reading from Couchbase
    val sCont = sSess.sparkContext
    sCont.couchbaseGet[JsonDocument](Seq("airline_10123", "airline_10748"))
        .collect()
        .foreach(println)

    //Create RDD and persist to Couchbase
    sCont
        .couchbaseGet[JsonDocument](Seq("airline_10123", "airline_10748"))
        .map(oldDoc => {
          val id = "my_" + oldDoc.id()
          val content = JsonObject.create().put("name", oldDoc.content().getString("name"))
          JsonDocument.create(id, content)
        })
        .saveToCouchbase()

    // Create a DataFrame with Schema Inference
    val airlines = sSess.read.couchbase(schemaFilter = EqualTo("type", "airline"))
    airlines.createOrReplaceTempView("airline")
    val airports = sSess.read.couchbase(schemaFilter = EqualTo("type", "airport"))
    airports.createOrReplaceTempView("airport")
    val landmarks = sSess.read.couchbase(schemaFilter = EqualTo("type", "landmark"))
    landmarks.createOrReplaceTempView("landmarks")

    // Print The Schema
    airlines.printSchema()
    airports.printSchema()
    landmarks.printSchema()

    // find all landmarks in the same city as the given FAA code
    val toFind = "SFO" // try SFO or LAX
    airports
      .join(landmarks, airports("city") === landmarks("city"))
      .select(airports("faa"), landmarks("name"), landmarks("url"))
      .where(airports("faa") === toFind and landmarks("url").isNotNull)
      .orderBy(landmarks("name").asc)
      .show(20)
  }
}
