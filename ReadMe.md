

Prerequisites: Spark, sbt, and scala installed locally. Couchbase Day two-node cluster.

1. Clone https://github.com/justinmichaels006/SparkOverview.git
2. From the root of that cloned directory you should be able to execute “sbt assembly”.
3. This should create a fat-jar in the target directory.
4. To execute submit this jar to spark-submit
"/usr/local/spark-2.1.1-bin-hadoop2.7/bin/spark-submit target/scala-2.11/my-app-assembly-1.0.jar”. 
Note: As you can see this is assuming you’re running from the root of the project SparkOverview otherwise you would need to provide the absolute file path to the jar file.

Examples Covered:

Simple k/v get operations to create an RDD and then the creation of a new document (line 22 of the code). 

Example output:

JsonDocument{id='airline_10123', cas=1503549567825805312, expiry=0, content={"country":"United States","iata":"TQ","callsign":"TXW","name":"Texas Wings","icao":"TXW","id":10123,"type":"airline"}, mutationToken=null}
JsonDocument{id='airline_10748', cas=1503549567833997312, expiry=0, content={"country":"United States","iata":"ZQ","callsign":"LOCAIR","name":"Locair","icao":"LOC","id":10748,"type":"airline"}, mutationToken=null}

The next is the N1QL queries and inferring the schema. This just shows our ability to execute SparkSQL and the connector does the translation to N1QL queries. Since the results are a dataframe the schema is inferred. Example output of the Airline temptable:

root
 |-- META_ID: string (nullable = true)
 |-- callsign: string (nullable = true)
 |-- country: string (nullable = true)
 |-- iata: string (nullable = true)
 |-- icao: string (nullable = true)
 |-- id: long (nullable = true)
 |-- name: string (nullable = true)
 |-- type: string (nullable = true)

The final query is a join of all landmarks for a given city (SFO by default). Example output:

+---+--------------------+--------------------+
|faa|                name|                 url|
+---+--------------------+--------------------+
|SFO|                1015|http://www.1015.com/|
|SFO|      21st Amendment|http://www.21st-a...|
|SFO|      4 Star Theatre|http://www.hkinsf...|
|SFO|          440 Castro|http://www.the440...|
|SFO|            540 Club|http://www.540-cl...|
|SFO| 6th Avenue Aquarium|http://www.6thave...|
|SFO|        826 Valencia|http://826valenci…|

Some other notes bout quickstart:
We can create a Spark Dstream based on a DCP feed from Couchbase for streaming applications. Example still pending but is coming.
Loading data into Couchbase is easy. There is an example commented out loading a json file of stock information in main(). I can send you the file if that’s interesting.
The shake.scala shows downloading a file from a URL and parsing. This is still a bit of work in progress but does work.
The device lookup is messy as we’re still working through some details there. 