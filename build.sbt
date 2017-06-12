lazy val root = (project in file(".")).
  settings(
    name := "my-app",
    version := "1.0",
    scalaVersion := "2.11.8"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-streaming" % "2.1.0",
  "org.apache.spark" %% "spark-sql" % "2.1.0",
  "com.couchbase.client" %% "spark-connector" % "2.1.0"
)