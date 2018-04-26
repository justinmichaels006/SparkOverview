import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession

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

}
