import java.io.{BufferedInputStream, BufferedOutputStream, File, FileOutputStream}
import java.net.URL
import org.apache.spark.sql.SparkSession

class shake (sConf: SparkSession) {

 /*
 * "info" takes a single String argument, prints it on a line,
 * and returns it.
 */
  def info(message: String): String = {
    println(message)
    message  // No additional formatting
  }

  def curl(sourceURLString: String, targetDirectoryString: String): File = {

    // The path separator on your platform: "/" on Linux and MacOS, "\" on Windows.
    val pathSeparator = File.separator

    // Use the name of the remote file as the file name in the target directory.
    // We split on the URL path elements using the separator, which is ALWAYS "/"
    // on all platforms for URLs. This gives us an array of path elements; the
    // name will be the last one.
    val sourceFileName = sourceURLString.split("/").last
    val outFileName = targetDirectoryString + pathSeparator + sourceFileName

    // Set up a connection and buffered input stream for the source file.
    println(s"Downloading $sourceURLString to $outFileName")
    val sourceURL = new URL(sourceURLString)
    val connection = sourceURL.openConnection()
    val in = new BufferedInputStream(connection.getInputStream())

    // If here, the connection was successfully opened (i.e., no exceptions thrown).
    // Now create the target directory (nothing happens if it already exists).
    val targetDirectory = new File(targetDirectoryString)
    targetDirectory.mkdirs()

    // Setup the output file and a stream to write to it.
    val outFile = new File(outFileName)
    val out = new BufferedOutputStream(new FileOutputStream(outFile))

    // Create a buffer to hold the in-flight bytes.
    val hundredK = 100*1024
    val bytes = Array.fill[Byte](hundredK)(0)   // Create byte buffer, elements set to 0
    // Array elements are _mutable_.
    // Loop until we've read everything.
    var loops = 0                               // A counter for progress feedback.
    var count = in.read(bytes, 0, hundredK)     // Read up to "hundredK" bytes at a time.
    while (count != -1) {                       // Haven't hit the end of input yet?
      if (loops % 10 == 0) print(".")         // Print occasional feedback.
      loops += 1                              // increment the counter.
      out.write(bytes, 0, count)              // Write to the new file.
      count = in.read(bytes, 0, hundredK)     // Read the next chunk and loop...
    }
    println("\nFinished!")
    in.close()                                  // Clean up! Close file & stream handles
    out.flush()
    out.close()
    outFile                                     // Returned file (if we got this far)
  }

  val shakespeare = new File("data/shakespeare")

  val success = if (shakespeare.exists == false) {   // doesn't exist already? In Java, I would need parentheses: .exists()
    if (shakespeare.mkdirs() == false) {           // did the attempt fail??
      error(s"Failed to create directory path: $shakespeare")  // ignore returned string
      false
    } else {                                       // successful
      info(s"Created $shakespeare")
      true
    }
  } else {
    info(s"$shakespeare already exists")
    true
  }
  println("success = " + success)

  val pathSeparator = File.separator
  val targetDirName = shakespeare.toString
  val urlRoot = "http://www.cs.usyd.edu.au/~matty/Shakespeare/texts/comedies/"
  val plays = Seq(
    "tamingoftheshrew", "comedyoferrors", "loveslabourslost", "midsummersnightsdream",
    "merrywivesofwindsor", "muchadoaboutnothing", "asyoulikeit", "twelfthnight")

  if (success) {
    println(s"Downloading plays from $urlRoot.")
    val successes = for {
      play <- plays
      playFileName = targetDirName + pathSeparator + play
      playFile = new File(playFileName)
      if (playFile.exists == false)
      file = curl(urlRoot + play, targetDirName)
    } yield {
      info(s"Downloaded $play and wrote $file")
      s"$playFileName:\tSuccess!"
    }

    println("Finished!")

    successes.foreach(println)
  }

  // If we already have everything we can start here...
  println("Pass println as the function to use for each element:")
  plays.foreach(println)

  println("\nUsing an anonymous function that calls println: `str => println(str)`")
  println("(Note that the type of the argument `str` is inferred to be String.)")
  plays.foreach(str => println(str))

  val iiFirstPass1 = sConf.sparkContext.wholeTextFiles(shakespeare.toString).
    flatMap { location_contents_tuple2 =>
      val words = location_contents_tuple2._2.split("""\W+""")
      val fileName = location_contents_tuple2._1.split(pathSeparator).last
      words.map(word => ((word, fileName), 1))
    }.
    reduceByKey((count1, count2) => count1 + count2).
    map { word_file_count_tup3 =>
      (word_file_count_tup3._1._1, (word_file_count_tup3._1._2, word_file_count_tup3._2))
    }.
    groupByKey.
    sortByKey(ascending = true).
    mapValues { iterable =>
      val vect = iterable.toVector.sortBy { file_count_tup2 =>
        (-file_count_tup2._2, file_count_tup2._1)
      }
      vect.mkString(",")
    }
  iiFirstPass1.take(50).foreach(println)

}
