import co.uk.gresearch.aws.glue.Util
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.amazonaws.services.glue.util.{GlueArgParser, JsonOptions}
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

object S3SparkTest {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sc)

    val infilesBucket = "s3://refdata-default-refdata-in-file-uploads"
    val outputBucket = "s3://refdata-default-refdata-in-file-uploads-datalake"



    val argsWithIdx = args.zipWithIndex
    val filenameOpt = argsWithIdx.find( _._1 == "ROBTEST").map{case (_,idx) => argsWithIdx(idx+1)._1}
    val (infilePath,outputPath) = filenameOpt match{
      case Some(filename) =>
        println(s"found filename arg: $filename")
        (s"$infilesBucket/$filename",s"$outputBucket/${filename.split("\\.")(0)}")
      case None =>
            println("Missing filename arg")
          throw new IllegalStateException("Missing mandaroty JOB argument")
    }

    val inputPaths = Set(infilePath)
    val source = glueContext.getSourceWithFormat("s3", JsonOptions(Map("paths" -> inputPaths)),format = "csv")
    val df: DynamicFrame = source.getDynamicFrame()
    println("about to map roots")
    val h = df.map{r =>
      println(s"printing root: ${r.getRoot}");
      r}

    println("about to print foreach")
    val hdf =  h.toDF()
    hdf.foreach(f => println(f))
    println("counting...")
    println(hdf.count())
    println("about to helper print")
    Util.help(df)
    println("about to show")
    df.show(1)

    val output = glueContext.getSinkWithFormat("s3",JsonOptions(Map("path" -> outputPath)),format = "parquet")
    output.writeDynamicFrame(df)
  }
}

