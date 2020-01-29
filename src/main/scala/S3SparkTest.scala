import co.uk.gresearch.aws.glue.Util
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext

object S3SparkTest {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sc)

    val inputPaths = Set("s3://refdata-default-refdata-in-file-uploads")
    val source = glueContext.getSourceWithFormat("s3", JsonOptions(Map("paths" -> inputPaths)),format = "csv")
    val df: DynamicFrame = source.getDynamicFrame()
    Util.help(df)
    df.show(1)
  }
}

