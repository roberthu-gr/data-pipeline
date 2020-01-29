package co.uk.gresearch.aws.glue

import com.amazonaws.services.glue.DynamicFrame

object Util {

  def help(df: DynamicFrame){
    println("UTIL HELP")
    df.show()
  }
}
