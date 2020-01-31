package co.uk.gresearch.pipeline.parser

import org.apache.spark.sql.DataFrame

object RobFileParser {

  def convertData(dataFrame: DataFrame): Unit ={
    dataFrame.toDF()
  }
}
