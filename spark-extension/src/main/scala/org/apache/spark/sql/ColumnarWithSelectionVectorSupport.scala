package org.apache.spark.sql

import org.apache.arrow.gandiva.evaluator.SelectionVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.vectorized.ColumnarBatch

trait ColumnarWithSelectionVectorSupport {

  final def executeColumnarWithSelection(): RDD[(ColumnarBatch, SelectionVector)] = {
    doExecuteColumnarWithSelection()
  }

  protected def doExecuteColumnarWithSelection(): RDD[(ColumnarBatch, SelectionVector)]
}
