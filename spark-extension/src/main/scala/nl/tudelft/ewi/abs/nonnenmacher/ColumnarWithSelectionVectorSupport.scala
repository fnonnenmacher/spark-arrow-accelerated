package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.gandiva.evaluator.SelectionVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.vectorized.ColumnarBatch

trait ColumnarWithSelectionVectorSupport {

  protected def doExecuteColumnarWithSelection(): RDD[(ColumnarBatch, SelectionVector)]

}
