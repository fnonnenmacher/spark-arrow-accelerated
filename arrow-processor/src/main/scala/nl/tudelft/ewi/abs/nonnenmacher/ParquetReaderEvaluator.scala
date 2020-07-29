package nl.tudelft.ewi.abs.nonnenmacher

import java.io.File

import org.apache.arrow.gandiva.expression.ArrowTypeHelper
import org.apache.arrow.vector.types.pojo.Schema

class ParquetReaderEvaluator {

  def readWholeFileWithDefaultMemoryPool(fileName: String, schema: Schema, numRows: Int): Unit = {
    val canonicalFileName: String =  new File(fileName).getCanonicalPath
    val inputSchemaBytes = ArrowTypeHelper.arrowSchemaToProtobuf(schema).toByteArray
    readWholeFileWithDefaultMemoryPool(canonicalFileName, inputSchemaBytes, numRows);
  }

  @native def readWholeFileWithDefaultMemoryPool(fileName: String, inputSchemaBytes: Array[Byte], numRows: Int): Unit
}
