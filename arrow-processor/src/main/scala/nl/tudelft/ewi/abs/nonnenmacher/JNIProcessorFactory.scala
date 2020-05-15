package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.gandiva.evaluator.GandivaJniLoader
import org.apache.arrow.gandiva.expression.ArrowTypeHelper
import org.apache.arrow.vector.types.pojo.Schema

object JNIProcessorFactory {

  private lazy val jni = {
    GandivaJniLoader.load()
    System.loadLibrary("protobuf")
    System.loadLibrary("arrow")
    System.loadLibrary("parquet")
    System.loadLibrary("fletcher_echo")
    System.loadLibrary("fletcher")
    System.loadLibrary("arrow-processor-native")

    new Initializer();
  }

  def copyProcessor(schema: Schema): JNIProcessor = {

    val schemaAsBytes = ArrowTypeHelper.arrowSchemaToProtobuf(schema).toByteArray

    val ptr = jni.initCopyProcessor(schemaAsBytes)

    new JNIProcessor(ptr, schema);
  }

  def threeIntAddingProcessor(inputSchema: Schema, outputSchema: Schema): JNIProcessor = {

    val schemaAsBytes = ArrowTypeHelper.arrowSchemaToProtobuf(inputSchema).toByteArray

    val ptr = jni.initThreeIntAddingProcessor(schemaAsBytes)

    new JNIProcessor(ptr, outputSchema);
  }

  def fletcherEchoSumProcessor(inputSchema: Schema, outputSchema: Schema): JNIProcessor = {
    val schemaAsBytes = ArrowTypeHelper.arrowSchemaToProtobuf(inputSchema).toByteArray

    val ptr = jni.initFletcherProcessor(schemaAsBytes)

    new JNIProcessor(ptr, outputSchema);
  }

  def parquetReader(fileName: String, outputSchema: Schema, numRows: Int): NativeParquetReader = {

    val schemaAsBytes = ArrowTypeHelper.arrowSchemaToProtobuf(outputSchema).toByteArray
    val processId = jni.initNativeParquetReader(fileName, schemaAsBytes, numRows)

    new NativeParquetReader(processId, outputSchema, numRows)
  }

  private class Initializer {

    @native def initThreeIntAddingProcessor(schema: Array[Byte]): Long

    @native def initCopyProcessor(schema: Array[Byte]): Long;

    @native def initFletcherProcessor(schema: Array[Byte]): Long

    @native def initNativeParquetReader(fileName: String, schemaAsBytes: Array[Byte], numRows: Int): Long
  }
}