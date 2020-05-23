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

  def parquetReader(fileName: String, fileSchema: Schema, outputSchema: Schema, batchSize: Int): NativeParquetReader = {

    val inputSchemaBytes = ArrowTypeHelper.arrowSchemaToProtobuf(outputSchema).toByteArray
    val outputSchemaBytes = ArrowTypeHelper.arrowSchemaToProtobuf(outputSchema).toByteArray
    val processId = jni.initNativeParquetReader(fileName, inputSchemaBytes, outputSchemaBytes, batchSize)

    new NativeParquetReader(processId, outputSchema, batchSize)
  }

  private class Initializer {

    @native def initThreeIntAddingProcessor(schema: Array[Byte]): Long

    @native def initCopyProcessor(schema: Array[Byte]): Long;

    @native def initFletcherProcessor(schema: Array[Byte]): Long

    @native def initNativeParquetReader(fileName: String, inputSchemaBytes: Array[Byte], outputSchemaBytes: Array[Byte], numRows: Int): Long
  }
}