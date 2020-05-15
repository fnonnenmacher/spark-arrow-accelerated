#include "CopyProcessor.h"
#include "nl_tudelft_ewi_abs_nonnenmacher_JNIProcessorFactory_Initializer.h"
#include "jni/ProtobufSchemaDeserializer.h"

std::shared_ptr<arrow::RecordBatch> CopyProcessor::process(std::shared_ptr<arrow::RecordBatch> input) {
    return input;
}

JNIEXPORT jlong JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_JNIProcessorFactory_00024Initializer_initCopyProcessor
        (JNIEnv * env , jobject, jbyteArray schema_arr){

    jsize schema_len = env->GetArrayLength(schema_arr);
    jbyte *schema_bytes = env->GetByteArrayElements(schema_arr, 0);
    std::shared_ptr<arrow::Schema> schema = ReadSchemaFromProtobufBytes(schema_bytes, schema_len);

    return (jlong) new CopyProcessor(schema);
}