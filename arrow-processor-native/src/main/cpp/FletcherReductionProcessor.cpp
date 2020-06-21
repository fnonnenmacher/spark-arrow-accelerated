//
// Created by Fabian Nonnenmacher on 18.06.20.
//

#include "FletcherReductionProcessor.h"
#include "jni/Assertions.h"
#include "jni/Converters.h"
#include "jni/ProtobufSchemaDeserializer.h"

#include <utility>

FletcherReductionProcessor::FletcherReductionProcessor(std::shared_ptr<arrow::Schema> input_schema) {
    schema = std::move(input_schema);

    // Create a Fletcher platform object, attempting to autodetect the platform.
    ASSERT_FLETCHER_OK(fletcher::Platform::Make(&platform, false));

    // Initialize the platform.
    ASSERT_FLETCHER_OK(platform->Init());
}

long FletcherReductionProcessor::reduce(const std::shared_ptr<arrow::RecordBatch> &record_batch) {

    // Create a context for our application on the platform.
    std::shared_ptr<fletcher::Context> context;
    ASSERT_FLETCHER_OK(fletcher::Context::Make(&context, platform));

    // Queue the recordbatch to our context.
    ASSERT_FLETCHER_OK(context->QueueRecordBatch(record_batch));

    // "Enable" the context, potentially copying the recordbatch to the device. This depends on your platform.
    // AWS EC2 F1 requires a copy, but OpenPOWER SNAP doesn't.
    ASSERT_FLETCHER_OK(context->Enable());

    // Create a kernel based on the context.
    fletcher::Kernel kernel(context);

    // Reset the kernel.
    ASSERT_FLETCHER_OK(kernel.Reset());

    // Start the kernel.
    ASSERT_FLETCHER_OK(kernel.Start());

    // Wait for the kernel to finish.
    ASSERT_FLETCHER_OK(kernel.WaitForFinish());

    uint32_t return_value_0;
    uint32_t return_value_1;

    // Obtain the return value.
    ASSERT_FLETCHER_OK(kernel.GetReturn(&return_value_0, &return_value_1));

    //TODO: interpret return value correctly
    std::cout << "RESULT returned from Fletcher: " << *reinterpret_cast<int32_t *>(&return_value_0) << std::endl;

    return 13L;
}

JNIEXPORT jlong JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_FletcherReductionProcessor_initFletcherReductionProcessor
        (JNIEnv *env, jobject, jbyteArray schema_arr) {

    jsize schema_len = env->GetArrayLength(schema_arr);
    jbyte *schema_bytes = env->GetByteArrayElements(schema_arr, 0);

    std::shared_ptr<arrow::Schema> schema = ReadSchemaFromProtobufBytes(schema_bytes, schema_len);

    return (jlong) new FletcherReductionProcessor(schema);
}

/*
 * Class:     nl_tudelft_ewi_abs_nonnenmacher_FletcherReductionProcessor
 * Method:    reduce
 * Signature: (JI[J[J)J
 */
JNIEXPORT jlong JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_FletcherReductionProcessor_reduce
        (JNIEnv *env, jobject, jlong process_ptr, jint num_rows, jlongArray in_buf_addrs, jlongArray in_buf_sizes) {

    FletcherReductionProcessor *processor = (FletcherReductionProcessor *) process_ptr;

    // Extract input RecordBatch
    int in_buf_len = env->GetArrayLength(in_buf_addrs);
    ASSERT(in_buf_len == env->GetArrayLength(in_buf_sizes), "mismatch in arraylen of buf_addrs and buf_sizes");

    jlong *in_addrs = env->GetLongArrayElements(in_buf_addrs, 0);
    jlong *in_sizes = env->GetLongArrayElements(in_buf_sizes, 0);

    std::shared_ptr<arrow::RecordBatch> in_batch;
    ASSERT_OK(make_record_batch_with_buf_addrs(processor->schema, num_rows, in_addrs, in_sizes, in_buf_len, &in_batch));

    return (jlong) processor->reduce(in_batch);
}

JNIEXPORT void JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_FletcherReductionProcessor_close
        (JNIEnv *, jobject, jlong process_ptr) {
    delete (FletcherReductionProcessor *) process_ptr;
}