//
// Created by Fabian Nonnenmacher on 18.06.20.
//

#include "FletcherReductionProcessor.h"
#include "jni/Assertions.h"
#include "jni/Converters.h"
#include "jni/ProtobufSchemaDeserializer.h"

#include <utility>
#include <unistd.h>

FletcherReductionProcessor::FletcherReductionProcessor(std::shared_ptr<arrow::Schema> input_schema) {
    schema = std::move(input_schema);

    // Create a Fletcher platform object, attempting to autodetect the platform.
    ASSERT_FLETCHER_OK(fletcher::Platform::Make(&platform, false));

    // Initialize the platform.
    ASSERT_FLETCHER_OK(platform->Init());

    //assert Schema is OK
    //TODO: 1 column string, the other long, both not null
//    std::shared_ptr<arrow::Field> string_field = schema->field(0);
//    if (string_field->type()->Equals(arrow::StringType()) && !string_field->nullable())

}

long trivialCpuVersion(const std::shared_ptr<arrow::RecordBatch> &record_batch) {

    auto strings = std::static_pointer_cast<arrow::StringArray>(record_batch->column(0));
    auto numbers = std::static_pointer_cast<arrow::Int64Array>(record_batch->column(1));

    const int64_t* raw_numbers = numbers->raw_values();

    int64_t sum = 0;
    for (int i = 0; i < record_batch->num_rows(); i++) {

        if (strings->GetString(i) == "Blue Ribbon Taxi Association Inc.") {
            sum += raw_numbers[i];
        }
    }
    return sum;
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

    long result = *reinterpret_cast<int64_t *>(&return_value_0);

    std::cout << "RESULT returned from Fletcher: " << *reinterpret_cast<int64_t *>(&return_value_0) << std::endl;

    //The echo platform does not return a proper value -> fallback to cpu impl
    if (platform->name() == "echo") {
        return trivialCpuVersion(record_batch);
    }
    return result;
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