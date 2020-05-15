//
// Created by Fabian Nonnenmacher on 15.05.20.
//

#include "FletcherEchoProcessor.h"
#include "nl_tudelft_ewi_abs_nonnenmacher_JNIProcessorFactory_Initializer.h"
#include "jni/ProtobufSchemaDeserializer.h"
#include "jni/Assertions.h"

JNIEXPORT jlong JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_JNIProcessorFactory_00024Initializer_initFletcherProcessor
        (JNIEnv * env, jobject, jbyteArray schema_arr){

    jsize schema_len = env->GetArrayLength(schema_arr);
    jbyte *schema_bytes = env->GetByteArrayElements(schema_arr, 0);
    std::shared_ptr<arrow::Schema> schema = ReadSchemaFromProtobufBytes(schema_bytes, schema_len);

    return (jlong) new FletcherEchoProcessor(schema);
}

FletcherEchoProcessor::FletcherEchoProcessor(std::shared_ptr<arrow::Schema> schema_) : Processor(std::move(schema_)) {

//    // Create a Fletcher platform object, attempting to autodetect the platform.
//    ASSERT_FLETCHER_OK(fletcher::Platform::Make(&platform, false));
//
//    // Initialize the platform.
//    ASSERT_FLETCHER_OK(platform->Init());
//
//    // Create a context for our application on the platform.
//    ASSERT_FLETCHER_OK(fletcher::Context::Make(&context, platform));
//
    // define output schema
    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
            arrow::field("out", arrow::int64())};
    schema_out = std::make_shared<arrow::Schema>(schema_vector);
}

std::shared_ptr<arrow::RecordBatch> FletcherEchoProcessor::process(std::shared_ptr<arrow::RecordBatch> record_batch) {

//    // Queue the recordbatch to our context.
//    ASSERT_FLETCHER_OK(context->QueueRecordBatch(record_batch));
//
//    // "Enable" the context, potentially copying the recordbatch to the device. This depends on your platform.
//    // AWS EC2 F1 requires a copy, but OpenPOWER SNAP doesn't.
//    ASSERT_FLETCHER_OK(context->Enable());
//
//    // Create a kernel based on the context.
//    fletcher::Kernel kernel(context);
//
//    // Start the kernel.
//    ASSERT_FLETCHER_OK(kernel.Start());
//
//
//    // Wait for the kernel to finish.
//    ASSERT_FLETCHER_OK(kernel.WaitForFinish());
//
//    uint32_t return_value_0;
//    uint32_t return_value_1;
//    // Obtain the return value.
//    ASSERT_FLETCHER_OK(kernel.GetReturn(&return_value_0, &return_value_1));

    //////////////////
    // Sum calculation in C++

    auto values = std::static_pointer_cast<arrow::Int64Array>(record_batch->column(0));

    long sum = 0;
    for (int i = 0; i < record_batch->num_rows(); i++) {
        sum += values->raw_values()[i];
    }

    // initialize builder for integer field vector
    arrow::Int64Builder res_builder(arrow::default_memory_pool());

    ASSERT_OK(res_builder.Append(sum));

    // get field vector from builder
    std::shared_ptr<arrow::Array> res_array;
    ASSERT_OK(res_builder.Finish(&res_array));

    return arrow::RecordBatch::Make(schema_out, 1, {res_array});
}