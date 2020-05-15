//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#include "ThreeIntAdderProcessor.h"
#include "nl_tudelft_ewi_abs_nonnenmacher_JNIProcessorFactory_Initializer.h"
#include "jni/Assertions.h"
#include "jni/ProtobufSchemaDeserializer.h"


using arrow::Int32Builder;

std::shared_ptr<arrow::RecordBatch> ThreeIntAdderProcessor::process(std::shared_ptr<arrow::RecordBatch> record_batch) {

    // get individual input field vectors
    auto vector_in1 = std::static_pointer_cast<arrow::Int32Array>(record_batch->column(0));
    auto vector_in2 = std::static_pointer_cast<arrow::Int32Array>(record_batch->column(1));
    auto vector_in3 = std::static_pointer_cast<arrow::Int32Array>(record_batch->column(2));

    // initialize builder for integer field vector
    arrow::MemoryPool *pool = arrow::default_memory_pool();
    Int32Builder res_builder(pool);

    // iterate over rows and calculate sum for every row
    for (int i = 0; i < record_batch->num_rows(); i++) {

        int v1 = vector_in1->raw_values()[i];
        int v2 = vector_in2->raw_values()[i];
        int v3 = vector_in3->raw_values()[i];
        int sum = v1 + v2 + v3;
        std::cout << "Calculate row: " << v1 << " + " << v2 << " + " << v3 << " = " << sum << std::endl;
        ASSERT_OK(res_builder.Append(sum));
    }

    // get field vector from builder
    std::shared_ptr<arrow::Array> res_array;
    ASSERT_OK(res_builder.Finish(&res_array));

    // define output schema
    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
            arrow::field("out", arrow::int32())};
    auto result_schema = std::make_shared<arrow::Schema>(schema_vector);

    // create record batch containing the created int vector
    std::vector<std::shared_ptr<arrow::Array>> vec;
    vec.push_back(res_array);
    return arrow::RecordBatch::Make(result_schema, record_batch->num_rows(), vec);
}

JNIEXPORT jlong JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_JNIProcessorFactory_00024Initializer_initThreeIntAddingProcessor
        (JNIEnv *env, jobject, jbyteArray schema_arr) {

    jsize schema_len = env->GetArrayLength(schema_arr);
    jbyte *schema_bytes = env->GetByteArrayElements(schema_arr, 0);
    std::shared_ptr<arrow::Schema> schema = ReadSchemaFromProtobufBytes(schema_bytes, schema_len);

    return (jlong) new ThreeIntAdderProcessor(schema);
}