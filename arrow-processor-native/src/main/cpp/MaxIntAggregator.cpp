#include "nl_tudelft_ewi_abs_nonnenmacher_MaxIntAggregator.h"

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "jni/Assertions.h"
#include "jni/Converters.h"
#include <iostream>

JNIEXPORT jint JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_MaxIntAggregator_agg
        (JNIEnv *env, jobject, jint num_rows, jlongArray in_buf_addrs, jlongArray in_buf_sizes) {

    // Extract input RecordBatch
    int in_buf_len = env->GetArrayLength(in_buf_addrs);
    ASSERT(in_buf_len == env->GetArrayLength(in_buf_sizes), "mismatch in arraylen of buf_addrs and buf_sizes");

    jlong *in_addrs = env->GetLongArrayElements(in_buf_addrs, 0);
    jlong *in_sizes = env->GetLongArrayElements(in_buf_sizes, 0);

    //TODO: HARD coded schema
    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
            arrow::field("out", arrow::int32())};
    auto schema = std::make_shared<arrow::Schema>(schema_vector);

    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_OK(make_record_batch_with_buf_addrs(schema, num_rows, in_addrs, in_sizes, in_buf_len, &batch));

    auto values = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));

    int current_max = INT32_MIN;
    const int *raw_values = values->raw_values();
    for (int i = 0; i< num_rows; i++) {
        if (raw_values[i] > current_max) {
            current_max = raw_values[i];
        }
    }
    return (jint) current_max;
}
