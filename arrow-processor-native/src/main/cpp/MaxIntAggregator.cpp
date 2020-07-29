#include "nl_tudelft_ewi_abs_nonnenmacher_MaxIntAggregator.h"

#include "arrow/api.h"
#include "arrow/io/api.h"
#include "jni/Assertions.h"
#include "jni/Converters.h"
#include <iostream>

JNIEXPORT void JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_MaxIntAggregator_agg
        (JNIEnv *env, jobject, jint num_rows, jlongArray in_buf_addrs, jlongArray in_buf_sizes, jintArray results) {

    // Extract input RecordBatch
    int in_buf_len = env->GetArrayLength(in_buf_addrs);
    ASSERT(in_buf_len == env->GetArrayLength(in_buf_sizes), "mismatch in arraylen of buf_addrs and buf_sizes");

    jlong *in_addrs = env->GetLongArrayElements(in_buf_addrs, 0);
    jlong *in_sizes = env->GetLongArrayElements(in_buf_sizes, 0);

    //TODO: schema is currently hardcoded based on the number of expected results
    //      => Better pass during initialization
    int num_cols = env->GetArrayLength(results);
    std::vector<std::shared_ptr<arrow::Field>> schema_vector(num_cols);

    std::generate(schema_vector.begin(), schema_vector.end(), []{return arrow::field("out", arrow::int32(), false);});
    auto schema = std::make_shared<arrow::Schema>(schema_vector);

    std::shared_ptr<arrow::RecordBatch> batch;
    ASSERT_OK(make_record_batch_with_buf_addrs(schema, num_rows, in_addrs, in_sizes, in_buf_len, &batch));

    for (int col = 0; col < batch->num_columns(); col++) {
        auto values = std::static_pointer_cast<arrow::Int32Array>(batch->column(col));

        int current_max = INT32_MIN;
        const int *raw_values = values->raw_values();
        for (int i = 0; i < num_rows; i++) {
            if (raw_values[i] > current_max) {
                current_max = raw_values[i];
            }
        }
        env->SetIntArrayRegion(results, col, 1, (int *) &current_max);
    }
}
