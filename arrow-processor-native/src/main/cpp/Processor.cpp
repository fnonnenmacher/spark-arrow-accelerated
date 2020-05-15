//
// Created by Fabian Nonnenmacher on 15.05.20.
//

#include "Processor.h"
#include "jni/Assertions.h"
#include "jni/Converters.h"
#include "nl_tudelft_ewi_abs_nonnenmacher_JNIProcessor.h"


JNIEXPORT void JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_JNIProcessor_process
(JNIEnv * env, jobject, jlong process_ptr, jint num_rows, jlongArray in_buf_addrs, jlongArray in_buf_sizes, jobject jexpander, jlongArray out_buf_addrs, jlongArray out_buf_sizes){

    Processor* jni_processor = (Processor*) process_ptr;

    // Extract input RecordBatch
    int in_buf_len = env->GetArrayLength(in_buf_addrs);
    ASSERT(in_buf_len == env->GetArrayLength(in_buf_sizes), "mismatch in arraylen of buf_addrs and buf_sizes");

    jlong *in_addrs = env->GetLongArrayElements(in_buf_addrs, 0);
    jlong *in_sizes = env->GetLongArrayElements(in_buf_sizes, 0);

    std::shared_ptr<arrow::RecordBatch> in_batch;
    ASSERT_OK(make_record_batch_with_buf_addrs(jni_processor->schema, num_rows, in_addrs, in_sizes, in_buf_len, &in_batch));

    // Call processing step
    std::shared_ptr<arrow::RecordBatch> out_batch = jni_processor->process(in_batch);

    // Extract buffers of output RecordBatch
    int out_buf_len = env->GetArrayLength(out_buf_addrs);
    ASSERT(out_buf_len == env->GetArrayLength(out_buf_sizes), "mismatch in arraylen of buf_addrs and buf_sizes");

    jlong *out_addrs = env->GetLongArrayElements(out_buf_addrs, 0);
    jlong *out_sizes = env->GetLongArrayElements(out_buf_sizes, 0);

    ASSERT_OK(copy_record_batch_ito_buffers(env, jexpander, out_batch, out_addrs, out_sizes, out_buf_len));
}

/*
 * Class:     nl_tudelft_ewi_abs_nonnenmacher_JNIProcessor
 * Method:    close
 * Signature: (J)Ljava/lang/Void;
 */
JNIEXPORT void JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_JNIProcessor_close
        (JNIEnv *env, jobject, jlong process_ptr){
    delete (Processor*) process_ptr;
}
