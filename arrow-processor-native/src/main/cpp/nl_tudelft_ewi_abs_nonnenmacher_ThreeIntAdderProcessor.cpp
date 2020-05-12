
#include "nl_tudelft_ewi_abs_nonnenmacher_ThreeIntAdderProcessor.h"
#include "nl_tudelft_ewi_abs_nonnenmacher_ThreeIntAdderProcessor_Initializer.h"
#include "ThreeIntAdderProcessor.h"
#include "PlasmaProcessor.h"
#include "SerializeBatchProcessor.h"
#include "jni/Converters.h"
#include <plasma/client.h>

#include "jni/Assertions.h"
#include "jni/ProtoTypeDeserializer.h"

JNIEXPORT jlong JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_ThreeIntAdderProcessor_00024Initializer_init
        (JNIEnv *, jobject) {
    return (jlong) new ThreeIntAdderProcessor();
}

JNIEXPORT jbyteArray JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_ThreeIntAdderProcessor_process
        (JNIEnv *env, jobject jobj, jlong proc_ptr, jbyteArray object_id_java) {

    auto proc = (ThreeIntAdderProcessor *) proc_ptr;

    shared_ptr<ObjectID> object_id = std::make_shared<ObjectID>(object_id_from_java(env, object_id_java));

    shared_ptr<ObjectID> object_id_out = proc->process(object_id);

    return object_id_to_java_(env, *object_id_out);
}

JNIEXPORT void JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_ThreeIntAdderProcessor_close
        (JNIEnv *env, jobject jobj, jlong proc_ptr) {
    delete ((ThreeIntAdderProcessor *) proc_ptr);
}


shared_ptr<arrow::RecordBatch> process(const shared_ptr<arrow::RecordBatch> &record_batch);

JNIEXPORT jbyteArray JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_ThreeIntAdderProcessor_process__J_3BI_3J_3J
        (JNIEnv *env, jobject jobj, jlong proc_ptr, jbyteArray schema_arr, jint num_rows, jlongArray buf_addrs,
         jlongArray buf_sizes) {

//    auto proc = (ThreeIntAdderProcessor *) proc_ptr;

    jclass localExceptionClass =
            env->FindClass("org/apache/arrow/gandiva/exceptions/GandivaException");
    jclass gandiva_exception_ = (jclass) env->NewGlobalRef(localExceptionClass);

    jsize schema_len = env->GetArrayLength(schema_arr);
    jbyte *schema_bytes = env->GetByteArrayElements(schema_arr, 0);

    std::shared_ptr<arrow::Schema> schema = ReadSchemaFromProtobufBytes(schema_bytes, schema_len);


    int in_bufs_len = env->GetArrayLength(buf_addrs);
    if (in_bufs_len != env->GetArrayLength(buf_sizes)) {
        env->ThrowNew(gandiva_exception_, "mismatch in arraylen of buf_addrs and buf_sizes");
        return nullptr;
    }

    jlong *in_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
    jlong *in_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);


    std::shared_ptr<arrow::RecordBatch> in_batch;
    ASSERT_OK(make_record_batch_with_buf_addrs(schema, num_rows, in_buf_addrs, in_buf_sizes, in_bufs_len, &in_batch));

    auto serialize_processor = std::make_unique<SerializeBatchProcessor>();
    auto plasma_writer_processor = new WriteToPlasmaProcessor;

    auto out_batch = process(in_batch);

    auto res = serialize_processor->process(out_batch);
    shared_ptr<ObjectID> object_id_out = plasma_writer_processor->process(res);

    return object_id_to_java_(env, *object_id_out);
}

shared_ptr<arrow::RecordBatch> process(const shared_ptr<arrow::RecordBatch> &record_batch) {
    // get individual input field vectors
    auto vector_in1 = std::static_pointer_cast<arrow::Int32Array>(record_batch->column(0));
    auto vector_in2 = std::static_pointer_cast<arrow::Int32Array>(record_batch->column(1));
    auto vector_in3 = std::static_pointer_cast<arrow::Int32Array>(record_batch->column(2));

    // initialize builder for integer field vector
    arrow::MemoryPool *pool = arrow::default_memory_pool();
    arrow::Int32Builder res_builder(pool);

    //iterate over rows and bild sum for every row
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