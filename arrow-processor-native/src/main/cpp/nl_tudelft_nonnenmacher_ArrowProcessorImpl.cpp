#include "nl_tudelft_ewi_abs_nonnenmacher_ArrowProcessorJni.h"

#include <iostream>
#include <string>
#include <plasma/client.h>
#include <arrow/ipc/api.h>
#include <arrow/io/memory.h>
#include <arrow/api.h>

using namespace plasma;

using BatchVector = std::vector<std::shared_ptr<arrow::RecordBatch>>;
using arrow::Int64Builder;

Status ReadBatches(std::shared_ptr<Buffer> buffer_, std::shared_ptr<arrow::RecordBatch> *out_batch);

ObjectID objectIdFromJavaArray(JNIEnv *env, _jbyteArray *object_id_java_array);

void readFromPlasma(std::shared_ptr<ObjectID> object_id, ObjectBuffer* object_buffers);

JNIEXPORT jlong JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_ArrowProcessorJni_sum
        (JNIEnv *env, jobject obj, jbyteArray object_id_java_array) {

    ObjectID object_id = objectIdFromJavaArray(env, object_id_java_array);
    std::cout << "object_id is " << object_id.hex() << std::endl;

    PlasmaClient client;
    arrow::Status r1 = client.Connect("/tmp/plasma");
    std::cout << "plasma client connect: " << r1.ok() << std::endl;

    ObjectBuffer object_buffer;
    arrow::Status r2 = client.Get(&object_id, 1, -1, &object_buffer);
    std::cout << "plasma client get: " << r2.ok() << std::endl;

    // Retrieve object data.
    auto buffer = object_buffer.data;
//    const uint8_t *data = buffer->data();
//    int64_t data_size = buffer->size();
//
//    std::cout << "data_size: " << data_size << std::endl;
//    std::cout << "object_buffer.metadata->size()" << object_buffer.metadata-> size() << std::endl;
//    std::cout << "data[0]: " << (int) data[0] << std::endl;

    std::shared_ptr<arrow::RecordBatch> record_batch;
    ReadBatches(buffer, &record_batch);

    auto values = std::static_pointer_cast<arrow::Int64Array>(record_batch->column(0));

    long sum = 0;
    for (int i=0; i<record_batch->num_rows(); i++){
        sum += values->raw_values()[i];
    }
    return (jlong) sum;
}

Status ReadBatches(std::shared_ptr<Buffer> buffer_, std::shared_ptr<arrow::RecordBatch> *out_batch) {
    auto buf_reader = std::make_shared<arrow::io::BufferReader>(buffer_);
    std::shared_ptr<arrow::ipc::RecordBatchReader> reader;
    Status status = arrow::ipc::RecordBatchStreamReader::Open(buf_reader, &reader);
    std::cout << "Status:" << (int) status.code() << std::endl;
    std::cout << "Status:" << status.message() << std::endl;
    return reader->ReadNext(out_batch);
}

ObjectID objectIdFromJavaArray(JNIEnv *env, _jbyteArray *java_array) {
    char *object_id_bin = (char *) env->GetByteArrayElements(java_array, NULL);
    ObjectID object_id = ObjectID::from_binary(object_id_bin);
    return object_id;
}