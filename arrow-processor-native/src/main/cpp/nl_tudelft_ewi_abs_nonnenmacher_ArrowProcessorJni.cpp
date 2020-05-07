#include "nl_tudelft_ewi_abs_nonnenmacher_ArrowProcessorJni.h"

#include "ParqueteReaderIterator.h"
#include "SerializeBatchProcessor.h"
#include "PlasmaProcessor.h"
#include "ParqueteToPlasmaReader.h"

#include <iostream>
#include <fstream>
#include <string>
#include <plasma/client.h>
#include <arrow/ipc/api.h>
#include <arrow/io/memory.h>
#include <arrow/api.h>
#include <fletcher/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

using namespace plasma;

using BatchVector = std::vector<std::shared_ptr<arrow::RecordBatch>>;
using arrow::Int32Builder;

Status ReadBatches(std::shared_ptr<Buffer> buffer_, std::shared_ptr<arrow::RecordBatch> *out_batch);

ObjectID objectIdFromJavaArray(JNIEnv *env, _jbyteArray *object_id_java_array);

//void readFromPlasma(std::shared_ptr<ObjectID> object_id, ObjectBuffer* object_buffers);

int createFletcherPlatformAndContext(std::shared_ptr<fletcher::Platform> *platform_out,
                                     std::shared_ptr<fletcher::Context> *context_out);

int
processRecordBatchOnFletcher(std::shared_ptr<fletcher::Platform> platform, std::shared_ptr<fletcher::Context> context,
                             const std::shared_ptr<arrow::RecordBatch> &record_batch, uint32_t *ret0, uint32_t *ret1);

jbyteArray to_java_id(JNIEnv *env, const ObjectID &object_id_out);

JNIEXPORT jbyteArray JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_ArrowProcessorJni_addingThreeValues
        (JNIEnv *env, jobject obj, jbyteArray object_id_java_array_in, jbyteArray object_id_java_array_out) {

    // convert java array to object id
    ObjectID object_id = objectIdFromJavaArray(env, object_id_java_array_in);
    ObjectID object_id_out = objectIdFromJavaArray(env, object_id_java_array_out);

    std::cout << "ObjectId (Input)  = " << object_id.hex() << std::endl;
    std::cout << "ObjectId (Output) = " << object_id_out.hex() << std::endl;

    // intitialize Plasma client
    PlasmaClient client;
    arrow::Status r1 = client.Connect("/tmp/plasma");
    std::cout << "Plasma client connected: " << r1.ok() << std::endl;

    // Read input arrow byte data from plasma
    ObjectBuffer object_buffer;
    client.Get(&object_id, 1, -1, &object_buffer);
    std::cout << "Plasma client read " << object_buffer.data->size() << " bytes." << std::endl;

    // Intepret bytes as RecordBuffer
    auto buffer = object_buffer.data;
    std::shared_ptr<arrow::RecordBatch> record_batch;
    ReadBatches(buffer, &record_batch);
    std::cout << "Read bytes are a valid recordbuffer" << std::endl;

    // get individual input field vectors
    auto vector_in1 = std::static_pointer_cast<arrow::Int32Array>(record_batch->column(0));
    auto vector_in2 = std::static_pointer_cast<arrow::Int32Array>(record_batch->column(1));
    auto vector_in3 = std::static_pointer_cast<arrow::Int32Array>(record_batch->column(2));

    // intitialize builder for integer field vector
    arrow::MemoryPool *pool = arrow::default_memory_pool();
    Int32Builder res_builder(pool);

    //iterate over rows and bild sum for every row
    for (int i = 0; i < record_batch->num_rows(); i++) {

        int v1 = vector_in1->raw_values()[i];
        int v2 = vector_in2->raw_values()[i];
        int v3 = vector_in3->raw_values()[i];
        int sum = v1 + v2 + v3;
        std::cout << "Calculate row: " << v1 << " + " << v2 << " + " << v3 << " = " << sum << std::endl;
        res_builder.Append(sum);
    }

    // get field vector from builder
    std::shared_ptr<arrow::Array> res_array;
    res_builder.Finish(&res_array);

    // define output schema
    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
            arrow::field("out", arrow::int32())};
    auto result_schema = std::make_shared<arrow::Schema>(schema_vector);

    // create ecord batch containing the created int vector
    std::vector<std::shared_ptr<arrow::Array>> vec;
    vec.push_back(res_array);
    std::shared_ptr<arrow::RecordBatch> res_record_batch = arrow::RecordBatch::Make(result_schema,
                                                                                    record_batch->num_rows(), vec);

    // DEBUG
    // Print data written to record batch to make sure it's correct
    // auto values = std::static_pointer_cast<arrow::Int32Array>(res_record_batch->column(0));
    // for (int i=0; i<res_record_batch->num_rows(); i++){
    //     std::cout << values->raw_values()[i] << std::endl;
    // }


    // DEBUG alternative:
    // write to file instead of byte array to validate consistency
    //    auto out_file = arrow::io::FileOutputStream::Open("res.rb", false).ValueOrDie();
    //    auto writer = arrow::ipc::RecordBatchFileWriter::Open(out_file.get(), result_schema).ValueOrDie();


    // Write record batch to byte array buffer
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto writer = arrow::ipc::RecordBatchStreamWriter::Open(sink.get(), res_record_batch->schema()).ValueOrDie();
    writer->WriteRecordBatch(*res_record_batch);
    writer->Close();
    std::shared_ptr<Buffer> buffer1 = sink->Finish().ValueOrDie();

    // TODO: nicer delete
    // make sure no other result object exists in Plasma store
    client.Delete(object_id_out);

    // Write bytes of result record batch to plasma store
    std::shared_ptr<Buffer> data_buffer;
    arrow::Status s1 = client.Create(object_id_out, buffer1->size(), NULL, 0, &data_buffer);

    // TODO: looks inefficent copying the previously generated byta array to the store
    for (size_t i = 0; i < buffer1->size(); i++) {
        data_buffer->mutable_data()[i] = buffer1->data()[i];
    }

    std::cout << "Plasma client created result object: " << s1.ok() << std::endl;
    arrow::Status s3 = client.Seal(object_id_out);
    std::cout << "Plasma client wrote result (" << buffer->size() << " bytes) to the plasma store" << std::endl;
}

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

    // Retrieve RecordBuffer
    auto buffer = object_buffer.data;
    std::shared_ptr<arrow::RecordBatch> record_batch;
    ReadBatches(buffer, &record_batch);

    //create Fletcher Platform and context
    std::shared_ptr<fletcher::Platform> platform;
    std::shared_ptr<fletcher::Context> context;
    createFletcherPlatformAndContext(&platform, &context);

    uint32_t return_value_0;
    uint32_t return_value_1;

    processRecordBatchOnFletcher(platform, context, record_batch, &return_value_0, &return_value_1);
    std::cout << "Potential Fletcher Result: " << return_value_0 << std::endl;

    // REPLACE this later when real fletcher platform in place
    auto values = std::static_pointer_cast<arrow::Int64Array>(record_batch->column(0));

    long sum = 0;
    for (int i = 0; i < record_batch->num_rows(); i++) {
        sum += values->raw_values()[i];
    }
    return (jlong) sum;
}

Status ReadBatches(std::shared_ptr<Buffer> buffer_, std::shared_ptr<arrow::RecordBatch> *out_batch) {
    auto buf_reader = std::make_shared<arrow::io::BufferReader>(buffer_);
    std::shared_ptr<arrow::ipc::RecordBatchReader> reader;
    Status status = arrow::ipc::RecordBatchStreamReader::Open(buf_reader, &reader);
//  std::cout << "Status:" << (int) status.code() << std::endl;
//  std::cout << "Status:" << status.message() << std::endl;
    return reader->ReadNext(out_batch);
}

ObjectID objectIdFromJavaArray(JNIEnv *env, _jbyteArray *java_array) {
    char *object_id_bin = (char *) env->GetByteArrayElements(java_array, NULL);
    ObjectID object_id = ObjectID::from_binary(object_id_bin);
    return object_id;
}

int createFletcherPlatformAndContext(std::shared_ptr<fletcher::Platform> *platform_out,
                                     std::shared_ptr<fletcher::Context> *context_out) {
    fletcher::Status status;

    // Create a Fletcher platform object, attempting to autodetect the platform.
    status = fletcher::Platform::Make(platform_out, false);

    if (!status.ok()) {
        std::cerr << "Could not create Fletcher platform!" << std::endl;
        return -1;
    }

    // Initialize the platform.
    status = (*platform_out)->Init();

    if (!status.ok()) {
        std::cerr << "Could not create Fletcher platform." << std::endl;
        return -1;
    }

    // Create a context for our application on the platform.
    status = fletcher::Context::Make(context_out, (*platform_out));

    if (!status.ok()) {
        std::cerr << "Could not create Fletcher context." << std::endl;
        return -1;
    }

    return 0;
}

int
processRecordBatchOnFletcher(std::shared_ptr<fletcher::Platform> platform, std::shared_ptr<fletcher::Context> context,
                             const std::shared_ptr<arrow::RecordBatch> &record_batch, uint32_t *ret0, uint32_t *ret1) {
    fletcher::Status status;
    // Queue the recordbatch to our context.
    status = context->QueueRecordBatch(record_batch);

    if (!status.ok()) {
        std::cerr << "Could not queue the RecordBatch to the context." << std::endl;
        return -1;
    }

    // "Enable" the context, potentially copying the recordbatch to the device. This depends on your platform.
    // AWS EC2 F1 requires a copy, but OpenPOWER SNAP doesn't.
    context->Enable();

    if (!status.ok()) {
        std::cerr << "Could not enable the context." << std::endl;
        return -1;
    }

    // Create a kernel based on the context.
    fletcher::Kernel kernel(context);

    // Start the kernel.
    status = kernel.Start();

    if (!status.ok()) {
        std::cerr << "Could not start the kernel." << std::endl;
        return -1;
    }

    // Wait for the kernel to finish.
    status = kernel.WaitForFinish();

    if (!status.ok()) {
        std::cerr << "Something went wrong waiting for the kernel to finish." << std::endl;
        return -1;
    }

    // Obtain the return value.
    status = kernel.GetReturn(ret0, ret1);

    if (!status.ok()) {
        std::cerr << "Could not obtain the return value." << std::endl;
        return -1;
    }
}


JNIEXPORT jlong JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_ArrowProcessorJni_readParquete(JNIEnv *env,
                                                                                            jobject,
                                                                                            jstring java_file_name) {
    const char *file_path = env->GetStringUTFChars(java_file_name, 0);
    return (jlong) new ParqueteToPlasmaReader(file_path);
}