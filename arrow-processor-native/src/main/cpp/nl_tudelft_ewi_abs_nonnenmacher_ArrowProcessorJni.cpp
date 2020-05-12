#include "nl_tudelft_ewi_abs_nonnenmacher_ArrowProcessorJni.h"

#include "jni/Converters.h"

#include <iostream>

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

int createFletcherPlatformAndContext(std::shared_ptr<fletcher::Platform> *platform_out,
                                     std::shared_ptr<fletcher::Context> *context_out);

int
processRecordBatchOnFletcher(std::shared_ptr<fletcher::Platform> platform, std::shared_ptr<fletcher::Context> context,
                             const std::shared_ptr<arrow::RecordBatch> &record_batch, uint32_t *ret0, uint32_t *ret1);


JNIEXPORT jlong JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_ArrowProcessorJni_sum
        (JNIEnv *env, jobject obj, jbyteArray object_id_java_array) {

    ObjectID object_id = object_id_from_java(env, object_id_java_array);
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