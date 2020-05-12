//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#include "SerializeBatchProcessor.h"
#include "jni/Assertions.h"

#include <utility>
#include <arrow/ipc/api.h>
#include <arrow/io/memory.h>


SerializeBatchProcessor::SerializeBatchProcessor() {
    buffer_output_stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
}

std::shared_ptr<arrow::Buffer> SerializeBatchProcessor::process(std::shared_ptr<arrow::RecordBatch> batch) {
    if (rb_writer == nullptr){//late initialization to have schema availabe
        rb_writer = arrow::ipc::NewStreamWriter(buffer_output_stream.get(),batch->schema()).ValueOrDie();
    }

    ASSERT_OK(buffer_output_stream->Reset());
    ASSERT_OK(rb_writer->WriteRecordBatch(*batch));
    return buffer_output_stream->Finish().ValueOrDie();
}

DeserializeBatchProcessor::DeserializeBatchProcessor() = default;

std::shared_ptr<arrow::RecordBatch> DeserializeBatchProcessor::process(std::shared_ptr<arrow::Buffer> buffer) {
    auto buf_reader = std::make_shared<arrow::io::BufferReader>(buffer);
    std::shared_ptr<arrow::ipc::RecordBatchReader> reader= arrow::ipc::RecordBatchStreamReader::Open(buf_reader).ValueOrDie();

    std::shared_ptr<arrow::RecordBatch> record_batch;
    ASSERT_OK(reader->ReadNext(&record_batch));
    return record_batch;
}
