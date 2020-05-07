//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#include "SerializeBatchProcessor.h"
#include "my_assert.h"

#include <utility>
#include <arrow/ipc/api.h>
#include <arrow/io/memory.h>


SerializeBatchProcessor::SerializeBatchProcessor() {
    buffer_output_stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
}

std::shared_ptr<arrow::Buffer> SerializeBatchProcessor::process(std::shared_ptr<arrow::RecordBatch> batch) {
    if (rb_writer == nullptr){//lazy initialization to have schema availabe
        rb_writer = arrow::ipc::NewStreamWriter(buffer_output_stream.get(),batch->schema()).ValueOrDie();
    }
    ASSERT_OK(buffer_output_stream->Reset());

    ASSERT_OK(rb_writer->WriteRecordBatch(*batch));
//    ASSERT_OK(rb_writer->Close());
    return buffer_output_stream->Finish().ValueOrDie();
}