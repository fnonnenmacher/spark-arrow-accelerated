//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#ifndef SPARK_EXAMPLE_SERIALIZEBATCHPROCESSOR_H
#define SPARK_EXAMPLE_SERIALIZEBATCHPROCESSOR_H


#include "Iterator.h"
#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>

class SerializeBatchProcessor : public BatchProcessor<std::shared_ptr<arrow::RecordBatch>, std::shared_ptr<arrow::Buffer>> {
private:
    std::shared_ptr<arrow::io::BufferOutputStream> buffer_output_stream;
    std::shared_ptr<arrow::ipc::RecordBatchWriter> rb_writer = nullptr;

public:
    std::shared_ptr<arrow::Buffer> process(std::shared_ptr<arrow::RecordBatch>) override;
    explicit SerializeBatchProcessor();
    ~SerializeBatchProcessor() override = default;
};

class DeserializeBatchProcessor : public BatchProcessor<std::shared_ptr<arrow::Buffer>, std::shared_ptr<arrow::RecordBatch>> {
public:
    std::shared_ptr<arrow::RecordBatch> process(std::shared_ptr<arrow::Buffer>) override;
    explicit DeserializeBatchProcessor();
    ~DeserializeBatchProcessor() override = default;
};

#endif //SPARK_EXAMPLE_SERIALIZEBATCHPROCESSOR_H
