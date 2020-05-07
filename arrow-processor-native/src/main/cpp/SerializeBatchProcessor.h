//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#ifndef SPARK_EXAMPLE_SERIALIZEBATCHPROCESSOR_H
#define SPARK_EXAMPLE_SERIALIZEBATCHPROCESSOR_H


#include "Iterator.h"
#include <arrow/api.h>

class SerializeBatchProcessor : public BatchProcessor<std::shared_ptr<arrow::RecordBatch>, std::shared_ptr<arrow::Buffer>> {
public:
    std::shared_ptr<arrow::Buffer> process(std::shared_ptr<arrow::RecordBatch>) override;
    explicit SerializeBatchProcessor();
    ~SerializeBatchProcessor() override = default;
};

#endif //SPARK_EXAMPLE_SERIALIZEBATCHPROCESSOR_H
