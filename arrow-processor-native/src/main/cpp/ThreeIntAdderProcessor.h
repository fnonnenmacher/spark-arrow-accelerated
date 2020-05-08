//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#ifndef SPARK_EXAMPLE_THREEINTADDERPROCESSOR_H
#define SPARK_EXAMPLE_THREEINTADDERPROCESSOR_H

#include "Iterator.h"
#include "arrow/api.h"
#include "arrow/io/api.h"
#include <plasma/client.h>
#include <iostream>

using namespace plasma;
using namespace std;

class ThreeIntAdderProcessor : public BatchProcessor<std::shared_ptr<ObjectID>, std::shared_ptr<ObjectID>> {
private:
    unique_ptr<BatchProcessor<std::shared_ptr<ObjectID>, std::shared_ptr<arrow::Buffer>>> plasma_reader_processor;
    unique_ptr<BatchProcessor<std::shared_ptr<arrow::Buffer>, std::shared_ptr<arrow::RecordBatch>>> deserialize_processor;
    unique_ptr<BatchProcessor<std::shared_ptr<arrow::RecordBatch>, std::shared_ptr<arrow::Buffer>>> serialize_processor;
    unique_ptr<BatchProcessor<std::shared_ptr<arrow::Buffer>, std::shared_ptr<ObjectID>>> plasma_writer_processor;
    static shared_ptr<arrow::RecordBatch> process(const shared_ptr<arrow::RecordBatch>& batch);
public:
    ThreeIntAdderProcessor();
    shared_ptr<ObjectID> process(shared_ptr<ObjectID> batch) override;
    ~ThreeIntAdderProcessor() override = default;
};
#endif //SPARK_EXAMPLE_THREEINTADDERPROCESSOR_H
