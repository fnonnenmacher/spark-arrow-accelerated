//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#ifndef SPARK_EXAMPLE_THREEINTADDERPROCESSOR_H
#define SPARK_EXAMPLE_THREEINTADDERPROCESSOR_H

#include "Processor.h"
#include "arrow/api.h"
#include "arrow/io/api.h"
#include <iostream>

using namespace std;

class ThreeIntAdderProcessor : Processor{
    std::shared_ptr<arrow::RecordBatch> process(std::shared_ptr<arrow::RecordBatch> input) override;
public:
    explicit ThreeIntAdderProcessor(std::shared_ptr<arrow::Schema> schema_)
    : Processor(std::move(schema_)){}
    ~ThreeIntAdderProcessor() override = default;
};
#endif //SPARK_EXAMPLE_THREEINTADDERPROCESSOR_H
