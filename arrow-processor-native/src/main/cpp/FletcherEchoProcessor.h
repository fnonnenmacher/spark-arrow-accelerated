//
// Created by Fabian Nonnenmacher on 15.05.20.
//

#ifndef SPARK_EXAMPLE_FLETCHERECHOPROCESSOR_H
#define SPARK_EXAMPLE_FLETCHERECHOPROCESSOR_H

//#include <fletcher/api.h>
#include "Processor.h"

class FletcherEchoProcessor : Processor {
//    std::shared_ptr<fletcher::Platform> platform;
//    std::shared_ptr<fletcher::Context> context;
    std::shared_ptr<arrow::Schema> schema_out;

    std::shared_ptr<arrow::RecordBatch> process(std::shared_ptr<arrow::RecordBatch> input) override;

public:
    explicit FletcherEchoProcessor(std::shared_ptr<arrow::Schema> schema_);
    ~FletcherEchoProcessor() override = default;
};


#endif //SPARK_EXAMPLE_FLETCHERECHOPROCESSOR_H
