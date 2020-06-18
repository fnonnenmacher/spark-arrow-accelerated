//
// Created by Fabian Nonnenmacher on 18.06.20.
//

#ifndef SPARK_EXAMPLE_FLETCHERREDUCTIONPROCESSOR_H
#define SPARK_EXAMPLE_FLETCHERREDUCTIONPROCESSOR_H

#include <arrow/api.h>
#include <iostream>
#include <utility>
#include <fletcher/api.h>

#include "nl_tudelft_ewi_abs_nonnenmacher_FletcherReductionProcessor.h"

class FletcherReductionProcessor {
    std::shared_ptr<fletcher::Platform> platform;
public:
    std::shared_ptr<arrow::Schema> schema;
    explicit FletcherReductionProcessor(std::shared_ptr<arrow::Schema> input_schema);
    ~FletcherReductionProcessor() = default;
    long reduce(const std::shared_ptr<arrow::RecordBatch> &input);
};


#endif //SPARK_EXAMPLE_FLETCHERREDUCTIONPROCESSOR_H
