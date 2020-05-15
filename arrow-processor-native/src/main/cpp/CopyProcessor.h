//
// Created by Fabian Nonnenmacher on 15.05.20.
//

#ifndef SPARK_EXAMPLE_COPYPROCESSOR_H
#define SPARK_EXAMPLE_COPYPROCESSOR_H


#include <utility>

#include "Processor.h"

/// \class CopyProcessor
/// \brief Simple processor which just uses the input record batch directly as output
///
/// The simplest imaginable processor, mainly used for testing if the memory converting between Java and C++ works correctly.
class CopyProcessor : Processor { ;
    std::shared_ptr<arrow::RecordBatch> process(std::shared_ptr<arrow::RecordBatch> input) override;

public:
    explicit CopyProcessor(std::shared_ptr<arrow::Schema> schema_)
    : Processor(std::move(schema_)){}
    ~CopyProcessor() override = default;
};


#endif //SPARK_EXAMPLE_COPYPROCESSOR_H
