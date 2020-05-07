//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#ifndef SPARK_EXAMPLE_PARQUETETOPLASMAREADER_H
#define SPARK_EXAMPLE_PARQUETETOPLASMAREADER_H

#include "Iterator.h"
#include "arrow/api.h"
#include "arrow/io/api.h"
#include <plasma/client.h>
#include <iostream>

using namespace plasma;
using namespace std;

class ParqueteToPlasmaReader : Iterator<std::shared_ptr<ObjectID>>{
private:
    unique_ptr<Iterator<std::shared_ptr<arrow::RecordBatch>>> record_batch_iterator;
    unique_ptr<BatchProcessor<std::shared_ptr<arrow::RecordBatch>, std::shared_ptr<arrow::Buffer>>> batch_to_buffer_processor;
    unique_ptr<BatchProcessor<std::shared_ptr<arrow::Buffer>, std::shared_ptr<ObjectID>>> buffer_to_plasma_processor;

public:
    explicit ParqueteToPlasmaReader(const char* file_name);
    bool hasNext() override;
    std::shared_ptr<ObjectID> next() override;
    ~ParqueteToPlasmaReader() override = default;
};

#endif //SPARK_EXAMPLE_PARQUETETOPLASMAREADER_H
