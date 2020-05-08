//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#ifndef SPARK_EXAMPLE_PARQUETEREADERITERATOR_H
#define SPARK_EXAMPLE_PARQUETEREADERITERATOR_H


#include <memory>
#include <arrow/api.h>
#include "my_assert.h"
#include <jni.h>
#include <parquet/arrow/reader.h>
#include "Iterator.h"

class ParqueteReaderIterator : public Iterator<std::shared_ptr<arrow::RecordBatch>> {
private :
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::shared_ptr<arrow::RecordBatchReader> rb_reader;
    std::shared_ptr<arrow::RecordBatch> next_batch = nullptr;
    std::vector<std::string> fields;
public:
    explicit ParqueteReaderIterator(const std::string& file_name, const std::vector<int>& fields);
    bool hasNext() override;
    std::shared_ptr<arrow::RecordBatch> next() override;
    ~ParqueteReaderIterator() override = default;
};

#endif //SPARK_EXAMPLE_PARQUETEREADERITERATOR_H
