#ifndef SPARK_EXAMPLE_JNINATIVEPARQUETREADER_H
#define SPARK_EXAMPLE_JNINATIVEPARQUETREADER_H


#include "Iterator.h"
#include <plasma/client.h>
#include <iostream>
#include <arrow/api.h>
#include <parquet/arrow/reader.h>

using namespace plasma;

class NativeParquetReader {
private:
    int _num_rows;
    std::shared_ptr<arrow::Schema> _schema;
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::shared_ptr<arrow::RecordBatchReader> rb_reader;
public:
    int num_rows() const;
    std::shared_ptr<arrow::Schema> schema() const;
    NativeParquetReader(const std::string& file_name, const std::shared_ptr<arrow::Schema>& schema, int num_rows);
    void ReadNext(std::shared_ptr<arrow::RecordBatch>* record_batch_out);
    ~NativeParquetReader() = default;
};

#endif //SPARK_EXAMPLE_JNINATIVEPARQUETREADER_H