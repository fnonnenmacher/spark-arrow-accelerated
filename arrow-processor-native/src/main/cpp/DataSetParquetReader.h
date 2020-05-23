#ifndef SPARK_EXAMPLE_JNINATIVEPARQUETREADER_H
#define SPARK_EXAMPLE_JNINATIVEPARQUETREADER_H

#include <iostream>
#include <arrow/api.h>
#include <arrow/dataset/api.h>
#include <parquet/arrow/reader.h>
#include <arrow/util/iterator.h>

class DataSetParquetReader {
private:
    std::shared_ptr<arrow::Schema> _schema;
    std::shared_ptr<arrow::RecordBatchIterator> recordBatchIter;
    std::shared_ptr<arrow::dataset::Dataset> dataset;
    std::shared_ptr<arrow::dataset::Scanner> scanner;
    std::shared_ptr<arrow::dataset::ScanTaskIterator> scan_task_it;
public:
    std::shared_ptr<arrow::Schema> schema() const { return _schema;};
    DataSetParquetReader(const std::string& file_name, const std::shared_ptr<arrow::Schema>& schema_file, const std::shared_ptr<arrow::Schema>& schema_out, int num_rows);
    std::shared_ptr<arrow::RecordBatch> ReadNext();
    ~DataSetParquetReader() = default;
};

#endif //SPARK_EXAMPLE_JNINATIVEPARQUETREADER_H