//
// Created by Fabian Nonnenmacher on 11.05.20.
//

#include "nl_tudelft_ewi_abs_nonnenmacher_NativeParquetReader.h"
#include "nl_tudelft_ewi_abs_nonnenmacher_JNIProcessorFactory_Initializer.h"
#include "JNINativeParquetReader.h"
#include "jni/Assertions.h"
#include "utils.h"
#include "jni/ProtobufSchemaDeserializer.h"
#include "jni/Converters.h"

NativeParquetReader::NativeParquetReader(const std::string &file_name, const std::shared_ptr<arrow::Schema> &schema,
                                         int num_rows) {
    this->_num_rows = num_rows;
    this->_schema = schema;

    parquet::ArrowReaderProperties properties = parquet::ArrowReaderProperties();
    properties.set_batch_size(num_rows);

    ASSERT_OK(parquet::arrow::FileReader::Make(arrow::default_memory_pool(),
                                               parquet::ParquetFileReader::OpenFile(file_name),
                                               properties,
                                               &reader));

    std::shared_ptr<arrow::Schema> file_schema;
    ASSERT_OK(reader->GetSchema(&file_schema));
    const std::vector<std::string> &field_names = file_schema->field_names();

    std::vector<int> field_indices;

    for (const auto &field: schema->field_names()) {
        int index = findInVector(field_names, field);
        field_indices.push_back(index);
        std::cout << "Found " << field <<" at index " << index << std::endl;
    }
    ASSERT_OK(reader->GetRecordBatchReader({0}, field_indices, &rb_reader));
}

void NativeParquetReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* record_batch_out) {
    ASSERT_OK(rb_reader->ReadNext(record_batch_out));
}

int NativeParquetReader::num_rows() const {
    return _num_rows;
}

std::shared_ptr<arrow::Schema> NativeParquetReader::schema() const {
    return _schema;
}