//
// Created by Fabian Nonnenmacher on 11.05.20.
//

#include "nl_tudelft_ewi_abs_nonnenmacher_NativeParquetReader.h"
#include "DataSetParquetReader.h"

#include "utils.h"
#include "jni/Assertions.h"
#include "jni/ProtobufSchemaDeserializer.h"
#include "jni/Converters.h"
#include "jni/JavaMemoryPool.h"

#include "arrow/api.h"
#include <arrow/dataset/api.h>
#include <arrow/filesystem/api.h>

using namespace arrow::dataset;

DataSetParquetReader::DataSetParquetReader(const std::shared_ptr<arrow::MemoryPool> &memory_pool,
                                           const std::string &file_name,
                                           const std::shared_ptr<arrow::Schema> &schema_file,
                                           const std::shared_ptr<arrow::Schema> &schema_out,
                                           int num_rows) {
    pool_ = memory_pool;

    // TODO: Support list of files
    std::shared_ptr<arrow::fs::LocalFileSystem> localfs = std::make_shared<arrow::fs::LocalFileSystem>();
    FileSource source(file_name, localfs.get());

    // Parameter for dataset creation
    std::shared_ptr<arrow::dataset::Expression> root_partition = arrow::dataset::scalar(true);
    auto format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    auto file_info = localfs->GetFileInfo(source.path()).ValueOrDie();

    // Create dataset
    dataset = arrow::dataset::FileSystemDataset::Make(schema_file,
                                                      root_partition,
                                                      format,
                                                      localfs,
                                                      {file_info}).ValueOrDie();

    std::shared_ptr<ScanContext> ctx_ = std::make_shared<ScanContext>();
    ctx_->pool = memory_pool.get();

    // Build & configure scanner
    std::shared_ptr<arrow::dataset::ScannerBuilder> scanner_builder = dataset->NewScan(ctx_).ValueOrDie();
//    ASSERT_OK(scanner_builder->Filter( ("int-field"_ < 1000).Copy()) ); //TODO send predicate push-down filters from scala
    ASSERT_OK(scanner_builder->Project(schema_out->field_names()));
    ASSERT_OK(scanner_builder->BatchSize(num_rows));

    scanner = scanner_builder->Finish().ValueOrDie();

    // batch_size oes not get set correctly with builder
    // Bug has been fixed, but not merged into 0.17.1
    // https://github.com/apache/arrow/pull/6967
    scanner->options()->batch_size = num_rows;

    // Get RecordBatchIterator
    scan_task_it = std::make_shared<arrow::dataset::ScanTaskIterator>(scanner->Scan().ValueOrDie());

    recordBatchIter = std::make_shared<arrow::RecordBatchIterator>(
            scan_task_it->Next().ValueOrDie()->Execute().ValueOrDie());

    //TODO: Semantics of ScanTask - Can there be multiple?

//    if (scan_task_it.Next().ok()) {
//        std::cout <<"There are more task iterators available" << std::endl;
//    }
}

std::shared_ptr<arrow::RecordBatch> DataSetParquetReader::ReadNext() {
    batch = recordBatchIter->Next().ValueOrDie();
    return batch;
}


JNIEXPORT jlong JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_NativeParquetReader_initNativeParquetReader
        (JNIEnv *env, jobject, jobject jmemorypool, jstring java_file_name, jbyteArray schema_file_jarr,
         jbyteArray schema_out_jarr,
         jint num_rows) {

    std::string file_name = get_java_string(env, java_file_name);

    jsize schema_file_len = env->GetArrayLength(schema_file_jarr);
    jbyte *schema_file_bytes = env->GetByteArrayElements(schema_file_jarr, 0);
    std::shared_ptr<arrow::Schema> schema_file = ReadSchemaFromProtobufBytes(schema_file_bytes, schema_file_len);

    jsize schema_out_len = env->GetArrayLength(schema_out_jarr);
    jbyte *schema_out_bytes = env->GetByteArrayElements(schema_out_jarr, 0);
    std::shared_ptr<arrow::Schema> schema_out = ReadSchemaFromProtobufBytes(schema_out_bytes, schema_out_len);

    std::shared_ptr<arrow::MemoryPool> pool = std::make_shared<JavaMemoryPool>(env, jmemorypool);
    return (jlong) new DataSetParquetReader(pool, file_name, schema_file, schema_out, (int) num_rows);
}

JNIEXPORT jboolean JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_NativeParquetReader_readNext
        (JNIEnv *env, jobject, jlong process_ptr, jlongArray jarr_vector_lengths, jlongArray jarr_vector_null_counts,
         jlongArray jarr_buf_addrs) {


    DataSetParquetReader *datasetParquetReader = (DataSetParquetReader *) process_ptr;

    // Read next record batch from parquet file
    std::shared_ptr<arrow::RecordBatch> out_batch = datasetParquetReader->ReadNext();

    // check if end reached
    std::shared_ptr<arrow::RecordBatch> end = arrow::IterationTraits<std::shared_ptr<arrow::RecordBatch>>::End();
    if (end == out_batch) {
        return false;
    }

    // Read buffers, field vector length & nullcount from RecordBatch
    const std::shared_ptr<arrow::Schema> &schema = out_batch->schema();
    auto num_fields = schema->num_fields();

    jlong buffer_addresses[num_fields * 3];
    jlong vector_lengths[num_fields];
    jlong vector_null_counts[num_fields];

    for (int i = 0; i < num_fields; i++) {
        const std::shared_ptr<arrow::Field> &field = schema->field(i);
        const std::shared_ptr<arrow::Array> &column = out_batch->column(i);
        const std::shared_ptr<arrow::ArrayData> &data = column->data();

        vector_lengths[i] = column->length();
        vector_null_counts[i] = column->null_count();

        const std::shared_ptr<arrow::Buffer> &validity_buffer = data->buffers[0];
        if (validity_buffer != nullptr) {
            buffer_addresses[3 * i + 0] = validity_buffer->address();
        } else {
            buffer_addresses[3 * i + 0] = 0;
        }

        const std::shared_ptr<arrow::Buffer> &value_buffer = data->buffers[1];
        buffer_addresses[3 * i + 1] = value_buffer->address();

        if (arrow::is_binary_like(field->type()->id())) {
            const std::shared_ptr<arrow::Buffer> &offset_buffer = data->buffers[2];
            buffer_addresses[3 * i + 2] = offset_buffer->address();
        } else {
            buffer_addresses[3 * i + 2] = 0;
        }
    }

    //Copy data into Java arrays
    env->SetLongArrayRegion(jarr_vector_lengths, 0, num_fields, vector_lengths);
    env->SetLongArrayRegion(jarr_vector_null_counts, 0, num_fields, vector_null_counts);
    env->SetLongArrayRegion(jarr_buf_addrs, 0, num_fields * 3, buffer_addresses);

    return true;
}

JNIEXPORT void JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_NativeParquetReader_close
        (JNIEnv *env, jobject, jlong process_ptr) {
    delete (DataSetParquetReader *) process_ptr;
}