//
// Created by Fabian Nonnenmacher on 11.05.20.
//

#include "nl_tudelft_ewi_abs_nonnenmacher_NativeParquetReader.h"
#include "nl_tudelft_ewi_abs_nonnenmacher_JNIProcessorFactory_Initializer.h"
#include "DataSetParquetReader.h"
#include "jni/Assertions.h"
#include "utils.h"
#include "jni/ProtobufSchemaDeserializer.h"
#include "jni/Converters.h"

#include <arrow/dataset/api.h>
#include "arrow/dataset/file_base.h"
#include <arrow/filesystem/api.h>

#include "arrow/type.h"
#include "arrow/util/iterator.h"

#include "arrow/dataset/filter.h"

using namespace arrow::dataset;

DataSetParquetReader::DataSetParquetReader(const std::string &file_name,
                                           const std::shared_ptr<arrow::Schema> &schema_file,
                                           const std::shared_ptr<arrow::Schema> &schema_out,
                                           int num_rows) {
    // output schema
    this->_schema = schema_out;

    // file path TODO: Support list of files
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

    // Build & configure scanner
    std::shared_ptr<arrow::dataset::ScannerBuilder> scanner_builder = dataset->NewScan().ValueOrDie();
//    ASSERT_OK(scanner_builder->Filter( ("int-field"_ < 1000).Copy()) ); //TODO send filters from spark
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


//    if (scan_task_it.Next().ok()) {
//        std::cout <<"There are more task iterators available" << std::endl;
//    }
}

std::shared_ptr<arrow::RecordBatch> DataSetParquetReader::ReadNext() {
    return recordBatchIter->Next().ValueOrDie(); //TODO detect end
}


JNIEXPORT jlong JNICALL
Java_nl_tudelft_ewi_abs_nonnenmacher_JNIProcessorFactory_00024Initializer_initNativeParquetReader
        (JNIEnv *env, jobject, jstring java_file_name, jbyteArray schema_file_jarr, jbyteArray schema_out_jarr,
         jint num_rows) {

    std::string file_name = get_java_string(env, java_file_name);

    jsize schema_file_len = env->GetArrayLength(schema_file_jarr);
    jbyte *schema_file_bytes = env->GetByteArrayElements(schema_file_jarr, 0);
    std::shared_ptr<arrow::Schema> schema_file = ReadSchemaFromProtobufBytes(schema_file_bytes, schema_file_len);

    jsize schema_out_len = env->GetArrayLength(schema_out_jarr);
    jbyte *schema_out_bytes = env->GetByteArrayElements(schema_out_jarr, 0);
    std::shared_ptr<arrow::Schema> schema_out = ReadSchemaFromProtobufBytes(schema_out_bytes, schema_out_len);

    return (jlong) new DataSetParquetReader(file_name, schema_file, schema_out, (int) num_rows);
}

JNIEXPORT jint JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_NativeParquetReader_readNext
        (JNIEnv *env, jobject, jlong process_ptr, jobject jexpander, jlongArray buf_addrs, jlongArray buf_sizes) {

    // Read next record batch from parquete file
    DataSetParquetReader *datasetParquetReader = (DataSetParquetReader *) process_ptr;
    std::shared_ptr<arrow::RecordBatch> out_batch = datasetParquetReader->ReadNext();

    // Convert java arrays
    int out_bufs_len = env->GetArrayLength(buf_addrs);
    if (out_bufs_len != env->GetArrayLength(buf_sizes)) {
        std::cout << "mismatch in arraylen of buf_addrs and buf_sizes";
        return -1;
    }

    jlong *out_buf_addrs = env->GetLongArrayElements(buf_addrs, 0);
    jlong *out_buf_sizes = env->GetLongArrayElements(buf_sizes, 0);

    //Copy imported recordbatch in buffers allocated from java
    ASSERT_OK(copy_record_batch_ito_buffers(env, jexpander, out_batch, out_buf_addrs, out_buf_sizes, out_bufs_len));

    return out_batch->num_rows();
}

JNIEXPORT void JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_NativeParquetReader_close
        (JNIEnv *env, jobject, jlong process_ptr) {
    delete (DataSetParquetReader *) process_ptr;
}