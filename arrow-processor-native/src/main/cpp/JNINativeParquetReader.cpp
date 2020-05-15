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

JNIEXPORT jlong JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_JNIProcessorFactory_00024Initializer_initNativeParquetReader
        (JNIEnv *env, jobject, jstring java_file_name, jbyteArray schema_arr, jint num_rows) {

    std::string file_name = get_java_string(env, java_file_name);

    jsize schema_len = env->GetArrayLength(schema_arr);
    jbyte *schema_bytes = env->GetByteArrayElements(schema_arr, 0);
    std::shared_ptr<arrow::Schema> schema = ReadSchemaFromProtobufBytes(schema_bytes, schema_len);


    return (jlong) new NativeParquetReader(file_name, schema, (int) num_rows);
}

JNIEXPORT jint JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_NativeParquetReader_readNext
        (JNIEnv *env, jobject, jlong process_ptr, jobject jexpander, jlongArray buf_addrs, jlongArray buf_sizes) {

    // Read next record batch from parquete file
    NativeParquetReader *nativeParquetReader = (NativeParquetReader *) process_ptr;
    std::shared_ptr<arrow::RecordBatch> out_batch;
    nativeParquetReader->ReadNext(&out_batch);

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
        (JNIEnv *env, jobject, jlong process_ptr){
    delete (NativeParquetReader *) process_ptr;
}