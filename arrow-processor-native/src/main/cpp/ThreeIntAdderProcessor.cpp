//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#include "ThreeIntAdderProcessor.h"
#include "SerializeBatchProcessor.h"
#include "PlasmaProcessor.h"
#include "jni/Assertions.h"

using arrow::Int32Builder;

ThreeIntAdderProcessor::ThreeIntAdderProcessor() {
    plasma_reader_processor = std::make_unique<ReadFromPlasmaProcessor>();
    deserialize_processor = std::make_unique<DeserializeBatchProcessor>();
    serialize_processor = std::make_unique<SerializeBatchProcessor>();
    plasma_writer_processor = std::make_unique<WriteToPlasmaProcessor>();
}

shared_ptr<ObjectID> ThreeIntAdderProcessor::process(shared_ptr<ObjectID> object_id) {
    auto rb_in_data = plasma_reader_processor->process(object_id);
    auto rb_in = deserialize_processor->process(rb_in_data);

    auto rb_out = process(rb_in);

    auto rb_out_data = serialize_processor->process(rb_out);
    auto object_id_out = plasma_writer_processor->process(rb_out_data);

    return object_id_out;
}

shared_ptr<arrow::RecordBatch> ThreeIntAdderProcessor::process(const shared_ptr<arrow::RecordBatch>& record_batch) {
    // get individual input field vectors
    auto vector_in1 = std::static_pointer_cast<arrow::Int32Array>(record_batch->column(0));
    auto vector_in2 = std::static_pointer_cast<arrow::Int32Array>(record_batch->column(1));
    auto vector_in3 = std::static_pointer_cast<arrow::Int32Array>(record_batch->column(2));

    // initialize builder for integer field vector
    arrow::MemoryPool *pool = arrow::default_memory_pool();
    Int32Builder res_builder(pool);

    //iterate over rows and bild sum for every row
    for (int i = 0; i < record_batch->num_rows(); i++) {

        int v1 = vector_in1->raw_values()[i];
        int v2 = vector_in2->raw_values()[i];
        int v3 = vector_in3->raw_values()[i];
        int sum = v1 + v2 + v3;
        std::cout << "Calculate row: " << v1 << " + " << v2 << " + " << v3 << " = " << sum << std::endl;
        ASSERT_OK(res_builder.Append(sum));
    }

    // get field vector from builder
    std::shared_ptr<arrow::Array> res_array;
    ASSERT_OK(res_builder.Finish(&res_array));

    // define output schema
    std::vector<std::shared_ptr<arrow::Field>> schema_vector = {
            arrow::field("out", arrow::int32())};
    auto result_schema = std::make_shared<arrow::Schema>(schema_vector);

    // create record batch containing the created int vector
    std::vector<std::shared_ptr<arrow::Array>> vec;
    vec.push_back(res_array);
    return arrow::RecordBatch::Make(result_schema, record_batch->num_rows(), vec);
}

