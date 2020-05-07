//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#include "PlasmaProcessor.h"
#include "my_assert.h"

#include <utility>

#include <random>

ObjectID random_object_id();

WriteToPlasmaProcessor::WriteToPlasmaProcessor() {
    client = std::make_unique<PlasmaClient>(PlasmaClient());
    ASSERT_OK(client->Connect("/tmp/plasma"));
    std::cout << "Plasma client connected."  << std::endl;
}

ObjectID WriteToPlasmaProcessor::process(std::shared_ptr<arrow::Buffer> buffer) {
    // Randomly generate an Object ID.
    ObjectID object_id = random_object_id();

    //TODO Same seed is used
    client->Delete(object_id);

    // Write bytes of result record batch to plasma store
    std::shared_ptr<Buffer> plasma_buffer;
    arrow::Status s1 = client->Create(object_id, buffer->size(), nullptr, 0, &plasma_buffer);

    //TODO
//    memcpy(plasma_buffer->mutable_data(), buffer->data(), sizeof(buffer->size()) );
    for (size_t i = 0; i < buffer->size(); i++) {
        plasma_buffer->mutable_data()[i] = buffer->data()[i];
    }

    std::cout << "Plasma client created result object: " << s1.ok() << std::endl;
    arrow::Status s3 = client->Seal(object_id);
    std::cout << "Plasma client wrote result (" << plasma_buffer->size() << " bytes) to the plasma store" << std::endl;

    return object_id;
}

WriteToPlasmaProcessor::~WriteToPlasmaProcessor() {
    arrow::Status s = client->Disconnect();
    std::cout << "Plasma client disconnected:"<< s.ok()  << std::endl;
};

// TODO
// Copied from plasma/test_util.h
ObjectID random_object_id() {
    static uint32_t random_seed = 0;
    std::mt19937 gen(random_seed++);
    std::uniform_int_distribution<uint32_t> d(0, std::numeric_limits<uint8_t>::max());
    ObjectID result;
    uint8_t *data = result.mutable_data();
    std::generate(data, data + kUniqueIDSize,
                  [&d, &gen] { return static_cast<uint8_t>(d(gen)); });
    return result;
}

ReadFromPlasmaProcessor::ReadFromPlasmaProcessor() {
    client = std::make_unique<PlasmaClient>(PlasmaClient());
    ASSERT_OK(client->Connect("/tmp/plasma"));
    std::cout << "Plasma client connected."  << std::endl;
}

ReadFromPlasmaProcessor::~ReadFromPlasmaProcessor() {
    arrow::Status s = client->Disconnect();
    std::cout << "Plasma client disconnected:"<< s.ok()  << std::endl;
}

std::unique_ptr<ObjectBuffer> ReadFromPlasmaProcessor::process(ObjectID object_id) {
    std::unique_ptr<ObjectBuffer> object_buffer(new ObjectBuffer);
    ASSERT_OK(client->Get(&object_id, 1, -1, object_buffer.get()));
    std::cout << "Plasma client read " << object_buffer->data->size() << " bytes." << std::endl;
    return object_buffer;
};
