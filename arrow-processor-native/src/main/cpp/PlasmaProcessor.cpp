//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#include "PlasmaProcessor.h"
#include "my_assert.h"

#include <utility>

#include <random>

std::shared_ptr<ObjectID> random_object_id();

WriteToPlasmaProcessor::WriteToPlasmaProcessor() {
    client = std::make_unique<PlasmaClient>(PlasmaClient());
    ASSERT_OK(client->Connect("/tmp/plasma"));
    std::cout << "Plasma client connected." << std::endl;
}

std::shared_ptr<ObjectID> WriteToPlasmaProcessor::process(std::shared_ptr<arrow::Buffer> buffer) {
    // We assume the previous object in the plasma store has been processed by now so we delete it
    if (last_plasma_object != nullptr) {
        ASSERT_OK(client->Release(*last_plasma_object));
        ASSERT_OK(client->Delete(*last_plasma_object));
    }

    // Randomly generate an Object ID.
    last_plasma_object = random_object_id();

    // ATTENTION: If program crashes object ID might not be deleted as it should be!
    // Then new object cannot be stored. Therfor sometimes t is helpful to uncomment the followign line
    client->Delete(*last_plasma_object);

    // Write bytes of result record batch to plasma store
    std::shared_ptr<Buffer> plasma_buffer;
    arrow::Status s1 = client->Create(*last_plasma_object, buffer->size(), nullptr, 0, &plasma_buffer);

    //Copy into plasma
    memcpy(plasma_buffer->mutable_data(), buffer->data(), buffer->size());

    arrow::Status s3 = client->Seal(*last_plasma_object);
//    std::cout << "Plasma client wrote result (" << plasma_buffer->size() << " bytes) to the plasma store" << std::endl;

    return last_plasma_object;
}

WriteToPlasmaProcessor::~WriteToPlasmaProcessor() {
    std::cout <<"PLASMA DESTRUCTOR CALLED";
    //delete the previously stored object
    if (last_plasma_object != nullptr) {
        client->Release(*last_plasma_object);
        Status s = client->Delete(*last_plasma_object);
        std::cout << "DESTRUCTOR: Object "<< last_plasma_object->hex() <<"deleted:" << s.ok() << std::endl;
    }
    arrow::Status s = client->Disconnect();
    std::cout << "Plasma client disconnected:" << s.ok() << std::endl;
};

// TODO Seed?!
// Copied from plasma/test_util.h
std::shared_ptr<ObjectID> random_object_id() {
    static uint32_t random_seed = 0;
    std::mt19937 gen(random_seed++);
    std::uniform_int_distribution<uint32_t> d(0, std::numeric_limits<uint8_t>::max());
    std::shared_ptr<ObjectID> result = std::make_shared<ObjectID>(ObjectID());
    uint8_t *data = result->mutable_data();
    std::generate(data, data + kUniqueIDSize,
                  [&d, &gen] { return static_cast<uint8_t>(d(gen)); });
    return result;
}

ReadFromPlasmaProcessor::ReadFromPlasmaProcessor() {
    client = std::make_unique<PlasmaClient>(PlasmaClient());
    ASSERT_OK(client->Connect("/tmp/plasma"));
    std::cout << "Plasma client connected." << std::endl;
}

ReadFromPlasmaProcessor::~ReadFromPlasmaProcessor() {
    arrow::Status s = client->Disconnect();
    std::cout << "Plasma client disconnected:" << s.ok() << std::endl;
}

std::shared_ptr<arrow::Buffer> ReadFromPlasmaProcessor::process(std::shared_ptr<ObjectID> object_id) {
    ObjectBuffer object_buffer;
    ASSERT_OK(client->Get(object_id.get(), 1, -1, &object_buffer));
    std::cout << "Plasma client read " << object_buffer.data->size() << " bytes." << std::endl;
    return object_buffer.data;
};
