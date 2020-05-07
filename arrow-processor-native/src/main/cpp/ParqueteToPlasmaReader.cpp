//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#include "ParqueteToPlasmaReader.h"

#include "ParqueteReaderIterator.h"
#include "SerializeBatchProcessor.h"
#include "PlasmaProcessor.h"

ParqueteToPlasmaReader::ParqueteToPlasmaReader(const char *file_name) {
    record_batch_iterator = std::make_unique<ParqueteReaderIterator>(file_name);
    batch_to_buffer_processor = std::make_unique<SerializeBatchProcessor>();
    buffer_to_plasma_processor = std::make_unique<WriteToPlasmaProcessor>();
}

bool ParqueteToPlasmaReader::hasNext() {
    return record_batch_iterator->hasNext();
}

std::shared_ptr<ObjectID> ParqueteToPlasmaReader::next() {
    auto record_batch = record_batch_iterator->next();
    auto buffer = batch_to_buffer_processor->process(record_batch);
    return buffer_to_plasma_processor->process(buffer);
}