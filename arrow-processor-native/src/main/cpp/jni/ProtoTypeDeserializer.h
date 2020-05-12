//
// Created by Fabian Nonnenmacher on 11.05.20.
//

#ifndef SPARK_EXAMPLE_PROTOTYPEDESERIALIZER_H
#define SPARK_EXAMPLE_PROTOTYPEDESERIALIZER_H

#include <arrow/api.h>
#include <jni.h>
#include "Types.pb.h"

std::shared_ptr<arrow::Schema> ReadSchemaFromProtobufBytes(jbyte *schema_bytes, jsize schema_len);

#endif //SPARK_EXAMPLE_PROTOTYPEDESERIALIZER_H
