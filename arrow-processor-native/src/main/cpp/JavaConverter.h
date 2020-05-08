//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#ifndef SPARK_EXAMPLE_JAVACONVERTER_H
#define SPARK_EXAMPLE_JAVACONVERTER_H

#include <jni.h>
#include <plasma/client.h>

jbyteArray object_id_to_java_(JNIEnv *env, const plasma::ObjectID &object_id_out);

plasma::ObjectID object_id_from_java(JNIEnv *env, _jbyteArray *java_array);

#endif //SPARK_EXAMPLE_JAVACONVERTER_H
