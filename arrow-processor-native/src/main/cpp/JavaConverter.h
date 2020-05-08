//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#ifndef SPARK_EXAMPLE_JAVACONVERTER_H
#define SPARK_EXAMPLE_JAVACONVERTER_H

#include <jni.h>
#include <plasma/client.h>

jbyteArray object_id_to_java_(JNIEnv *env, const plasma::ObjectID &object_id_out);

plasma::ObjectID object_id_from_java(JNIEnv *env, _jbyteArray *java_array);

std::string get_java_string(JNIEnv *env, jstring java_string);

std::vector<std::string> get_java_string_array(JNIEnv *env, jobjectArray jstringArr);

std::vector<int> get_java_int_array(JNIEnv *env, jintArray java_field_indices);

#endif //SPARK_EXAMPLE_JAVACONVERTER_H
