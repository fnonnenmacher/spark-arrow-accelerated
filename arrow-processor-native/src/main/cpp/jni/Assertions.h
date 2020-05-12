//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#ifndef SPARK_EXAMPLE_ASSERTIONS_H
#define SPARK_EXAMPLE_ASSERTIONS_H

#include <iostream>
#include <jni.h>

void JNI_OnLoad_Assertions(JNIEnv *env, void *reserved);

void throwJavaException(std::string msg);

#define ASSERT_OK(s)                                                                        \
  do {                                                                                      \
    ::arrow::Status _s = ::arrow::internal::GenericToStatus(s);                             \
    if (!_s.ok()) {                                                                         \
        std::cout << "ERROR " << _s.CodeAsString() << ": " << _s.message() << std::endl;    \
        throwJavaException("ERROR - " + s.CodeAsString() + ": " + s.message());             \
    }                                                                                       \
  } while (0)

#define ASSERT(condition, msg)                                                              \
  do {                                                                                      \
    if (!(condition)) {                                                                     \
    return ::arrow::Status::Invalid(msg);                                                                \
    }                                                                                       \
  } while (0)


#endif //SPARK_EXAMPLE_ASSERTIONS_H
