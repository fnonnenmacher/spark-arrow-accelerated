//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#ifndef SPARK_EXAMPLE_MY_ASSERT_H
#define SPARK_EXAMPLE_MY_ASSERT_H

#include <iostream>

#define ASSERT_OK(s)                                                                        \
  do {                                                                                      \
    ::arrow::Status _s = ::arrow::internal::GenericToStatus(s);                             \
    if (!_s.ok()) {                                                                         \
        std::cout << "ERROR " << _s.CodeAsString() << ": " << _s.message() << std::endl;    \
        throw std::exception();                                                             \
    }                                                                                       \
  } while (0)

#endif //SPARK_EXAMPLE_MY_ASSERT_H
