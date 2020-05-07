//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#ifndef SPARK_EXAMPLE_ITERATOR_H
#define SPARK_EXAMPLE_ITERATOR_H

template<typename T>
class Iterator {
public:
    virtual bool hasNext() = 0;
    virtual T next() = 0;
    virtual ~Iterator() = default;
};

template<typename U, typename T>
class BatchProcessor {
public:
    virtual T process(U batch) = 0;
    virtual ~BatchProcessor() = default;
};


#endif //SPARK_EXAMPLE_ITERATOR_H
