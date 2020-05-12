#ifndef SPARK_EXAMPLE_UTILS_H
#define SPARK_EXAMPLE_UTILS_H

#include <ios>

/*
Generic function to find an element in vector and also its position.
It returns a pair of bool & int i.e.
bool : Represents if element is present in vector or not.
int : Represents the index of element in vector if its found else -1
*/
template<typename T>
int findInVector(const std::vector<T> &vecOfElements, const T &element) {
    // Find given element in vector
    auto it = std::find(vecOfElements.begin(), vecOfElements.end(), element);
    if (it != vecOfElements.end()) {
        return distance(vecOfElements.begin(), it);
    } else {
        throw "Element " + element + " not found in vector.";
    }
}

#endif //SPARK_EXAMPLE_UTILS_H
