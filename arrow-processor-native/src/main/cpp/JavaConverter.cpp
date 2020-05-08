//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#include "JavaConverter.h"
#include <iostream>

using namespace plasma;
using namespace std;

jbyteArray object_id_to_java_(JNIEnv *env, const ObjectID &object_id_out) {
    jbyteArray javaObjectId = env->NewByteArray(UniqueID::size());
    env->SetByteArrayRegion(javaObjectId, 0, UniqueID::size(), (jbyte *) object_id_out.data());
    return javaObjectId;
}

plasma::ObjectID object_id_from_java(JNIEnv *env, _jbyteArray *java_array){
    char *object_id_bin = (char *) env->GetByteArrayElements(java_array, nullptr);
    ObjectID object_id = ObjectID::from_binary(object_id_bin);
    return object_id;
}

string get_java_string(JNIEnv *env, jstring java_string){
    const jsize strLen = env->GetStringUTFLength(java_string);
    const char *charBuffer = env->GetStringUTFChars(java_string, nullptr);
    string str(charBuffer, strLen);
    env->ReleaseStringUTFChars(java_string, charBuffer);
    env->DeleteLocalRef(java_string);
    return str;
}

vector<string> get_java_string_array(JNIEnv *env, jobjectArray jstringArr) {
    vector<string> stringVec;

    // Get length
    int len = env->GetArrayLength(jstringArr);

    for (int i=0; i<len; i++) {
        // Cast array element to string
        jstring jstr = (jstring) (env->GetObjectArrayElement(jstringArr, i));

        // Convert Java string to std::string
        const jsize strLen = env->GetStringUTFLength(jstr);
        const char *charBuffer = env->GetStringUTFChars(jstr, nullptr);
        string str(charBuffer, strLen);

        // Push back string to vector
        stringVec.push_back(str);

        // Release memory
        env->ReleaseStringUTFChars(jstr, charBuffer);
        env->DeleteLocalRef(jstr);
    }
    return stringVec;
}

std::vector<int> get_java_int_array(JNIEnv *env, jintArray java_int_array) {
    int len = env->GetArrayLength(java_int_array);
    int* java_ints = (int*)env->GetIntArrayElements(java_int_array, nullptr);

    vector<int> int_vec;
    int_vec.reserve(len);
    for (int i=0; i< len; i++){
        int_vec.push_back(java_ints[i]);
    }
    return int_vec;
}
