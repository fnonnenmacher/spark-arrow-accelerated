//
// Created by Fabian Nonnenmacher on 07.05.20.
//

#include "JavaConverter.h"

using namespace plasma;

jbyteArray object_id_to_java_(JNIEnv *env, const ObjectID &object_id_out) {
    jbyteArray javaObjectId = env->NewByteArray(UniqueID::size());
    env->SetByteArrayRegion(javaObjectId, 0, UniqueID::size(), (jbyte *) object_id_out.data());
    return javaObjectId;
}

plasma::ObjectID object_id_from_java(JNIEnv *env, _jbyteArray *java_array){
    char *object_id_bin = (char *) env->GetByteArrayElements(java_array, NULL);
    ObjectID object_id = ObjectID::from_binary(object_id_bin);
    return object_id;
}