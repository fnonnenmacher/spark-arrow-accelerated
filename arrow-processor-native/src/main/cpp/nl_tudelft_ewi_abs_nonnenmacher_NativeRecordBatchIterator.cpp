#include "nl_tudelft_ewi_abs_nonnenmacher_NativeRecordBatchIterator.h"

#include "ParqueteToPlasmaReader.h"

using namespace plasma;
using namespace std;

JNIEXPORT jboolean JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_NativeRecordBatchIterator_hasNext
        (JNIEnv *env, jobject obj, jlong p_native_ptr){
    return (jboolean) ((ParqueteToPlasmaReader *) p_native_ptr)->hasNext();
}

jbyteArray to_java(JNIEnv *env, const ObjectID &object_id_out) {
    jbyteArray javaObjectId = env->NewByteArray(UniqueID::size());
    env->SetByteArrayRegion(javaObjectId, 0, UniqueID::size(), (jbyte *) object_id_out.data());
    return javaObjectId;
}

JNIEXPORT jbyteArray JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_NativeRecordBatchIterator_next
        (JNIEnv *env, jobject obj, jlong p_native_ptr) {
    ObjectID object_id = ((ParqueteToPlasmaReader *) p_native_ptr)->next();
    return to_java(env, object_id);
}