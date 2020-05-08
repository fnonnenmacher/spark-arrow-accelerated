#include "nl_tudelft_ewi_abs_nonnenmacher_NativeRecordBatchIterator.h"

#include "ParqueteToPlasmaReader.h"
#include "JavaConverter.h"

using namespace plasma;
using namespace std;

JNIEXPORT jboolean JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_NativeRecordBatchIterator_hasNext
        (JNIEnv *env, jobject obj, jlong p_native_ptr){
    return (jboolean) ((ParqueteToPlasmaReader *) p_native_ptr)->hasNext();
}

JNIEXPORT jbyteArray JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_NativeRecordBatchIterator_next
        (JNIEnv *env, jobject obj, jlong p_native_ptr) {
    std::shared_ptr<ObjectID> object_id = ((ParqueteToPlasmaReader *) p_native_ptr)->next();
    return object_id_to_java_(env, *object_id);
}

JNIEXPORT void JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_NativeRecordBatchIterator_close
        (JNIEnv *env, jobject obj, jlong p_native_ptr){
    delete ((ParqueteToPlasmaReader *) p_native_ptr);
    std::cout <<"CLOSE CALLED";
}