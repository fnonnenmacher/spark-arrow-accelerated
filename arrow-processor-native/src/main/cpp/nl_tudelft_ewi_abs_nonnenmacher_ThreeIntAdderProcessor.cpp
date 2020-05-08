
#include "nl_tudelft_ewi_abs_nonnenmacher_ThreeIntAdderProcessor.h"
#include "nl_tudelft_ewi_abs_nonnenmacher_ThreeIntAdderProcessor_Initializer.h"
#include "ThreeIntAdderProcessor.h"
#include "JavaConverter.h"
#include <plasma/client.h>


JNIEXPORT jlong JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_ThreeIntAdderProcessor_00024Initializer_init
        (JNIEnv *, jobject) {
    return (jlong) new ThreeIntAdderProcessor();
}

JNIEXPORT jbyteArray JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_ThreeIntAdderProcessor_process
        (JNIEnv *env, jobject jobj, jlong proc_ptr, jbyteArray object_id_java) {

    auto proc = (ThreeIntAdderProcessor *) proc_ptr;

    shared_ptr<ObjectID> object_id = std::make_shared<ObjectID>(object_id_from_java(env, object_id_java));

    shared_ptr<ObjectID> object_id_out = proc->process(object_id);

    return object_id_to_java_(env, *object_id_out);
}

JNIEXPORT void JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_ThreeIntAdderProcessor_close
        (JNIEnv *env, jobject jobj, jlong proc_ptr) {
    delete ((ThreeIntAdderProcessor *) proc_ptr);
}