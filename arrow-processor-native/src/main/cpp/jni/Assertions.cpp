
#include "Assertions.h"

static jclass gandiva_exception_;
static JNIEnv* jenv;

void JNI_OnLoad_Assertions(JNIEnv *env, void *reserved) {
    jenv = env;
    jclass localExceptionClass =
            env->FindClass("org/apache/arrow/gandiva/exceptions/GandivaException");
    gandiva_exception_ = (jclass) env->NewGlobalRef(localExceptionClass);
    env->ExceptionDescribe();
    env->DeleteLocalRef(localExceptionClass);
}

void throwJavaException(std::string msg){
    jenv->ThrowNew(gandiva_exception_, msg.c_str());
    exit(-1);
}