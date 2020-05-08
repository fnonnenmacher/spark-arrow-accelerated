#include "nl_tudelft_ewi_abs_nonnenmacher_NativeRecordBatchIterator.h"
#include "nl_tudelft_ewi_abs_nonnenmacher_NativeRecordBatchIterator_Initializer.h"

#include "ParqueteToPlasmaReader.h"
#include "JavaConverter.h"


using namespace plasma;
using namespace std;

JNIEXPORT jlong JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_NativeRecordBatchIterator_00024Initializer_init
        (JNIEnv *env, jobject obj, jstring java_file_name, jintArray java_field_indices) {

    // convert java array to object id
    string file_path = get_java_string(env, java_file_name);
    vector<int> field_indices = get_java_int_array(env, java_field_indices);

    cout << "NativeRecordBatchIterator_init(filepath='" << file_path << "', field_names=['" << field_indices.at(0);
    for (int i=1; i <field_indices.size(); i++) {
        cout << "', '" << field_indices.at(i);
    }
    cout << "']) called." << endl;

    return (jlong) new ParqueteToPlasmaReader(file_path, field_indices);
}

JNIEXPORT jboolean JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_NativeRecordBatchIterator_hasNext
        (JNIEnv *env, jobject obj, jlong p_native_ptr) {
    cout << "NativeRecordBatchIterator_hasNext() called."<< endl;
    return (jboolean) ((ParqueteToPlasmaReader *) p_native_ptr)->hasNext();
}

JNIEXPORT jbyteArray JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_NativeRecordBatchIterator_next
        (JNIEnv *env, jobject obj, jlong p_native_ptr) {
    std::shared_ptr<ObjectID> object_id = ((ParqueteToPlasmaReader *) p_native_ptr)->next();
    cout << "NativeRecordBatchIterator_next() called."<< endl;
    return object_id_to_java_(env, *object_id);
}

JNIEXPORT void JNICALL Java_nl_tudelft_ewi_abs_nonnenmacher_NativeRecordBatchIterator_close
        (JNIEnv *env, jobject obj, jlong p_native_ptr) {
    delete ((ParqueteToPlasmaReader *) p_native_ptr);
    cout << "NativeRecordBatchIterator_close() called."<< endl;
}