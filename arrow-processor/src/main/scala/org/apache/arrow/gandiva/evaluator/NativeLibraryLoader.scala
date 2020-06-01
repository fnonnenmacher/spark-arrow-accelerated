package org.apache.arrow.gandiva.evaluator

object NativeLibraryLoader {

  def load(): Unit = {
    _load
  }

  private lazy val _load: Boolean = {
    JniLoader.getInstance() //needs to load gandiva libraries first to avoid conflicts (only package visible)

    System.loadLibrary("protobuf")
    System.loadLibrary("arrow")
    System.loadLibrary("parquet")
    System.loadLibrary("arrow_dataset")
    System.loadLibrary("fletcher_echo")
    System.loadLibrary("fletcher")
    System.loadLibrary("arrow-processor-native")
    true
  }
}
