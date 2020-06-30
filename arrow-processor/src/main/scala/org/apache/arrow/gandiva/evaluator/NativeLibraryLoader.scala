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

    System.getenv("FLETCHER_PLATFORM") match {
      case null | "ECHO" => System.loadLibrary("fletcher_echo")
      case "SNAP" => {
        System.loadLibrary("ocxl")
        System.load("fletcher_snap")
      }
      case other => throw new Exception(s"Fletcher platform '$other' not supported.")
    }

    System.loadLibrary("fletcher")
    System.loadLibrary("arrow-processor-native")
    true
  }
}
