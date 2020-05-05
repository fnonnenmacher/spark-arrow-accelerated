package nl.tudelft.ewi.abs.nonnenmacher

class ArrowProcessorJni {

  @native def sum(objectId: Array[Byte]): Long

  @native def readParquete(filename: String, objectIdOut: Array[Byte]): Unit

  @native def addingThreeValues(objectIdIn: Array[Byte], objectIdOut: Array[Byte]): Array[Byte]

}
