package nl.tudelft.ewi.abs.nonnenmacher

class ArrowProcessorJni {

  @native def sum(objectId: Array[Byte]): Long

  @native def readParquete(filename: String): Long

  @native def addingThreeValues(objectIdIn: Array[Byte]): Array[Byte]

}
