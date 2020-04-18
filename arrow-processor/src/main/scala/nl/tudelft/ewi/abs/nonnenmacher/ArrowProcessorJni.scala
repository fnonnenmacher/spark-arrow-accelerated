package nl.tudelft.ewi.abs.nonnenmacher

class ArrowProcessorJni {

  @native def sum(objectId: Array[Byte]): Long

  @native def addingThreeValues(objectIdIn: Array[Byte], objectIdOut: Array[Byte]): Array[Byte]

}
