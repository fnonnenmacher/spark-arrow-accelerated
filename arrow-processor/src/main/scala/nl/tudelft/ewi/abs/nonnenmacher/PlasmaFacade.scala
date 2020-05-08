package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.plasma.PlasmaClient

import scala.util.Random

object PlasmaFacade {

  type ObjectId = Array[Byte]

  val PLASMA_SOCKET_NAME = "/tmp/plasma"

  private lazy val random: Random = {
    new Random(12345)
  }

  lazy val client: PlasmaClient = {
    System.loadLibrary("plasma_java")
    new PlasmaClient(PLASMA_SOCKET_NAME, "", 0)
  }

  def randomObjectId(): ObjectId = {
    //Attention: ObjectId must not contain bytes with value 0 - > therefore `random.nextBytes()` not possible.
    def randomByte(): Byte = {
      (random.nextInt(255) + 1).toByte
    }

    new ObjectId(20).map(_ => randomByte())
  }

  def create(data: Array[Byte]): ObjectId = {
    val objectId = randomObjectId()
    client.put(objectId, data, null)
    objectId
  }

  def delete(objectId: ObjectId): Unit = {
    client.delete(objectId)
  }

  def get(objectId: ObjectId): Array[Byte] = {
    client.get(objectId, 0, false)
  }

  def writeToPlasma(): ClosableFunction[Array[Byte], Array[Byte]] = new ClosableFunction[Array[Byte], Array[Byte]] {

    var objectId: ObjectId = _;

    override def apply(data: Array[Byte]): Array[Byte] = {
      deletePreviousObjectId()
      objectId = PlasmaFacade.create(data)
      objectId
    }

    private def deletePreviousObjectId(): Unit = {
      if (objectId != null) {
        PlasmaFacade.delete(objectId);
      }
    }

    override def close(): Unit = deletePreviousObjectId();
  }
}
