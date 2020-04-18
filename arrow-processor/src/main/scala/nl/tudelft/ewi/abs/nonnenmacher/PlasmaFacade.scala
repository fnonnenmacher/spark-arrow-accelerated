package nl.tudelft.ewi.abs.nonnenmacher

import org.apache.arrow.plasma.PlasmaClient

import scala.util.Random

object PlasmaFacade {

  type ObjectId = Array[Byte]

  val PLASMA_SOCKET_NAME = "/tmp/plasma"

  private lazy val random: Random = {
    val r = new Random()
    r.setSeed(12345)
    r
  }

  lazy val client: PlasmaClient = {
    System.loadLibrary("plasma_java")
    new PlasmaClient(PLASMA_SOCKET_NAME, "", 0)
  }

  def randomObjectId(): ObjectId = {
    //Attention: ObjectId must not contain bytes with value 0 - > therefore `random.nextBytes()` not possible.
    def randomByte(): Byte = {
      (random.nextInt(255) + 1).toByte;
    }

    new ObjectId(20).map(_ => randomByte())
  }

  def create(data: Array[Byte]): ObjectId = {

    val objectId = randomObjectId();
    PlasmaFacade.delete(objectId); //TODO remove this later, just because random with seed always generates same id
    client.put(objectId, data, null);
    objectId
  }

  def delete(objectId: ObjectId): Unit = {
    client.delete(objectId)
  }

  def get(objectId: ObjectId) : Array[Byte] = {
    val res = client.get(objectId, 0, false)
    client.delete(objectId)
    res
  }
}
