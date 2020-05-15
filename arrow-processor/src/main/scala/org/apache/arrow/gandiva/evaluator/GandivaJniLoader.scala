package org.apache.arrow.gandiva.evaluator

object GandivaJniLoader {

  def load(): Unit ={
    JniLoader.getInstance()
  }
}
