package com.weathernexus.utilities.bufr.readers

import scala.collection.immutable.Map

trait Reader[K, E] {
  protected val data: Map[K, List[E]]

  def get(key: K): Option[List[E]] = data.get(key)
  
  def getFirst(key: K): Option[E] = data.get(key).flatMap(_.headOption)
  
  def find(predicate: E => Boolean): List[E] =
    data.values.flatten.filter(predicate).toList
  
  def getAll: Map[K, List[E]] = data
  
  def getAllKeys: Set[K] = data.keys.toSet
}