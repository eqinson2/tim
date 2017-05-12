package com.ericsson.ema.tim.dml

import com.ericsson.ema.tim.dml.predicate.Predicate

/**
  * Created by eqinson on 2017/5/12.
  */
trait Selector {
	def from(tab: String): Selector

	def where(predicate: Predicate): Selector

	def limit(limit: Int): Selector

	def skip(skip: Int): Selector

	def collect(): List[Object]

	def collectBySelectFields(): List[List[Object]]

	def count(): Long

	def exists(): Boolean
}
