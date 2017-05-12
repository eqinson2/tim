package com.ericsson.ema.tim.dml.predicate

/**
  * Created by eqinson on 2017/5/12.
  */
trait Predicate {
	def eval(tuple: Object): Boolean
}
