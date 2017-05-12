package com.ericsson.ema.tim.dml.predicate

import com.ericsson.ema.tim.dml.SelectClause

/**
  * Created by eqinson on 2017/5/12.
  */
abstract class AbstractPredicate(val field: String, val valueToComp: Object) extends SelectClause {
	override def getField: String = field

	def eval(tuple: Object): Boolean
}
