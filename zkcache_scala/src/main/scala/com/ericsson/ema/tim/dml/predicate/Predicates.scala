package com.ericsson.ema.tim.dml.predicate

import com.ericsson.ema.tim.exception.DmlBadSyntaxException

/**
  * Created by eqinson on 2017/5/20.
  */
class Eq private(override val field: String, val valueToComp: Object) extends PredicateClause {
	override val StringMatcher: matcherFuncType = _ == _
	override val IntMatcher: matcherFuncType = (l, r) => l.asInstanceOf[Integer] == Integer.valueOf(r.asInstanceOf[String])
	override val BoolMatcher: matcherFuncType = (l, r) => l.asInstanceOf[java.lang.Boolean] == java.lang.Boolean.valueOf(r.asInstanceOf[String])
}

object Eq {
	def apply(field: String, value: String) = new Eq(field, value)
}

class UnEq private(override val field: String, val valueToComp: Object) extends PredicateClause {
	override val StringMatcher: matcherFuncType = _ != _
	override val IntMatcher: matcherFuncType = (l, r) => l.asInstanceOf[Integer] != Integer.valueOf(r.asInstanceOf[String])
	override val BoolMatcher: matcherFuncType = (l, r) => l.asInstanceOf[java.lang.Boolean] != java.lang.Boolean.valueOf(r.asInstanceOf[String])
}

object UnEq {
	def apply(field: String, value: String) = new UnEq(field, value)
}

class Like private(override val field: String, val valueToComp: Object) extends PredicateClause {
	override val StringMatcher: matcherFuncType = (l, r) => l.asInstanceOf[String].matches(r.asInstanceOf[String])
}

object Like {
	def apply(field: String, value: String) = new Like(field, value)
}

class UnLike private(override val field: String, val valueToComp: Object) extends PredicateClause {
	override val StringMatcher: matcherFuncType = (l, r) => !l.asInstanceOf[String].matches(r.asInstanceOf[String])
}

object UnLike {
	def apply(field: String, value: String) = new UnLike(field, value)
}

class BiggerThan private(override val field: String, val valueToComp: Object) extends PredicateClause {
	override val IntMatcher: matcherFuncType = (l, r) => l.asInstanceOf[Integer].compareTo(r.asInstanceOf[Integer]) > 0
}

object BiggerThan {
	def apply(field: String, value: Int) = new BiggerThan(field, Integer.valueOf(value))
}

class LessThan private(override val field: String, val valueToComp: Object) extends PredicateClause {
	override val IntMatcher: matcherFuncType = (l, r) => l.asInstanceOf[Integer].compareTo(r.asInstanceOf[Integer]) < 0
}

object LessThan {
	def apply(field: String, value: Int) = new LessThan(field, Integer.valueOf(value))
}

class Range private(override val field: String, val from: Int, val to: Int) extends PredicateClause {
	if (from > to) throw DmlBadSyntaxException("from must be lte to")
	override val IntMatcher: matcherFuncType = (l, _) => l.asInstanceOf[Integer].compareTo(from) >= 0 && l.asInstanceOf[Integer].compareTo(to) < 0
	override val valueToComp: Object = new AnyRef
}

object Range {
	def apply(field: String, from: Int, to: Int) = new Range(field, from, to)
}