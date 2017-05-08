package com.ericsson.ema.tim.pojo


/**
  * Created by eqinson on 2017/5/8.
  */
class NameType(var theName: String, var theType: String) {
	override def toString: String = "NameType{" + "name='" + theName + '\'' + ", type='" + theType + '\'' + '}'
}

class TableTuple(theName: String, theType: String) extends NameType(theName: String, theType: String) {
	private var tuples: List[NameType] = _

	def getTuples: List[NameType] = {
		if (tuples == null)
			tuples = List[NameType]()
		tuples
	}

	override def toString: String = {
		super.toString + "\n" +
			"TableTuple{" + "tuples=" + tuples.map(_ + "\n").reduce(_ + "\n" + _)
		+'}'
	}
}

class Table(val name: String, val records: TableTuple) {
	override def toString: String = "Table{" + "name='" + name + '\'' + ", records=" + records + '}'
}
