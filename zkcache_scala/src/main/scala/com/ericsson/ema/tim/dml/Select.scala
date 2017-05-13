package com.ericsson.ema.tim.dml


import java.lang.reflect.InvocationTargetException

import com.ericsson.ema.tim.dml.group.GroupBy
import com.ericsson.ema.tim.dml.order.{ChainableOrderings, OrderBy}
import com.ericsson.ema.tim.dml.predicate.Predicate
import com.ericsson.ema.tim.exception.DmlBadSyntaxException
import com.ericsson.ema.tim.lock.ZKCacheRWLockMap.zkCacheRWLock
import com.ericsson.ema.tim.reflection.{AccessType, MethodInvocationCache, Tab2MethodInvocationCacheMap}

/**
  * Created by eqinson on 2017/5/12.
  */
object Select {
	def apply() = new Select

	def apply(fields: String*) = new Select(fields: _*)
}


class Select private() extends Selector with ChainableOrderings {
	private val TUPLE_FIELD: String = "records"
	var selectedFields: List[String] = List[String]()
	var context: TableInfoContext = _
	var methodInvocationCache: MethodInvocationCache = _
	private var predicates = List[Predicate]()
	private var orderBys = List[OrderBy]()
	private var groupBy: GroupBy = _
	private var limit = Integer.MIN_VALUE
	private var skip = Integer.MIN_VALUE
	private var table: String = _
	private var records: List[Object] = _

	def this(fields: String*) {
		this()
		this.selectedFields = if (fields.nonEmpty) fields.toList else selectedFields
	}

	override def from(tab: String): Selector = {
		this.table = tab
		this
	}

	override def where(predicate: Predicate): Selector = {
		this.predicates :+= predicate
		predicate.asInstanceOf[SelectClause].selector = this
		this
	}

	override def limit(limit: Int): Selector = {
		if (limit <= 0) throw DmlBadSyntaxException("Error: limit must be > 0")
		this.limit = limit
		this
	}

	override def skip(skip: Int): Selector = {
		if (skip <= 0) throw DmlBadSyntaxException("Error: skip must be > 0")
		this.skip = skip
		this
	}

	override def collect(): List[Object] = {
		if (selectedFields.nonEmpty)
			throw DmlBadSyntaxException("Error: must use collectBySelectFields if some fields are to be selected")

		internalExecute()
	}

	override def collectBySelectFields(): List[List[Object]] = {
		if (selectedFields.isEmpty)
			throw DmlBadSyntaxException("Error: Must use execute if full fields are to be selected")

		for (obj <- internalExecute())
			yield selectedFields.map(invokeGetByReflection(obj, _)).foldRight(List[Object]())(_ :: _)
	}

	/**
	  * rwlock table when internalExecute
	  *
	  * @return List of tuple
	  */
	private def internalExecute(): List[Object] = {
		zkCacheRWLock.readLockTable(table)
		try {
			initExecuteContext()
			var result = records
			if (predicates.nonEmpty)
				result = records.filter(internalPredicate())
			if (orderBys.nonEmpty)
				result = result.sorted(orderBys.map(_.ordering()).reduce(_ thenOrdering _))
			if (skip > 0)
				result = result.drop(skip)
			if (limit > 0)
				result = result.take(limit)
			result
		} finally {
			zkCacheRWLock.readUnLockTable(table)
		}
	}

	private def initExecuteContext(): Unit = {
		this.context = TableInfoMap.tableInfoMap.lookup(table).getOrElse(throw DmlBadSyntaxException("Error: Selecting a " + "non-existing table:" + table))
		this.methodInvocationCache = Tab2MethodInvocationCacheMap.tab2MethodInvocationCacheMap.lookup(table)
		//it is safe because records must be List according to JavaBean definition
		val tupleField = invokeGetByReflection(context.tabledata, TUPLE_FIELD)
		assert(tupleField.isInstanceOf[java.util.List[Object]])
		import scala.collection.JavaConversions._
		this.records = tupleField.asInstanceOf[java.util.List[Object]].toList
	}

	private def invokeGetByReflection(obj: Object, wantedField: String): Object = {
		val getter = methodInvocationCache.get(obj.getClass, wantedField, AccessType.GET)
		try
			getter.invoke(obj)
		catch {
			case e@(_: IllegalAccessException | _: InvocationTargetException) =>
				throw DmlBadSyntaxException(e.getMessage) //should never happen
		}
	}

	private def internalPredicate(): Object => Boolean = {
		r: Object => predicates.map(_.eval(r)).reduce(_ && _)
	}

	override def count(): Long = {
		if (limit != Integer.MIN_VALUE || skip != Integer.MIN_VALUE)
			throw DmlBadSyntaxException("Error: meaningless to specify skip/limit in count.")

		zkCacheRWLock.readLockTable(table)
		try {
			initExecuteContext()
			records.count(internalPredicate())
		} finally {
			zkCacheRWLock.readUnLockTable(table)
		}
	}

	override def exists(): Boolean = {
		if (limit != Integer.MIN_VALUE || skip != Integer.MIN_VALUE)
			throw DmlBadSyntaxException("Error: meaningless to specify skip/limit in exists.")

		zkCacheRWLock.readLockTable(table)
		try {
			initExecuteContext()
			records.exists(internalPredicate())
		} finally {
			zkCacheRWLock.readUnLockTable(table)
		}
	}

	override def orderBy(field: String, asc: String = "asc"): Selector = {
		val o = OrderBy(field, asc)
		this.orderBys :+= o
		o.selector = this
		this
	}

	override def groupBy(field: String): Selector = {
		if (groupBy != null)
			throw DmlBadSyntaxException("Error: only support one groupBy Clause")

		val g = new GroupBy(field)
		this.groupBy = g
		g.selector = this
		this
	}

	override def collectByGroup(): Map[Object, List[Object]] = {
		zkCacheRWLock.readLockTable(table)
		try {
			initExecuteContext()
			var result = records
			if (predicates.nonEmpty)
				result = records.filter(internalPredicate())
			if (orderBys.nonEmpty)
				result = result.sorted(orderBys.map(_.ordering()).reduce(_ thenOrdering _))
			if (skip > 0)
				result = result.drop(skip)
			if (limit > 0)
				result = result.take(limit)
			if (groupBy != null)
				result.groupBy(groupBy.keyExtractor())
			else throw DmlBadSyntaxException("Error: must specify groupBy when using collectByGroup.")
		} finally {
			zkCacheRWLock.readUnLockTable(table)
		}
	}
}



