package com.ericsson.ema.tim.pojo

import javassist.{CannotCompileException, NotFoundException}

import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by eqinson on 2017/5/8.
  */
object PojoGenerator {
	private[this] val LOGGER = LoggerFactory.getLogger(PojoGenerator.getClass)
	private[this] val typesForTuple = Map("int" -> classOf[Integer], "string" -> classOf[String], "bool" -> classOf[java.lang.Boolean])

	val pojoPkg: String = PojoGenerator.getClass.getPackage.getName

	private[this] def generateTupleClz(table: Table): Unit = {
		val props = table.records.tuples.foldLeft(mutable.LinkedHashMap[String, Class[_]]())((m, n) => {
			m.put(n.theName, typesForTuple.getOrElse(n.theType, throw new RuntimeException("bug: illegal typesForTuple")))
			m
		})

		if (LOGGER.isDebugEnabled)
			props.foreach(kv => LOGGER.debug("name: {}, type: {}", kv._1, kv._2: Any))

		val classToGen = pojoPkg + "." + table.name + "Data"
		try {
			val clz = PojoGenUtil.generatePojo(classToGen, props)
			Thread.currentThread.setContextClassLoader(clz.getClassLoader)
		}
		catch {
			case e@(_: NotFoundException | _: CannotCompileException) =>
				LOGGER.error(e.getMessage)
				throw new RuntimeException(e)
		}
	}

	def generateTableClz(table: Table): Unit = {
		generateTupleClz(table)
		val props = Map("records" -> classOf[java.util.List[_]])
		val classTOGen: String = pojoPkg + "." + table.name
		try
			PojoGenUtil.generateListPojo(classTOGen, props)
		catch {
			case e@(_: NotFoundException | _: CannotCompileException) =>
				LOGGER.error(e.getMessage)
				throw new RuntimeException(e)
		}
	}
}
