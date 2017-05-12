package com.ericsson.ema.tim.reflection

import java.beans.{IntrospectionException, Introspector}
import java.lang.reflect.Method
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock

import com.ericsson.ema.tim.exception.DmlNoSuchFieldException
import org.slf4j.LoggerFactory

/**
  * Created by eqinson on 2017/5/5.
  */
class MethodInvocationCache {
	private val LOGGER = LoggerFactory.getLogger(classOf[MethodInvocationCache])

	private val getterStore = new ConcurrentHashMap[MethodInvocationKey, Method]
	private val setterStore = new ConcurrentHashMap[MethodInvocationKey, Method]
	private val lock = new ReentrantLock


	def cleanup(): Unit = {
		getterStore.clear()
		setterStore.clear()
	}

	private def lookup(clz: Class[_], property: String): Method = {
		val beanInfo = Introspector.getBeanInfo(clz)
		beanInfo.getPropertyDescriptors.toList.filter(property == _.getName).map(_.getReadMethod) match {
			case h :: _ => h
			case _      => throw DmlNoSuchFieldException(property)
		}
	}

	def get(clz: Class[_], field: String, accessType: AccessType.AccessType): Method = {
		val key = new MethodInvocationKey(clz, field)
		val store = if (accessType == AccessType.GET) getterStore else setterStore
		var cached = store.get(key)
		if (cached == null) {
			lock.lock()
			try {
				cached = store.get(key)
				if (cached == null) {
					try
						cached = lookup(clz, field)
					catch {
						case e: IntrospectionException =>
							LOGGER.error(e.getMessage)
							throw new RuntimeException(e)
					}
					store.put(key, cached)
				}
			} finally {
				lock.unlock()
			}
		}
		cached
	}

	class MethodInvocationKey private() {
		private[MethodInvocationKey] var hashcode: Int = _
		private[MethodInvocationKey] var lookupClass: Class[_] = _
		private[MethodInvocationKey] var methodName: String = _

		def this(lookupClass: Class[_], methodName: String) = {
			this()
			this.lookupClass = lookupClass
			this.methodName = methodName
			var result = if (lookupClass != null) lookupClass.hashCode else 0
			result = 31 * result + (if (methodName != null) methodName.hashCode else 0)
			this.hashcode = result
		}

		override def equals(o: Any): Boolean = {
			o match {
				case that: MethodInvocationKey => (this eq that) ||
					(lookupClass eq that.lookupClass) && (methodName == that.methodName)
				case _                         => false
			}
		}

		override def hashCode(): Int = this.hashcode
	}

}

object AccessType extends Enumeration {
	type AccessType = Value
	val GET, SET = Value
}