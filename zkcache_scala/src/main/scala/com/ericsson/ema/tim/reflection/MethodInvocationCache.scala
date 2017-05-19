package com.ericsson.ema.tim.reflection

import java.beans.Introspector
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
		Option(store.get(key)) match {
			case Some(cached) => cached
			case None         =>
				lock.lock()
				try {
					val cached = lookup(clz, field)
					store.put(key, cached)
					cached
				}
				finally {
					lock.unlock()
				}
		}
	}

	class MethodInvocationKey private() {
		private var hashcode: Int = _
		private var lookupClass: Class[_] = _
		private var methodName: String = _

		def this(lookupClass: Class[_], methodName: String) = {
			this()
			this.lookupClass = lookupClass
			this.methodName = methodName

			var result = Option(lookupClass) match {
				case Some(clz) => clz.hashCode()
				case None      => 0
			}
			result = 31 * result + {
				Option(methodName) match {
					case Some(m) => m.hashCode()
					case None    => 0
				}
			}
			this.hashcode = result
		}

		override def equals(o: Any): Boolean = {
			o match {
				case that: MethodInvocationKey => (this eq that) ||
					(lookupClass == that.lookupClass) && (methodName == that.methodName)
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