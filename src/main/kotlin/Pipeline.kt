package com.reactor.flow

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

class FlowPipeline<T> private constructor(private val upstream: () -> Iterator<T>) {

    fun <R> map(transform: (T) -> R): FlowPipeline<R> {
        return FlowPipeline {
            val iter = upstream()
            object : Iterator<R> {
                override fun hasNext() = iter.hasNext()
                override fun next() = transform(iter.next())
            }
        }
    }

    fun filter(predicate: (T) -> Boolean): FlowPipeline<T> {
        return FlowPipeline {
            val iter = upstream()
            object : Iterator<T> {
                private var nextItem: T? = null
                private var hasNextItem = false

                override fun hasNext(): Boolean {
                    while (!hasNextItem && iter.hasNext()) {
                        val item = iter.next()
                        if (predicate(item)) {
                            nextItem = item
                            hasNextItem = true
                        }
                    }
                    return hasNextItem
                }

                override fun next(): T {
                    if (!hasNext()) throw NoSuchElementException()
                    hasNextItem = false
                    val ret = nextItem!!
                    nextItem = null
                    return ret
                }
            }
        }
    }

    fun toList(): List<T> {
        val list = mutableListOf<T>()
        val iter = upstream()
        while (iter.hasNext()) {
            list.add(iter.next())
        }
        return list
    }

    companion object {
        fun <T> fromIterable(items: Iterable<T>): FlowPipeline<T> {
            return FlowPipeline { items.iterator() }
        }
    }
}
