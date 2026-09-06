package com.devpulse.reactor

interface FlowSubscriber<T> {
    fun onNext(item: T)
    fun onError(t: Throwable)
    fun onComplete()
}

class SimpleFlow<T>(private val emitter: (FlowSubscriber<T>) -> Unit) {
    fun subscribe(subscriber: FlowSubscriber<T>) = emitter(subscriber)
}
