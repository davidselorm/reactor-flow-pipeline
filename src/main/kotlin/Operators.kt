package com.devpulse.reactor

fun <T, R> SimpleFlow<T>.map(transform: (T) -> R): SimpleFlow<R> = SimpleFlow { sub ->
    subscribe(object : FlowSubscriber<T> {
        override fun onNext(item: T) = sub.onNext(transform(item))
        override fun onError(t: Throwable) = sub.onError(t)
        override fun onComplete() = sub.onComplete()
    })
}
