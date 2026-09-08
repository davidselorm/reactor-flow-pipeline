package com.reactor.flow

fun <T> FlowPipeline<T>.distinctUntilChanged(): FlowPipeline<T> {
    return FlowPipeline.fromIterable(this.toList().distinct())
}
