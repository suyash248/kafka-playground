package org.github.message.consumers.processors;

@FunctionalInterface
public interface ErrorHandler<V> {
    void onError(V records,  Exception e);
}
