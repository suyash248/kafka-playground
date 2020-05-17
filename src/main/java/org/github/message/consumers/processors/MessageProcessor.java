package org.github.message.consumers.processors;

import java.util.List;

@FunctionalInterface
public interface MessageProcessor<V> {
    void process(List<V> recordList);
}
