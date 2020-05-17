package com.soni.message.consumers;

import java.util.List;

@FunctionalInterface
public interface MessageProcessor<V> {
    void process(List<V> recordList);
}
