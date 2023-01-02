package org.springframework.data.keyvalue.core;

import reactor.core.publisher.Flux;

public interface ReactiveKeyValueCallback<T> {

    /**
     * Gets called by {@code ReactiveKeyValueTemplate#execute(ReactiveKeyValueCallback)}. Allows for returning a result object created
     * within the callback, i.e. a domain object or a collection of domain objects.
     *
     * @param adapter
     * @return
     */
    Flux<T> doInKeyValue(ReactiveKeyValueAdapter adapter);
}
