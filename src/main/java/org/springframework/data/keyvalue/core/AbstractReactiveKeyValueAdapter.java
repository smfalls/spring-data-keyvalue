package org.springframework.data.keyvalue.core;

import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.lang.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public abstract class AbstractReactiveKeyValueAdapter implements ReactiveKeyValueAdapter {

    private final ReactiveQueryEngine<? extends ReactiveKeyValueAdapter, ?, ?> engine;

    /**
     * Creates new {@link AbstractKeyValueAdapter} with using the default query engine.
     */
    protected AbstractReactiveKeyValueAdapter() {
        this(null);
    }

    /**
     * Creates new {@link AbstractKeyValueAdapter} with using the default query engine.
     *
     * @param engine will be defaulted to {@link SpelQueryEngine} if {@literal null}.
     */
    protected AbstractReactiveKeyValueAdapter(@Nullable ReactiveQueryEngine<? extends ReactiveKeyValueAdapter, ?, ?> engine) {

        this.engine = engine != null ? engine : new ReactiveSpelQueryEngine();
        this.engine.registerAdapter(this);
    }

    /**
     * Get the {@link QueryEngine} used.
     *
     * @return
     */
    protected ReactiveQueryEngine<? extends ReactiveKeyValueAdapter, ?, ?> getQueryEngine() {
        return engine;
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueAdapter#get(java.lang.Object, java.lang.String, java.lang.Class)
     */
    @Override
    public <T> Mono<T> get(Object id, String keyspace, Class<T> type) {
        return get(id, keyspace).cast(type);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueAdapter#delete(java.lang.Object, java.lang.String, java.lang.Class)
     */
    @Override
    public <T> Mono<T> delete(Object id, String keyspace, Class<T> type) {
        return delete(id, keyspace).cast(type);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueAdapter#find(org.springframework.data.keyvalue.core.query.KeyValueQuery, java.lang.String, java.lang.Class)
     */
    @Override
    public <T> Flux<T> find(KeyValueQuery<?> query, String keyspace, Class<T> type) {
        return engine.execute(query, keyspace, type);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueAdapter#find(org.springframework.data.keyvalue.core.query.KeyValueQuery, java.lang.String)
     */
    @Override
    public Flux<?> find(KeyValueQuery<?> query, String keyspace) {
        return engine.execute(query, keyspace);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueAdapter#count(org.springframework.data.keyvalue.core.query.KeyValueQuery, java.lang.String)
     */
    @Override
    public Mono<Long> count(KeyValueQuery<?> query, String keyspace) {
        return engine.count(query, keyspace);
    }

}
