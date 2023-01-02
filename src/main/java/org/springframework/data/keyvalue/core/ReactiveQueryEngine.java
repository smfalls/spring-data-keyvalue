package org.springframework.data.keyvalue.core;

import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.lang.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;

public abstract class ReactiveQueryEngine<ADAPTER extends ReactiveKeyValueAdapter, CRITERIA, SORT> {

    private final Optional<CriteriaAccessor<CRITERIA>> criteriaAccessor;
    private final Optional<SortAccessor<SORT>> sortAccessor;

    private @Nullable ADAPTER adapter;

    public ReactiveQueryEngine(@Nullable CriteriaAccessor<CRITERIA> criteriaAccessor, @Nullable SortAccessor<SORT> sortAccessor) {

        this.criteriaAccessor = Optional.ofNullable(criteriaAccessor);
        this.sortAccessor = Optional.ofNullable(sortAccessor);
    }

    /**
     * Extract query attributes and delegate to concrete execution.
     *
     * @param query
     * @param keyspace
     * @return
     */
    public Flux<?> execute(KeyValueQuery<?> query, String keyspace) {

        CRITERIA criteria = this.criteriaAccessor.map(it -> it.resolve(query)).orElse(null);
        SORT sort = this.sortAccessor.map(it -> it.resolve(query)).orElse(null);

        return execute(criteria, sort, query.getOffset(), query.getRows(), keyspace);
    }

    /**
     * Extract query attributes and delegate to concrete execution.
     *
     * @param query
     * @param keyspace
     * @return
     */
    public <T> Flux<T> execute(KeyValueQuery<?> query, String keyspace, Class<T> type) {

        CRITERIA criteria = this.criteriaAccessor.map(it -> it.resolve(query)).orElse(null);
        SORT sort = this.sortAccessor.map(it -> it.resolve(query)).orElse(null);

        return execute(criteria, sort, query.getOffset(), query.getRows(), keyspace, type);
    }

    /**
     * Extract query attributes and delegate to concrete execution.
     *
     * @param query
     * @param keyspace
     * @return
     */
    public Mono<Long> count(KeyValueQuery<?> query, String keyspace) {

        CRITERIA criteria = this.criteriaAccessor.map(it -> it.resolve(query)).orElse(null);
        return count(criteria, keyspace);
    }

    /**
     * @param criteria
     * @param sort
     * @param offset
     * @param rows
     * @param keyspace
     * @return
     */
    public abstract Flux<?> execute(@Nullable CRITERIA criteria, @Nullable SORT sort, long offset, int rows,
                                    String keyspace);

    /**
     * @param criteria
     * @param sort
     * @param offset
     * @param rows
     * @param keyspace
     * @param type
     * @return
     * @since 1.1
     */
    @SuppressWarnings("unchecked")
    public <T> Flux<T> execute(@Nullable CRITERIA criteria, @Nullable SORT sort, long offset, int rows,
                                     String keyspace, Class<T> type) {
        return execute(criteria, sort, offset, rows, keyspace).cast(type);
    }

    /**
     * @param criteria
     * @param keyspace
     * @return
     */
    public abstract Mono<Long> count(@Nullable CRITERIA criteria, String keyspace);

    /**
     * Get the {@link ReactiveKeyValueAdapter} used.
     *
     * @return
     */
    @Nullable
    protected ADAPTER getAdapter() {
        return this.adapter;
    }

    /**
     * Get the required {@link ReactiveKeyValueAdapter} used or throw {@link IllegalStateException} if the adapter is not set.
     *
     * @return the required {@link ReactiveKeyValueAdapter}.
     * @throws IllegalStateException if the adapter is not set.
     */
    protected ADAPTER getRequiredAdapter() {

        ADAPTER adapter = getAdapter();

        if (adapter != null) {
            return adapter;
        }

        throw new IllegalStateException("Required ReactiveKeyValueAdapter is not set!");
    }

    /**
     * @param adapter
     */
    @SuppressWarnings("unchecked")
    public void registerAdapter(ReactiveKeyValueAdapter adapter) {

        if (this.adapter == null) {
            this.adapter = (ADAPTER) adapter;
        } else {
            throw new IllegalArgumentException("Cannot register more than one adapter for this ReactiveQueryEngine.");
        }
    }
}
