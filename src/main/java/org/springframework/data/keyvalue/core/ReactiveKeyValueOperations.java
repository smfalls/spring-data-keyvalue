package org.springframework.data.keyvalue.core;

import org.reactivestreams.Publisher;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.annotation.KeySpace;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.mapping.context.MappingContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;

public interface ReactiveKeyValueOperations extends DisposableBean {

    /**
     * Add given object. Object needs to have id property to which a generated value will be assigned.
     *
     * @param objectToInsert
     * @return the inserted object.
     */
    <T> Mono<T> insert(T objectToInsert);

    /**
     * Add object with given id.
     *
     * @param id must not be {@literal null}.
     * @param objectToInsert must not be {@literal null}.
     * @return the inserted object.
     */
    <T> Mono<T> insert(Object id, T objectToInsert);

    /**
     * Get all elements of given type. Respects {@link KeySpace} if present and therefore returns all elements that can be
     * assigned to requested type.
     *
     * @param type must not be {@literal null}.
     * @return empty iterable if no elements found.
     */
    <T> Flux<T> findAll(Class<T> type);

    /**
     * Get all elements ordered by sort. Respects {@link KeySpace} if present and therefore returns all elements that can
     * be assigned to requested type.
     *
     * @param sort must not be {@literal null}.
     * @param type must not be {@literal null}.
     * @return
     */
    <T> Flux<T> findAll(Sort sort, Class<T> type);

    /**
     * Get element of given type with given id. Respects {@link KeySpace} if present and therefore returns all elements
     * that can be assigned to requested type.
     *
     * @param id must not be {@literal null}.
     * @param type must not be {@literal null}.
     * @return {@link Optional#empty()} if not found.
     */
    <T> Mono<T> findById(Object id, Class<T> type);

    /**
     * Execute operation against underlying store.
     *
     * @param action must not be {@literal null}.
     * @return
     */
    <T> Publisher<T> execute(ReactiveKeyValueCallback<T> action);

    /**
     * Get all elements matching the given query. <br />
     * Respects {@link KeySpace} if present and therefore returns all elements that can be assigned to requested type..
     *
     * @param query must not be {@literal null}.
     * @param type must not be {@literal null}.
     * @return empty iterable if no match found.
     */
    <T> Flux<T> find(KeyValueQuery<?> query, Class<T> type);

    /**
     * Get all elements in given range. Respects {@link KeySpace} if present and therefore returns all elements that can
     * be assigned to requested type.
     *
     * @param offset
     * @param rows
     * @param type must not be {@literal null}.
     * @return
     */
    <T> Flux<T> findInRange(long offset, int rows, Class<T> type);

    /**
     * Get all elements in given range ordered by sort. Respects {@link KeySpace} if present and therefore returns all
     * elements that can be assigned to requested type.
     *
     * @param offset
     * @param rows
     * @param sort
     * @param type
     * @return
     */
    <T> Flux<T> findInRange(long offset, int rows, Sort sort, Class<T> type);

    /**
     * @param objectToUpdate must not be {@literal null}.
     * @return the updated object.
     */
    <T> Mono<T> update(T objectToUpdate);

    /**
     * @param id must not be {@literal null}.
     * @param objectToUpdate must not be {@literal null}.
     * @return the updated object.
     */
    <T> Mono<T> update(Object id, T objectToUpdate);

    /**
     * Remove all elements of type. Respects {@link KeySpace} if present and therefore removes all elements that can be
     * assigned to requested type.
     *
     * @param type must not be {@literal null}.
     */
    Mono<Void> delete(Class<?> type);

    /**
     * @param objectToDelete must not be {@literal null}.
     * @return
     */
    <T> Mono<T> delete(T objectToDelete);

    /**
     * Delete item of type with given id.
     *
     * @param id must not be {@literal null}.
     * @param type must not be {@literal null}.
     * @return the deleted item or {@literal null} if no match found.
     */
    <T> Mono<T> delete(Object id, Class<T> type);

    /**
     * Total number of elements with given type available. Respects {@link KeySpace} if present and therefore counts all
     * elements that can be assigned to requested type.
     *
     * @param type must not be {@literal null}.
     * @return
     */
    Mono<Long> count(Class<?> type);

    /**
     * Total number of elements matching given query. Respects {@link KeySpace} if present and therefore counts all
     * elements that can be assigned to requested type.
     *
     * @param query
     * @param type
     * @return
     */
    Mono<Long> count(KeyValueQuery<?> query, Class<?> type);

    /**
     * @return mapping context in use.
     */
    MappingContext<?, ?> getMappingContext();

}
