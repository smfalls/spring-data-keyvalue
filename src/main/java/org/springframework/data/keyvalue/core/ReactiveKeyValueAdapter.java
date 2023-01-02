package org.springframework.data.keyvalue.core;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.util.CloseableIterator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;

public interface ReactiveKeyValueAdapter extends DisposableBean {

    /**
     * Add object with given id to keyspace.
     *
     * @param id must not be {@literal null}.
     * @param keyspace must not be {@literal null}.
     * @return the item previously associated with the id.
     */
    Mono<Object> put(Object id, Object item, String keyspace);

    /**
     * Check if a object with given id exists in keyspace.
     *
     * @param id must not be {@literal null}.
     * @param keyspace must not be {@literal null}.
     * @return true if item of type with id exists.
     */
    Mono<Boolean> contains(Object id, String keyspace);

    /**
     * Get the object with given id from keyspace.
     *
     * @param id must not be {@literal null}.
     * @param keyspace must not be {@literal null}.
     * @return {@literal null} in case no matching item exists.
     */
    Mono<Object> get(Object id, String keyspace);

    /**
     * Get the object with given id from keyspace.
     *
     * @param id must not be {@literal null}.
     * @param keyspace must not be {@literal null}.
     * @param type must not be {@literal null}.
     * @return {@literal null} in case no matching item exists.
     * @since 1.1
     */
    <T> Mono<T> get(Object id, String keyspace, Class<T> type);

    /**
     * Delete and return the object with given type and id.
     *
     * @param id must not be {@literal null}.
     * @param keyspace must not be {@literal null}.
     * @return {@literal null} if object could not be found
     */
    Mono<Object> delete(Object id, String keyspace);

    /**
     * Delete and return the object with given type and id.
     *
     * @param id must not be {@literal null}.
     * @param keyspace must not be {@literal null}.
     * @param type must not be {@literal null}.
     * @return {@literal null} if object could not be found
     * @since 1.1
     */
    <T> Mono<T> delete(Object id, String keyspace, Class<T> type);

    /**
     * Get all elements for given keyspace.
     *
     * @param keyspace must not be {@literal null}.
     * @return empty {@link Collection} if nothing found.
     */
    Flux<?> getAllOf(String keyspace);

    /**
     * Get all elements for given keyspace.
     *
     * @param keyspace must not be {@literal null}.
     * @param type must not be {@literal null}.
     * @return empty {@link Collection} if nothing found.
     * @since 2.5
     */
    @SuppressWarnings("unchecked")
    default <T> Flux<T> getAllOf(String keyspace, Class<T> type) {
        return (Flux<T>) getAllOf(keyspace);
    }

    /**
     * Returns a {@link CloseableIterator} that iterates over all entries.
     *
     * @param keyspace must not be {@literal null}.
     * @return
     */
    Flux<Map.Entry<Object, Object>> entries(String keyspace);

    /**
     * Returns a {@link CloseableIterator} that iterates over all entries.
     *
     * @param keyspace must not be {@literal null}.
     * @param type must not be {@literal null}.
     * @return
     * @since 2.5
     */
    @SuppressWarnings("unchecked")
    default <T> Flux<Map.Entry<Object, T>> entries(String keyspace, Class<T> type) {
        return (Flux) entries(keyspace);
    }

    /**
     * Remove all objects of given type.
     *
     * @param keyspace must not be {@literal null}.
     */
    Mono<Void> deleteAllOf(String keyspace);

    /**
     * Removes all objects.
     */
    void clear();

    /**
     * Find all matching objects within {@literal keyspace}.
     *
     * @param query must not be {@literal null}.
     * @param keyspace must not be {@literal null}.
     * @return empty {@link Collection} if no match found.
     */
    default Flux<?> find(KeyValueQuery<?> query, String keyspace) {
        return find(query, keyspace, Object.class);
    }

    /**
     * @param query must not be {@literal null}.
     * @param keyspace must not be {@literal null}.
     * @param type must not be {@literal null}.
     * @return empty {@link Collection} if no match found.
     * @since 1.1
     */
    <T> Flux<T> find(KeyValueQuery<?> query, String keyspace, Class<T> type);

    /**
     * Count number of objects within {@literal keyspace}.
     *
     * @param keyspace must not be {@literal null}.
     * @return
     */
    Mono<Long> count(String keyspace);

    /**
     * Count all matching objects within {@literal keyspace}.
     *
     * @param query must not be {@literal null}.
     * @param keyspace must not be {@literal null}.
     * @return
     */
    Mono<Long> count(KeyValueQuery<?> query, String keyspace);
}
