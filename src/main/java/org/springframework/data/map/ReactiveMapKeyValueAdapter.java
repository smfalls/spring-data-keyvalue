/*
 * Copyright 2014-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.map;

import org.springframework.core.CollectionFactory;
import org.springframework.data.keyvalue.core.*;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link KeyValueAdapter} implementation for {@link Map}.
 *
 * @author Christoph Strobl
 * @author Derek Cochran
 * @author Marcel Overdijk
 */
public class ReactiveMapKeyValueAdapter extends AbstractReactiveKeyValueAdapter {

	@SuppressWarnings("rawtypes") //
	private final Class<? extends Map> keySpaceMapType;
	private final Map<String, Map<Object, Object>> store;

	/**
	 * Create new {@link ReactiveMapKeyValueAdapter} using {@link ConcurrentHashMap} as backing store type.
	 */
	public ReactiveMapKeyValueAdapter() {
		this(ConcurrentHashMap.class);
	}

	/**
	 * Create new {@link ReactiveMapKeyValueAdapter} using the given query engine.
	 *
	 * @param engine the query engine.
	 * @since 2.4
	 */
	public ReactiveMapKeyValueAdapter(ReactiveQueryEngine<? extends ReactiveKeyValueAdapter, ?, ?> engine) {
		this(ConcurrentHashMap.class, engine);
	}

	/**
	 * Creates a new {@link ReactiveMapKeyValueAdapter} using the given {@link Map} as backing store.
	 *
	 * @param mapType must not be {@literal null}.
	 */
	@SuppressWarnings("rawtypes")
	public ReactiveMapKeyValueAdapter(Class<? extends Map> mapType) {
		this(CollectionFactory.createMap(mapType, 100), mapType, null);
	}

	/**
	 * Creates a new {@link ReactiveMapKeyValueAdapter} using the given {@link Map} as backing store and query engine.
	 *
	 * @param mapType must not be {@literal null}.
	 * @param engine the query engine.
	 * @since 2.4
	 */
	@SuppressWarnings("rawtypes")
	public ReactiveMapKeyValueAdapter(Class<? extends Map> mapType, ReactiveQueryEngine<? extends ReactiveKeyValueAdapter, ?, ?> engine) {
		this(CollectionFactory.createMap(mapType, 100), mapType, engine);
	}

	/**
	 * Create new instance of {@link ReactiveMapKeyValueAdapter} using given dataStore for persistence.
	 *
	 * @param store must not be {@literal null}.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ReactiveMapKeyValueAdapter(Map<String, Map<Object, Object>> store) {
		this(store, (Class<? extends Map>) ClassUtils.getUserClass(store), null);
	}

	/**
	 * Create new instance of {@link ReactiveMapKeyValueAdapter} using given dataStore for persistence and query engine.
	 *
	 * @param store must not be {@literal null}.
	 * @param engine the query engine.
	 * @since 2.4
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ReactiveMapKeyValueAdapter(Map<String, Map<Object, Object>> store, ReactiveQueryEngine<? extends ReactiveKeyValueAdapter, ?, ?> engine) {
		this(store, (Class<? extends Map>) ClassUtils.getUserClass(store), engine);
	}

	/**
	 * Creates a new {@link ReactiveMapKeyValueAdapter} with the given store and type to be used when creating key spaces and
	 * query engine.
	 *
	 * @param store must not be {@literal null}.
	 * @param keySpaceMapType must not be {@literal null}.
	 * @param engine the query engine.
	 */
	@SuppressWarnings("rawtypes")
	private ReactiveMapKeyValueAdapter(Map<String, Map<Object, Object>> store, Class<? extends Map> keySpaceMapType, ReactiveQueryEngine<? extends ReactiveKeyValueAdapter, ?, ?> engine) {

		super(engine);

		Assert.notNull(store, "Store must not be null");
		Assert.notNull(keySpaceMapType, "Map type to be used for key spaces must not be null");

		this.store = store;
		this.keySpaceMapType = keySpaceMapType;
	}

	@Override
	public Mono<Object> put(Object id, Object item, String keyspace) {

		Assert.notNull(id, "Cannot add item with null id");
		Assert.notNull(keyspace, "Cannot add item for null collection");

		return Mono.fromSupplier(() -> getKeySpaceMap(keyspace).put(id, item));
	}

	@Override
	public Mono<Boolean> contains(Object id, String keyspace) {
		return get(id, keyspace)
				.map(o -> Boolean.TRUE)
				.switchIfEmpty(Mono.just(Boolean.FALSE));
	}

	@Override
	public Mono<Long> count(String keyspace) {
		return Mono.fromSupplier(() -> (long) getKeySpaceMap(keyspace).size());
	}

	@Override
	public Mono<Object> get(Object id, String keyspace) {

		Assert.notNull(id, "Cannot get item with null id");
		return Mono.fromSupplier(() -> getKeySpaceMap(keyspace).get(id));
	}

	@Override
	public Mono<Object> delete(Object id, String keyspace) {

		Assert.notNull(id, "Cannot delete item with null id");
		return Mono.fromSupplier(() -> getKeySpaceMap(keyspace).remove(id));
	}

	@Override
	public Flux<?> getAllOf(String keyspace) {
		return Mono.fromSupplier(() -> getKeySpaceMap(keyspace).values()).flatMapMany(Flux::fromIterable);
	}

	@Override
	public Flux<Entry<Object, Object>> entries(String keyspace) {
		return Mono.fromSupplier(() -> getKeySpaceMap(keyspace).entrySet()).flatMapMany(Flux::fromIterable);
	}

	@Override
	public Mono<Void> deleteAllOf(String keyspace) {
		return Mono.fromRunnable(() -> getKeySpaceMap(keyspace).clear());
	}

	@Override
	public void clear() {
		store.clear();
	}

	@Override
	public void destroy() throws Exception {
		clear();
	}

	/**
	 * Get map associated with given key space.
	 *
	 * @param keyspace must not be {@literal null}.
	 * @return
	 */
	protected Map<Object, Object> getKeySpaceMap(String keyspace) {

		Assert.notNull(keyspace, "Collection must not be null for lookup");
		return store.computeIfAbsent(keyspace, k -> CollectionFactory.createMap(keySpaceMapType,  1000));
	}

}
