package org.springframework.data.keyvalue.core;

import org.reactivestreams.Publisher;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.event.KeyValueEvent;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentEntity;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentProperty;
import org.springframework.data.keyvalue.core.mapping.context.KeyValueMappingContext;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public class ReactiveKeyValueTemplate implements ReactiveKeyValueOperations, ApplicationEventPublisherAware {

    private static final PersistenceExceptionTranslator DEFAULT_PERSISTENCE_EXCEPTION_TRANSLATOR = new KeyValuePersistenceExceptionTranslator();

    private final ReactiveKeyValueAdapter adapter;
    private final MappingContext<? extends KeyValuePersistentEntity<?, ?>, ? extends KeyValuePersistentProperty<?>> mappingContext;
    private final IdentifierGenerator identifierGenerator;

    private PersistenceExceptionTranslator exceptionTranslator = DEFAULT_PERSISTENCE_EXCEPTION_TRANSLATOR;
    private @Nullable ApplicationEventPublisher eventPublisher;
    private boolean publishEvents = true;
    private @SuppressWarnings("rawtypes") Set<Class<? extends KeyValueEvent>> eventTypesToPublish = Collections
            .emptySet();

    /**
     * Create new {@link KeyValueTemplate} using the given {@link KeyValueAdapter} with a default
     * {@link KeyValueMappingContext}.
     *
     * @param adapter must not be {@literal null}.
     */
    public ReactiveKeyValueTemplate(ReactiveKeyValueAdapter adapter) {
        this(adapter, new KeyValueMappingContext<>());
    }

    /**
     * Create new {@link KeyValueTemplate} using the given {@link KeyValueAdapter} and {@link MappingContext}.
     *
     * @param adapter must not be {@literal null}.
     * @param mappingContext must not be {@literal null}.
     */
    public ReactiveKeyValueTemplate(ReactiveKeyValueAdapter adapter,
                            MappingContext<? extends KeyValuePersistentEntity<?, ?>, ? extends KeyValuePersistentProperty<?>> mappingContext) {
        this(adapter, mappingContext, DefaultIdentifierGenerator.INSTANCE);
    }

    /**
     * Create new {@link KeyValueTemplate} using the given {@link KeyValueAdapter} and {@link MappingContext}.
     *
     * @param adapter must not be {@literal null}.
     * @param mappingContext must not be {@literal null}.
     * @param identifierGenerator must not be {@literal null}.
     * @since 2.4
     */
    public ReactiveKeyValueTemplate(ReactiveKeyValueAdapter adapter,
                            MappingContext<? extends KeyValuePersistentEntity<?, ?>, ? extends KeyValuePersistentProperty<?>> mappingContext,
                            IdentifierGenerator identifierGenerator) {

        Assert.notNull(adapter, "Adapter must not be null!");
        Assert.notNull(mappingContext, "MappingContext must not be null!");
        Assert.notNull(identifierGenerator, "IdentifierGenerator must not be null!");

        this.adapter = adapter;
        this.mappingContext = mappingContext;
        this.identifierGenerator = identifierGenerator;
    }

    /**
     * Set the {@link PersistenceExceptionTranslator} used for converting {@link RuntimeException}.
     *
     * @param exceptionTranslator must not be {@literal null}.
     */
    public void setExceptionTranslator(PersistenceExceptionTranslator exceptionTranslator) {

        Assert.notNull(exceptionTranslator, "ExceptionTranslator must not be null!");
        this.exceptionTranslator = exceptionTranslator;
    }

    /**
     * Define the event types to publish via {@link ApplicationEventPublisher}.
     *
     * @param eventTypesToPublish use {@literal null} or {@link Collections#emptySet()} to stop publishing.
     */
    @SuppressWarnings("rawtypes")
    public void setEventTypesToPublish(Set<Class<? extends KeyValueEvent>> eventTypesToPublish) {

        if (CollectionUtils.isEmpty(eventTypesToPublish)) {
            this.publishEvents = false;
        } else {
            this.publishEvents = true;
            this.eventTypesToPublish = Collections.unmodifiableSet(eventTypesToPublish);
        }
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.context.ApplicationEventPublisherAware#setApplicationEventPublisher(org.springframework.context.ApplicationEventPublisher)
     */
    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.eventPublisher = applicationEventPublisher;
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueOperations#insert(java.lang.Object)
     */
    @Override
    public <T> Mono<T> insert(T objectToInsert) {

        KeyValuePersistentEntity<?, ?> entity = getKeyValuePersistentEntity(objectToInsert);

        GeneratingIdAccessor generatingIdAccessor = new GeneratingIdAccessor(entity.getPropertyAccessor(objectToInsert),
                entity.getIdProperty(), identifierGenerator);
        Object id = generatingIdAccessor.getOrGenerateIdentifier();

        return insert(id, objectToInsert);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueOperations#insert(java.lang.Object, java.lang.Object)
     */
    @Override
    public <T> Mono<T> insert(Object id, T objectToInsert) {

        Assert.notNull(id, "Id for object to be inserted must not be null!");
        Assert.notNull(objectToInsert, "Object to be inserted must not be null!");

        String keyspace = resolveKeySpace(objectToInsert.getClass());

        return executeSingleOrEmpty(adapter -> potentiallyPublishEvent(KeyValueEvent.beforeInsert(id, keyspace, objectToInsert.getClass(), objectToInsert))
                        .then(adapter.contains(id, keyspace))
                        .flatMap(exists -> {
                            if(exists) {
                                return Mono.error(new DuplicateKeyException(
                                        String.format("Cannot insert existing object with id %s!. Please use update.", id)));
                            }
                            else {
                                return adapter.put(id, objectToInsert, keyspace);
                            }
                        })
                        .then(potentiallyPublishEvent(KeyValueEvent.afterInsert(id, keyspace, objectToInsert.getClass(), objectToInsert)))
                        .flux()
                )
                .thenReturn(objectToInsert);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueOperations#update(java.lang.Object)
     */
    @Override
    public <T> Mono<T> update(T objectToUpdate) {

        KeyValuePersistentEntity<?, ?> entity = getKeyValuePersistentEntity(objectToUpdate);

        if (!entity.hasIdProperty()) {
            return Mono.error(new InvalidDataAccessApiUsageException(
                    String.format("Cannot determine id for type %s", ClassUtils.getUserClass(objectToUpdate))));
        }

        return update(entity.getIdentifierAccessor(objectToUpdate).getRequiredIdentifier(), objectToUpdate);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueOperations#update(java.lang.Object, java.lang.Object)
     */
    @Override
    public <T> Mono<T> update(Object id, T objectToUpdate) {

        Assert.notNull(id, "Id for object to be inserted must not be null!");
        Assert.notNull(objectToUpdate, "Object to be updated must not be null!");

        String keyspace = resolveKeySpace(objectToUpdate.getClass());

        return potentiallyPublishEvent(KeyValueEvent.beforeUpdate(id, keyspace, objectToUpdate.getClass(), objectToUpdate))
                .then(executeSingleOrEmpty(adapter -> adapter.put(id, objectToUpdate, keyspace).flux()))
                .flatMap(existing -> potentiallyPublishEvent(KeyValueEvent.afterUpdate(id, keyspace, objectToUpdate.getClass(), objectToUpdate, existing)))
                .switchIfEmpty(potentiallyPublishEvent(KeyValueEvent.afterUpdate(id, keyspace, objectToUpdate.getClass(), objectToUpdate, null)))
                .thenReturn(objectToUpdate);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueOperations#findAllOf(java.lang.Class)
     */
    @Override
    public <T> Flux<T> findAll(Class<T> type) {

        Assert.notNull(type, "Type to fetch must not be null!");

        return execute(adapter -> adapter.getAllOf(resolveKeySpace(type), type)
                .filter(candidate -> typeCheck(type, candidate))
                .cast(type)
        );
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueOperations#findById(java.lang.Object, java.lang.Class)
     */
    @Override
    public <T> Mono<T> findById(Object id, Class<T> type) {

        Assert.notNull(id, "Id for object to be found must not be null!");
        Assert.notNull(type, "Type to fetch must not be null!");

        String keyspace = resolveKeySpace(type);

        return potentiallyPublishEvent(KeyValueEvent.beforeGet(id, keyspace, type))
                .then(executeSingleOrEmpty(adapter -> adapter.get(id, keyspace, type)
                            .filter(value -> typeCheck(type, value))
                            .cast(type)
                            .flux()
                ))
                .transform(configureToPotentiallyPublishEventAfter(result -> KeyValueEvent.afterGet(id, keyspace, type, result)));
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueOperations#delete(java.lang.Class)
     */
    @Override
    public Mono<Void> delete(Class<?> type) {

        Assert.notNull(type, "Type to delete must not be null!");

        String keyspace = resolveKeySpace(type);

        return potentiallyPublishEvent(KeyValueEvent.beforeDropKeySpace(keyspace, type))

                .then(executeSingleOrEmpty(adapter -> adapter.deleteAllOf(keyspace).flux()))

                .then(potentiallyPublishEvent(KeyValueEvent.afterDropKeySpace(keyspace, type)));
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueOperations#delete(java.lang.Object)
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> Mono<T> delete(T objectToDelete) {

        Class<T> type = (Class<T>) ClassUtils.getUserClass(objectToDelete);
        KeyValuePersistentEntity<?, ?> entity = getKeyValuePersistentEntity(objectToDelete);

        return delete(entity.getIdentifierAccessor(objectToDelete).getIdentifier(), type);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueOperations#delete(java.lang.Object, java.lang.Class)
     */
    @Override
    public <T> Mono<T> delete(Object id, Class<T> type) {

        Assert.notNull(id, "Id for object to be deleted must not be null!");
        Assert.notNull(type, "Type to delete must not be null!");

        String keyspace = resolveKeySpace(type);

        return potentiallyPublishEvent(KeyValueEvent.beforeDelete(id, keyspace, type))

                .then(executeSingleOrEmpty(adapter -> adapter.delete(id, keyspace, type).flux()))

                .transform(configureToPotentiallyPublishEventAfter(result -> KeyValueEvent.afterDelete(id, keyspace, type, result)));

    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueOperations#count(java.lang.Class)
     */
    @Override
    public Mono<Long> count(Class<?> type) {

        Assert.notNull(type, "Type for count must not be null!");
        return adapter.count(resolveKeySpace(type));
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueOperations#execute(org.springframework.data.keyvalue.core.ReactiveKeyValueCallback)
     */
    @Override
    public <T> Flux<T> execute(ReactiveKeyValueCallback<T> action) {

        Assert.notNull(action, "ReactiveKeyValueCallback must not be null!");

        return action.doInKeyValue(this.adapter)
                .onErrorMap(RuntimeException.class, this::resolveExceptionIfPossible);
    }

    protected <T> Mono<T> executeSingleOrEmpty(ReactiveKeyValueCallback<T> action) {
        return execute(action)
                .singleOrEmpty()
                .onErrorMap(IndexOutOfBoundsException.class, e -> new IllegalStateException(String.format("ReactiveKeyValueCallback %s returned multiple values!", action)));
    }

    /**
     * Execute {@link ReactiveKeyValueCallback} and require a non-{@literal null} return value.
     *
     * @param action
     * @param <T>
     * @return
     */
    protected <T> Mono<T> executeRequired(ReactiveKeyValueCallback<T> action) {
        return execute(action)
                .single()
                .onErrorMap(IndexOutOfBoundsException.class, e -> new IllegalStateException(String.format("ReactiveKeyValueCallback %s returned multiple values!", action)))
                .onErrorMap(NoSuchElementException.class, e -> new IllegalStateException(String.format("ReactiveKeyValueCallback %s returned null values!", action)));
    }



    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueOperations#find(org.springframework.data.keyvalue.core.query.KeyValueQuery, java.lang.Class)
     */
    @Override
    public <T> Flux<T> find(KeyValueQuery<?> query, Class<T> type) {

        return execute(adapter -> adapter.find(query, resolveKeySpace(type), type)
                    .filter(candidate -> typeCheck(type, candidate))
                    .cast(type)
        );
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueOperations#findAllOf(org.springframework.data.domain.Sort, java.lang.Class)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public <T> Flux<T> findAll(Sort sort, Class<T> type) {
        return find(new KeyValueQuery(sort), type);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueOperations#findInRange(long, int, java.lang.Class)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public <T> Flux<T> findInRange(long offset, int rows, Class<T> type) {
        return find(new KeyValueQuery().skip(offset).limit(rows), type);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueOperations#findInRange(long, int, org.springframework.data.domain.Sort, java.lang.Class)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public <T> Flux<T> findInRange(long offset, int rows, Sort sort, Class<T> type) {
        return find(new KeyValueQuery(sort).skip(offset).limit(rows), type);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueOperations#count(org.springframework.data.keyvalue.core.query.KeyValueQuery, java.lang.Class)
     */
    @Override
    public Mono<Long> count(KeyValueQuery<?> query, Class<?> type) {
        return executeRequired(adapter -> adapter.count(query, resolveKeySpace(type)).flux());
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.KeyValueOperations#getMappingContext()
     */
    @Override
    public MappingContext<?, ?> getMappingContext() {
        return this.mappingContext;
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.beans.factory.DisposableBean#destroy()
     */
    @Override
    public void destroy() throws Exception {
        this.adapter.clear();
    }

    private KeyValuePersistentEntity<?, ?> getKeyValuePersistentEntity(Object objectToInsert) {
        return this.mappingContext.getRequiredPersistentEntity(ClassUtils.getUserClass(objectToInsert));
    }

    private String resolveKeySpace(Class<?> type) {
        return this.mappingContext.getRequiredPersistentEntity(type).getKeySpace();
    }

    private RuntimeException resolveExceptionIfPossible(RuntimeException e) {

        DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(e);
        return translatedException != null ? translatedException : e;
    }

    @SuppressWarnings("rawtypes")
    private <T> Mono<T> potentiallyPublishEvent(KeyValueEvent event) {
        return potentiallyPublishEvent(() -> event);
    }

    @SuppressWarnings("rawtypes")
    private <T> Mono<T> potentiallyPublishEvent(Supplier<KeyValueEvent> eventSupplier) {
        return Mono.fromRunnable(() -> {
            if (eventPublisher == null) {
                return;
            }

            KeyValueEvent event = eventSupplier.get();

            if (publishEvents && (eventTypesToPublish.isEmpty() || eventTypesToPublish.contains(event.getClass()))) {
                eventPublisher.publishEvent(event);
            }
        });
    }

    @SuppressWarnings("rawtypes")
    private <T> Function<Mono<T>, Publisher<T>> configureToPotentiallyPublishEventAfter(Function<T, KeyValueEvent> result) {
        return tMono -> tMono
                .flatMap(t -> potentiallyPublishEvent(() -> result.apply(t)).thenReturn(t))
                .switchIfEmpty(potentiallyPublishEvent(() -> result.apply(null)).then(Mono.empty()));
    }

    private static boolean typeCheck(Class<?> requiredType, @Nullable Object candidate) {
        return candidate == null || ClassUtils.isAssignable(requiredType, candidate.getClass());
    }
}
