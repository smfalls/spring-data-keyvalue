package org.springframework.data.keyvalue.core;

import org.springframework.expression.spel.SpelEvaluationException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReactiveSpelQueryEngine extends ReactiveQueryEngine<ReactiveKeyValueAdapter, SpelCriteria, Comparator<?>> {

    private static final SpelExpressionParser PARSER = new SpelExpressionParser();

    /**
     * Creates a new {@link SpelQueryEngine}.
     */
    public ReactiveSpelQueryEngine() {
        super(new SpelCriteriaAccessor(PARSER), new SpelSortAccessor(PARSER));
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.ReactiveQueryEngine#execute(java.lang.Object, java.lang.Object, int, int, java.lang.String)
     */
    @Override
    public Flux<?> execute(@Nullable SpelCriteria criteria, @Nullable Comparator<?> sort, long offset, int rows,
                           String keyspace) {
        return getRequiredAdapter().getAllOf(keyspace)
                .collectList()
                .map(objects -> sortAndFilterMatchingRange(objects, criteria, sort, offset, rows))
                .flatMapMany(Flux::fromIterable);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.keyvalue.core.ReactiveQueryEngine#count(java.lang.Object, java.lang.String)
     */
    @Override
    public Mono<Long> count(@Nullable SpelCriteria criteria, String keyspace) {
        return getRequiredAdapter().getAllOf(keyspace)
                .collectList()
                .map(objects -> (long) filterMatchingRange(objects, criteria, -1, -1).size());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private List<?> sortAndFilterMatchingRange(Iterable<?> source, @Nullable SpelCriteria criteria,
                                               @Nullable Comparator sort, long offset, int rows) {

        List<?> tmp = IterableConverter.toList(source);
        if (sort != null) {
            tmp.sort(sort);
        }

        return filterMatchingRange(tmp, criteria, offset, rows);
    }

    private static <S> List<S> filterMatchingRange(List<S> source, @Nullable SpelCriteria criteria, long offset,
                                                   int rows) {

        Stream<S> stream = source.stream();

        if (criteria != null) {
            stream = stream.filter(it -> evaluateExpression(criteria, it));
        }
        if (offset > 0) {
            stream = stream.skip(offset);
        }
        if (rows > 0) {
            stream = stream.limit(rows);
        }

        return stream.collect(Collectors.toList());
    }

    private static boolean evaluateExpression(SpelCriteria criteria, Object candidate) {

        try {
            return criteria.getExpression().getValue(criteria.getContext(), candidate, Boolean.class);
        } catch (SpelEvaluationException e) {
            criteria.getContext().setVariable("it", candidate);
            return criteria.getExpression().getValue(criteria.getContext()) == null ? false
                    : criteria.getExpression().getValue(criteria.getContext(), Boolean.class);
        }
    }

}
