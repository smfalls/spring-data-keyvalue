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
package org.springframework.data.keyvalue.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.annotation.AliasFor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.keyvalue.annotation.KeySpace;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.map.MapKeyValueAdapter;
import org.springframework.data.map.ReactiveMapKeyValueAdapter;
import reactor.test.StepVerifier;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Mark Paluch
 */
class ReactiveKeyValueTemplateTests {

	private static final Foo FOO_ONE = new Foo("one");
	private static final Foo FOO_TWO = new Foo("two");
	private static final Foo FOO_THREE = new Foo("three");
	private static final Bar BAR_ONE = new Bar("one");
	private static final ClassWithTypeAlias ALIASED = new ClassWithTypeAlias("super");
	private static final SubclassOfAliasedType SUBCLASS_OF_ALIASED = new SubclassOfAliasedType("sub");
	private static final KeyValueQuery<String> STRING_QUERY = new KeyValueQuery<>("foo == 'two'");

	private ReactiveKeyValueTemplate operations;

	@BeforeEach
	void setUp() {
		this.operations = new ReactiveKeyValueTemplate(new ReactiveMapKeyValueAdapter());
	}

	@AfterEach
	void tearDown() throws Exception {
		this.operations.destroy();
	}

	private <T> void insertAndVerifyCompleted(Object id, T o) {
		operations.insert(id, o)
				.as(StepVerifier::create)
				.expectNextCount(1)
				.verifyComplete();
	}

	@Test // DATACMNS-525
	void insertShouldNotThorwErrorWhenExecutedHavingNonExistingIdAndNonNullValue() {
		operations.insert("1", FOO_ONE)
				.as(StepVerifier::create)
				.expectNextCount(1)
				.verifyComplete();
	}

	@Test // DATACMNS-525
	void insertShouldThrowExceptionForNullId() {
		assertThatIllegalArgumentException().isThrownBy(() -> operations.insert(null, FOO_ONE));
	}

	@Test // DATACMNS-525
	void insertShouldThrowExceptionForNullObject() {
		assertThatIllegalArgumentException().isThrownBy(() -> operations.insert("some-id", null));
	}

	@Test // DATACMNS-525
	void insertShouldThrowExecptionWhenObjectOfSameTypeAlreadyExists() {

		operations.insert("1", FOO_ONE)
				.as(StepVerifier::create)
				.expectError(DuplicateKeyException.class);
	}

	@Test // DATACMNS-525
	void insertShouldWorkCorrectlyWhenObjectsOfDifferentTypesWithSameIdAreInserted() {

		operations.insert("1", FOO_ONE)
				.as(StepVerifier::create)
				.expectNextCount(1)
				.verifyComplete();
		operations.insert("1", BAR_ONE)
				.as(StepVerifier::create)
				.expectNextCount(1)
				.verifyComplete();
	}

	@Test // DATACMNS-525
	void createShouldReturnSameInstanceGenerateId() {

		ClassWithStringId source = new ClassWithStringId();
		operations.insert(source)
				.as(StepVerifier::create)
				.consumeNextWith(target -> {
					assertThat(target).isSameAs(source);
				})
				.verifyComplete();
	}

	@Test // DATACMNS-525
	void createShouldRespectExistingId() {

		ClassWithStringId source = new ClassWithStringId();
		source.id = "one";

		operations.insert(source)
				.as(StepVerifier::create)
				.expectNextCount(1)
				.verifyComplete();

		operations.findById("one", ClassWithStringId.class)
				.as(StepVerifier::create)
				.consumeNextWith(target -> {
					assertThat(target).isSameAs(source);
				})
				.verifyComplete();
	}

	@Test // DATACMNS-525
	void findByIdShouldReturnObjectWithMatchingIdAndType() {

		insertAndVerifyCompleted("1", FOO_ONE);

		operations.findById("1", Foo.class)
				.as(StepVerifier::create)
				.consumeNextWith(target -> {
					assertThat(target).isSameAs(FOO_ONE);
				})
				.verifyComplete();
	}

	@Test // DATACMNS-525
	void findByIdSouldReturnOptionalEmptyIfNoMatchingIdFound() {

		insertAndVerifyCompleted("1", FOO_ONE);

		operations.findById("2", Foo.class)
				.as(StepVerifier::create)
				.verifyComplete();
	}

	@Test // DATACMNS-525
	void findByIdShouldReturnOptionalEmptyIfNoMatchingTypeFound() {

		insertAndVerifyCompleted("1", FOO_ONE);

		operations.findById("1", Bar.class)
				.as(StepVerifier::create)
				.verifyComplete();
	}

	@Test // DATACMNS-525
	void findShouldExecuteQueryCorrectly() {

		insertAndVerifyCompleted("1", FOO_ONE);
		insertAndVerifyCompleted("2", FOO_TWO);

		operations.find(STRING_QUERY, Foo.class)
				.as(StepVerifier::create)
				.consumeNextWith(target -> {
					assertThat(target).isEqualTo(FOO_TWO);
				})
				.verifyComplete();
	}

	@Test // DATACMNS-525
	void readShouldReturnEmptyCollectionIfOffsetOutOfRange() {

		insertAndVerifyCompleted("1", FOO_ONE);
		insertAndVerifyCompleted("2", FOO_TWO);
		insertAndVerifyCompleted("3", FOO_THREE);

		operations.findInRange(5, 5, Foo.class)
				.as(StepVerifier::create)
				.verifyComplete();
	}

	@Test // DATACMNS-525
	void updateShouldReplaceExistingObject() {

		insertAndVerifyCompleted("1", FOO_ONE);
		operations.update("1", FOO_TWO)
				.as(StepVerifier::create)
				.expectNextCount(1)
				.verifyComplete();

		operations.findById("1", Foo.class)
				.as(StepVerifier::create)
				.consumeNextWith(target -> {
					assertThat(target).isEqualTo(FOO_TWO);
				})
				.verifyComplete();
	}

	@Test // DATACMNS-525
	void updateShouldRespectTypeInformation() {

		insertAndVerifyCompleted("1", FOO_ONE);
		operations.update("1", BAR_ONE)
				.as(StepVerifier::create)
				.expectNextCount(1)
				.verifyComplete();

		operations.findById("1", Foo.class)
				.as(StepVerifier::create)
				.consumeNextWith(target -> {
					assertThat(target).isEqualTo(FOO_ONE);
				})
				.verifyComplete();
	}

	@Test // DATACMNS-525
	void deleteShouldRemoveObjectCorrectly() {

		insertAndVerifyCompleted("1", FOO_ONE);
		operations.delete("1", Foo.class)
				.as(StepVerifier::create)
				.expectNextCount(1)
				.verifyComplete();

		operations.findById("1", Foo.class)
				.as(StepVerifier::create)
				.verifyComplete();
	}

	@Test // DATACMNS-525
	void deleteReturnsNullWhenNotExisting() {

		insertAndVerifyCompleted("1", FOO_ONE);

		operations.delete("2", Foo.class)
				.as(StepVerifier::create)
				.verifyComplete();
	}

	@Test // DATACMNS-525
	void deleteReturnsRemovedObject() {

		insertAndVerifyCompleted("1", FOO_ONE);

		operations.delete("1", Foo.class)
				.as(StepVerifier::create)
				.consumeNextWith(target -> {
					assertThat(target).isEqualTo(FOO_ONE);
				});
	}

	@Test // DATACMNS-525
	void deleteThrowsExceptionWhenIdCannotBeExctracted() {
		assertThatIllegalArgumentException().isThrownBy(() -> operations.delete(FOO_ONE));
	}

	@Test // DATACMNS-525
	void countShouldReturnZeroWhenNoElementsPresent() {
		operations.count(Foo.class)
				.as(StepVerifier::create)
				.consumeNextWith(count -> {
					assertThat(count).isEqualTo(0L);
				})
				.verifyComplete();
	}

	@Test // DATACMNS-525
	void insertShouldRespectTypeAlias() {

		insertAndVerifyCompleted("1", ALIASED);
		insertAndVerifyCompleted("2", SUBCLASS_OF_ALIASED);

		operations.findAll(ALIASED.getClass())
				.buffer(2)
				.as(StepVerifier::create)
				.consumeNextWith(results -> {
					assertThat((List) results).contains(ALIASED, SUBCLASS_OF_ALIASED);
				})
				.verifyComplete();
	}

	@Data
	@AllArgsConstructor
	static class Foo {

		String foo;

	}

	@Data
	@AllArgsConstructor
	static class Bar {

		String bar;
	}

	@Data
	static class ClassWithStringId implements Serializable {

		private static final long serialVersionUID = -7481030649267602830L;
		@Id String id;
		String value;
	}

	@ExplicitKeySpace(name = "aliased")
	@Data
	static class ClassWithTypeAlias implements Serializable {

		private static final long serialVersionUID = -5921943364908784571L;
		@Id String id;
		String name;

		ClassWithTypeAlias(String name) {
			this.name = name;
		}
	}

	static class SubclassOfAliasedType extends ClassWithTypeAlias {

		private static final long serialVersionUID = -468809596668871479L;

		SubclassOfAliasedType(String name) {
			super(name);
		}

	}

	@KeySpace
	@Persistent
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	@interface ExplicitKeySpace {

		@AliasFor(annotation = KeySpace.class, value = "value")
		String name() default "";

	}
}
