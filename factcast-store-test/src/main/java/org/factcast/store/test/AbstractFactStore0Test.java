/*
 * Copyright © 2018 Mercateo AG (http://www.mercateo.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.factcast.store.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.factcast.core.Fact;
import org.factcast.core.FactCast;
import org.factcast.core.MarkFact;
import org.factcast.core.spec.FactSpec;
import org.factcast.core.store.FactStore;
import org.factcast.core.store.StateToken;
import org.factcast.core.subscription.Subscription;
import org.factcast.core.subscription.SubscriptionRequest;
import org.factcast.core.subscription.observer.FactObserver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.test.annotation.DirtiesContext;

import lombok.SneakyThrows;

@SuppressWarnings("OptionalGetWithoutIsPresent")
public abstract class AbstractFactStore0Test {

    static final FactSpec ANY = FactSpec.ns("default");

    protected FactCast fc;

    protected FactStore store;

    @BeforeEach
    void setUp() {
        store = createStoreToTest();
        fc = FactCast.from(store);
    }

    protected abstract FactStore createStoreToTest();

    @DirtiesContext
    @Test
    public void testEmptyStore() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            FactObserver observer = mock(FactObserver.class);
            Subscription s = fc.subscribeToFacts(SubscriptionRequest.catchup(ANY).fromScratch(),
                    observer);
            s.awaitComplete();
            verify(observer).onCatchup();
            verify(observer).onComplete();
            verify(observer, never()).onError(Mockito.any());
            verify(observer, never()).onNext(any());
        });
    }

    @DirtiesContext
    @Test
    public void testUniquenessConstraint() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            Assertions.assertThrows(IllegalArgumentException.class, () -> {
                final UUID id = UUID.randomUUID();
                fc.publish(Fact.of("{\"id\":\"" + id
                        + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
                fc.publish(Fact.of("{\"id\":\"" + id
                        + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
                fail();
            });
        });
    }

    @DirtiesContext
    @Test
    public void testEmptyStoreFollowNonMatching() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            TestFactObserver observer = testObserver();
            fc.subscribeToFacts(SubscriptionRequest.follow(ANY).fromScratch(), observer)
                    .awaitCatchup();
            verify(observer).onCatchup();
            verify(observer, never()).onComplete();
            verify(observer, never()).onError(any());
            verify(observer, never()).onNext(any());
            fc.publishWithMark(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"other\"}", "{}"));
            fc.publishWithMark(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"other\"}", "{}"));
            observer.await(2);
            // the mark facts only
            verify(observer, times(2)).onNext(any());
            assertEquals(MarkFact.MARK_TYPE, observer.values.get(0).type());
            assertEquals(MarkFact.MARK_TYPE, observer.values.get(1).type());
        });
    }

    private TestFactObserver testObserver() {
        return spy(new TestFactObserver());
    }

    private static class TestFactObserver implements FactObserver {

        private final List<Fact> values = new CopyOnWriteArrayList<>();

        @Override
        public void onNext(Fact element) {
            values.add(element);
        }

        @SneakyThrows
        public void await(int count) {
            while (true) {
                if (values.size() >= count) {
                    return;
                } else {
                    Thread.sleep(50);
                }
            }
        }
    }

    @DirtiesContext
    @Test
    public void testEmptyStoreFollowMatching() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            TestFactObserver observer = testObserver();
            fc.subscribeToFacts(SubscriptionRequest.follow(ANY).fromScratch(), observer)
                    .awaitCatchup();
            verify(observer).onCatchup();
            verify(observer, never()).onComplete();
            verify(observer, never()).onError(any());
            verify(observer, never()).onNext(any());
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            observer.await(1);
        });
    }

    @DirtiesContext
    @Test
    public void testEmptyStoreEphemeral() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            TestFactObserver observer = testObserver();
            fc.subscribeToFacts(SubscriptionRequest.follow(ANY).fromNowOn(), observer)
                    .awaitCatchup();
            // nothing recieved
            verify(observer).onCatchup();
            verify(observer, never()).onComplete();
            verify(observer, never()).onError(any());
            verify(observer, never()).onNext(any());
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            observer.await(1);
            verify(observer, times(1)).onNext(any());
        });
    }

    @DirtiesContext
    @Test
    public void testEmptyStoreEphemeralWithCancel() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            TestFactObserver observer = testObserver();
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            Subscription subscription = fc.subscribeToFacts(SubscriptionRequest.follow(ANY)
                    .fromNowOn(), observer).awaitCatchup();
            // nothing recieved
            verify(observer).onCatchup();
            verify(observer, never()).onComplete();
            verify(observer, never()).onError(any());
            verify(observer, never()).onNext(any());
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            observer.await(1);
            verify(observer, times(1)).onNext(any());
            subscription.close();
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            Thread.sleep(100);
            // additional event not received
            verify(observer, times(1)).onNext(any());
        });
    }

    @DirtiesContext
    @Test
    public void testEmptyStoreFollowWithCancel() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            TestFactObserver observer = testObserver();
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            Subscription subscription = fc.subscribeToFacts(SubscriptionRequest.follow(ANY)
                    .fromScratch(), observer).awaitCatchup();
            // nothing recieved
            verify(observer).onCatchup();
            verify(observer, never()).onComplete();
            verify(observer, never()).onError(any());
            verify(observer, times(3)).onNext(any());
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            observer.await(4);
            subscription.close();
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            Thread.sleep(100);
            // additional event not received
            verify(observer, times(4)).onNext(any());
        });
    }

    @DirtiesContext
    @Test
    public void testEmptyStoreCatchupMatching() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            FactObserver observer = mock(FactObserver.class);
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            fc.subscribeToFacts(SubscriptionRequest.catchup(ANY).fromScratch(), observer)
                    .awaitComplete();
            verify(observer).onCatchup();
            verify(observer).onComplete();
            verify(observer, never()).onError(any());
            verify(observer).onNext(any());
        });
    }

    @DirtiesContext
    @Test
    public void testEmptyStoreFollowMatchingDelayed() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            TestFactObserver observer = testObserver();
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            fc.subscribeToFacts(SubscriptionRequest.follow(ANY).fromScratch(), observer)
                    .awaitCatchup();
            verify(observer).onCatchup();
            verify(observer, never()).onComplete();
            verify(observer, never()).onError(any());
            verify(observer).onNext(any());
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            observer.await(2);
        });
    }

    @DirtiesContext
    @Test
    public void testEmptyStoreFollowNonMatchingDelayed() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            TestFactObserver observer = testObserver();
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"ns\":\"default\",\"type\":\"t1\"}", "{}"));
            fc.subscribeToFacts(SubscriptionRequest.follow(ANY).fromScratch(), observer)
                    .awaitCatchup();
            verify(observer).onCatchup();
            verify(observer, never()).onComplete();
            verify(observer, never()).onError(any());
            verify(observer).onNext(any());
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"ns\":\"other\",\"type\":\"t1\"}", "{}"));
            observer.await(1);
        });
    }

    @DirtiesContext
    @Test
    public void testFetchById() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            UUID id = UUID.randomUUID();
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            Optional<Fact> f = fc.fetchById(id);
            assertFalse(f.isPresent());
            fc.publish(Fact.of("{\"id\":\"" + id + "\",\"type\":\"someType\",\"ns\":\"default\"}",
                    "{}"));
            f = fc.fetchById(id);
            assertTrue(f.isPresent());
            assertEquals(id, f.map(Fact::id).orElse(null));
        });
    }

    @DirtiesContext
    @Test
    public void testAnySubscriptionsMatchesMark() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            FactObserver observer = mock(FactObserver.class);
            UUID mark = fc.publishWithMark(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"ns\":\"" + UUID.randomUUID() + "\",\"type\":\"noone_knows\"}", "{}"));
            ArgumentCaptor<Fact> af = ArgumentCaptor.forClass(Fact.class);
            doNothing().when(observer).onNext(af.capture());
            fc.subscribeToFacts(SubscriptionRequest.catchup(ANY).fromScratch(), observer)
                    .awaitComplete();
            verify(observer).onNext(any());
            assertEquals(mark, af.getValue().id());
            verify(observer).onComplete();
            verify(observer).onCatchup();
            verifyNoMoreInteractions(observer);
        });
    }

    @DirtiesContext
    @Test
    public void testRequiredMetaAttribute() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            FactObserver observer = mock(FactObserver.class);
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"ns\":\"default\",\"type\":\"noone_knows\"}", "{}"));
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"ns\":\"default\",\"type\":\"noone_knows\",\"meta\":{\"foo\":\"bar\"}}",
                    "{}"));
            FactSpec REQ_FOO_BAR = FactSpec.ns("default").meta("foo", "bar");
            fc.subscribeToFacts(SubscriptionRequest.catchup(REQ_FOO_BAR).fromScratch(), observer)
                    .awaitComplete();
            verify(observer).onNext(any());
            verify(observer).onCatchup();
            verify(observer).onComplete();
            verifyNoMoreInteractions(observer);
        });
    }

    @DirtiesContext
    @Test
    public void testScriptedWithPayloadFiltering() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            FactObserver observer = mock(FactObserver.class);
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"ns\":\"default\",\"type\":\"noone_knows\",\"hit\":\"me\"}", "{}"));
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"ns\":\"default\",\"type\":\"noone_knows\",\"meta\":{\"foo\":\"bar\"}}",
                    "{}"));
            FactSpec SCRIPTED = FactSpec.ns("default").jsFilterScript(
                    "function (h,e){ return (h.hit=='me')}");
            fc.subscribeToFacts(SubscriptionRequest.catchup(SCRIPTED).fromScratch(), observer)
                    .awaitComplete();
            verify(observer).onNext(any());
            verify(observer).onCatchup();
            verify(observer).onComplete();
            verifyNoMoreInteractions(observer);
        });
    }

    @DirtiesContext
    @Test
    public void testScriptedWithHeaderFiltering() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            FactObserver observer = mock(FactObserver.class);
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"ns\":\"default\",\"type\":\"noone_knows\",\"hit\":\"me\"}", "{}"));
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"ns\":\"default\",\"type\":\"noone_knows\",\"meta\":{\"foo\":\"bar\"}}",
                    "{}"));
            FactSpec SCRIPTED = FactSpec.ns("default").jsFilterScript(
                    "function (h){ return (h.hit=='me')}");
            fc.subscribeToFacts(SubscriptionRequest.catchup(SCRIPTED).fromScratch(), observer)
                    .awaitComplete();
            verify(observer).onNext(any());
            verify(observer).onCatchup();
            verify(observer).onComplete();
            verifyNoMoreInteractions(observer);
        });
    }

    @DirtiesContext
    @Test
    public void testScriptedFilteringMatchAll() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            FactObserver observer = mock(FactObserver.class);
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"ns\":\"default\",\"type\":\"noone_knows\",\"hit\":\"me\"}", "{}"));
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"ns\":\"default\",\"type\":\"noone_knows\",\"meta\":{\"foo\":\"bar\"}}",
                    "{}"));
            FactSpec SCRIPTED = FactSpec.ns("default").jsFilterScript(
                    "function (h){ return true }");
            fc.subscribeToFacts(SubscriptionRequest.catchup(SCRIPTED).fromScratch(), observer)
                    .awaitComplete();
            verify(observer, times(2)).onNext(any());
            verify(observer).onCatchup();
            verify(observer).onComplete();
            verifyNoMoreInteractions(observer);
        });
    }

    @DirtiesContext
    @Test
    public void testScriptedFilteringMatchNone() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            FactObserver observer = mock(FactObserver.class);
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"ns\":\"default\",\"type\":\"noone_knows\",\"hit\":\"me\"}", "{}"));
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID()
                    + "\",\"ns\":\"default\",\"type\":\"noone_knows\",\"meta\":{\"foo\":\"bar\"}}",
                    "{}"));
            FactSpec SCRIPTED = FactSpec.ns("default").jsFilterScript(
                    "function (h){ return false }");
            fc.subscribeToFacts(SubscriptionRequest.catchup(SCRIPTED).fromScratch(), observer)
                    .awaitComplete();
            verify(observer).onCatchup();
            verify(observer).onComplete();
            verifyNoMoreInteractions(observer);
        });
    }

    @DirtiesContext
    @Test
    public void testIncludeMarks() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            final UUID id = UUID.randomUUID();
            fc.publishWithMark(Fact.of("{\"id\":\"" + id
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            FactObserver observer = mock(FactObserver.class);
            fc.subscribeToFacts(SubscriptionRequest.catchup(FactSpec.ns("default")).fromScratch(),
                    observer).awaitComplete();
            verify(observer, times(2)).onNext(any());
        });
    }

    @DirtiesContext
    @Test
    public void testSkipMarks() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            final UUID id = UUID.randomUUID();
            fc.publishWithMark(Fact.of("{\"id\":\"" + id
                    + "\",\"type\":\"someType\",\"ns\":\"default\"}", "{}"));
            FactObserver observer = mock(FactObserver.class);
            fc.subscribeToFacts(SubscriptionRequest.catchup(FactSpec.ns("default"))
                    .skipMarks()
                    .fromScratch(), observer).awaitComplete();
            verify(observer, times(1)).onNext(any());
        });
    }

    @DirtiesContext
    @Test
    public void testMatchBySingleAggId() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            final UUID id = UUID.randomUUID();
            final UUID aggId1 = UUID.randomUUID();
            fc.publishWithMark(Fact.of("{\"id\":\"" + id
                    + "\",\"type\":\"someType\",\"ns\":\"default\",\"aggIds\":[\"" + aggId1
                    + "\"]}", "{}"));
            FactObserver observer = mock(FactObserver.class);
            fc.subscribeToFacts(SubscriptionRequest.catchup(FactSpec.ns("default").aggId(aggId1))
                    .skipMarks()
                    .fromScratch(), observer).awaitComplete();
            verify(observer, times(1)).onNext(any());
        });
    }

    @DirtiesContext
    @Test
    public void testMatchByOneOfAggId() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            final UUID id = UUID.randomUUID();
            final UUID aggId1 = UUID.randomUUID();
            final UUID aggId2 = UUID.randomUUID();
            fc.publishWithMark(Fact.of("{\"id\":\"" + id
                    + "\",\"type\":\"someType\",\"ns\":\"default\",\"aggIds\":[\"" + aggId1
                    + "\",\"" + aggId2 + "\"]}", "{}"));
            FactObserver observer = mock(FactObserver.class);
            fc.subscribeToFacts(SubscriptionRequest.catchup(FactSpec.ns("default").aggId(aggId1))
                    .skipMarks()
                    .fromScratch(), observer).awaitComplete();
            verify(observer, times(1)).onNext(any());
            observer = mock(FactObserver.class);
            fc.subscribeToFacts(SubscriptionRequest.catchup(FactSpec.ns("default").aggId(aggId2))
                    .skipMarks()
                    .fromScratch(), observer).awaitComplete();
            verify(observer, times(1)).onNext(any());
        });
    }

    @DirtiesContext
    @Test
    public void testMatchBySecondAggId() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            final UUID id = UUID.randomUUID();
            final UUID aggId1 = UUID.randomUUID();
            final UUID aggId2 = UUID.randomUUID();
            fc.publishWithMark(Fact.of("{\"id\":\"" + id
                    + "\",\"type\":\"someType\",\"ns\":\"default\",\"aggIds\":[\"" + aggId1
                    + "\",\"" + aggId2 + "\"]}", "{}"));
            FactObserver observer = mock(FactObserver.class);
            fc.subscribeToFacts(SubscriptionRequest.catchup(FactSpec.ns("default").aggId(aggId2))
                    .skipMarks()
                    .fromScratch(), observer).awaitComplete();
            verify(observer, times(1)).onNext(any());
        });
    }

    @DirtiesContext
    @Test
    public void testDelayed() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            final UUID id = UUID.randomUUID();
            TestFactObserver obs = new TestFactObserver();
            try (Subscription s = fc.subscribeToFacts(SubscriptionRequest.follow(500, FactSpec.ns(
                    "default").aggId(id)).skipMarks().fromScratch(), obs)) {
                fc.publishWithMark(Fact.of("{\"id\":\"" + id
                        + "\",\"type\":\"someType\",\"ns\":\"default\",\"aggIds\":[\"" + id
                        + "\"]}", "{}"));
                // will take some time on pgstore
                obs.await(1);
            }
        });
    }

    @DirtiesContext
    @Test
    public void testSerialOf() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            final UUID id = UUID.randomUUID();
            assertFalse(fc.serialOf(id).isPresent());
            UUID mark1 = fc.publishWithMark(Fact.of("{\"id\":\"" + id
                    + "\",\"type\":\"someType\",\"ns\":\"default\",\"aggIds\":[\"" + id + "\"]}",
                    "{}"));
            assertTrue(fc.serialOf(mark1).isPresent());
            assertTrue(fc.serialOf(id).isPresent());
            long serMark = fc.serialOf(mark1).getAsLong();
            long serFact = fc.serialOf(id).getAsLong();
            assertTrue(serFact < serMark);
        });
    }

    @DirtiesContext
    @Test
    public void testSerialHeader() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            UUID id = UUID.randomUUID();
            fc.publish(Fact.of("{\"id\":\"" + id
                    + "\",\"type\":\"someType\",\"ns\":\"default\",\"aggIds\":[\"" + id + "\"]}",
                    "{}"));
            UUID id2 = UUID.randomUUID();
            fc.publish(Fact.of("{\"id\":\"" + id2
                    + "\",\"type\":\"someType\",\"meta\":{\"foo\":\"bar\"},\"ns\":\"default\",\"aggIds\":[\""
                    + id2 + "\"]}", "{}"));
            OptionalLong serialOf = fc.serialOf(id);
            assertTrue(serialOf.isPresent());
            Fact f = fc.fetchById(id).get();
            Fact fact2 = fc.fetchById(id2).get();
            assertEquals(serialOf.getAsLong(), f.serial());
            assertTrue(f.before(fact2));
        });
    }

    @DirtiesContext
    @Test
    public void testUniqueIdentConstraintInLog() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            String ident = UUID.randomUUID().toString();
            UUID id = UUID.randomUUID();
            UUID id2 = UUID.randomUUID();
            Fact f1 = Fact.of("{\"id\":\"" + id
                    + "\",\"type\":\"someType\",\"ns\":\"default\",\"aggIds\":[\"" + id
                    + "\"],\"meta\":{\"unique_identifier\":\"" + ident + "\"}}", "{}");
            Fact f2 = Fact.of("{\"id\":\"" + id2
                    + "\",\"type\":\"someType\",\"ns\":\"default\",\"aggIds\":[\"" + id2
                    + "\"],\"meta\":{\"unique_identifier\":\"" + ident + "\"}}", "{}");
            fc.publish(f1);
            // needs to fail due to uniqueIdentitfier not being unique
            try {
                fc.publish(f2);
                fail("Expected IllegalArgumentException due to unique_identifier being used a sencond time");
            } catch (IllegalArgumentException e) {
                // make sure, f1 was stored before
                assertTrue(fc.fetchById(id).isPresent());
            }
        });
    }

    @DirtiesContext
    @Test
    public void testUniqueIdentConstraintInBatch() {
        Assertions.assertTimeout(Duration.ofMillis(10000), () -> {
            String ident = UUID.randomUUID().toString();
            UUID id = UUID.randomUUID();
            UUID id2 = UUID.randomUUID();
            Fact f1 = Fact.of("{\"id\":\"" + id
                    + "\",\"type\":\"someType\",\"ns\":\"default\",\"aggIds\":[\"" + id
                    + "\"],\"meta\":{\"unique_identifier\":\"" + ident + "\"}}", "{}");
            Fact f2 = Fact.of("{\"id\":\"" + id2
                    + "\",\"type\":\"someType\",\"ns\":\"default\",\"aggIds\":[\"" + id2
                    + "\"],\"meta\":{\"unique_identifier\":\"" + ident + "\"}}", "{}");
            // needs to fail due to uniqueIdentitfier not being unique
            try {
                fc.publish(Arrays.asList(f1, f2));
                fail("Expected IllegalArgumentException due to unique_identifier being used twice in a batch");
            } catch (IllegalArgumentException e) {
                // make sure, f1 was not stored either
                assertFalse(fc.fetchById(id).isPresent());
            }
        });
    }

    @Test
    public void testChecksMandatoryNamespaceOnPublish() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            fc.publish(Fact.of("{\"id\":\"" + UUID.randomUUID() + "\",\"type\":\"someType\"}",
                    "{}"));
        });
    }

    @Test
    public void testChecksMandatoryIdOnPublish() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            fc.publish(Fact.of("{\"ns\":\"default\",\"type\":\"someType\"}", "{}"));
        });
    }

    @Test
    public void testEnumerateNameSpaces() {
        // no namespaces
        assertEquals(0, fc.enumerateNamespaces().size());
        fc.publish(Fact.builder().ns("ns1").build("{}"));
        fc.publish(Fact.builder().ns("ns2").build("{}"));
        Set<String> ns = fc.enumerateNamespaces();
        assertEquals(2, ns.size());
        assertTrue(ns.contains("ns1"));
        assertTrue(ns.contains("ns2"));
    }

    @Test
    public void testEnumerateTypesNull() {
        Assertions.assertThrows(NullPointerException.class, () -> {
            fc.enumerateTypes(null);
        });
    }

    @Test
    void testEnumerateTypes() {
        fc.publish(Fact.builder().ns("ns1").type("t1").build("{}"));
        fc.publish(Fact.builder().ns("ns2").type("t2").build("{}"));
        assertEquals(1, fc.enumerateTypes("ns1").size());
        assertTrue(fc.enumerateTypes("ns1").contains("t1"));
        fc.publish(Fact.builder().ns("ns1").type("t1").build("{}"));
        fc.publish(Fact.builder().ns("ns1").type("t1").build("{}"));
        fc.publish(Fact.builder().ns("ns1").type("t2").build("{}"));
        fc.publish(Fact.builder().ns("ns1").type("t3").build("{}"));
        fc.publish(Fact.builder().ns("ns1").type("t2").build("{}"));
        assertEquals(3, fc.enumerateTypes("ns1").size());
        assertTrue(fc.enumerateTypes("ns1").contains("t1"));
        assertTrue(fc.enumerateTypes("ns1").contains("t2"));
        assertTrue(fc.enumerateTypes("ns1").contains("t3"));
    }

    @Test
    public void testFollow() throws Exception {
        Fact f = Fact.builder().ns("followtest").id(UUID.randomUUID()).build("{}");
        fc.publish(f);
        AtomicReference<CountDownLatch> l = new AtomicReference<>(new CountDownLatch(1));
        SubscriptionRequest request = SubscriptionRequest.follow(FactSpec.ns("followtest"))
                .fromScratch();
        FactObserver observer = element -> l.get().countDown();
        fc.subscribeToFacts(request, observer);
        l.get().await();
        l.set(new CountDownLatch(3));
        fc.publish(Fact.builder().ns("followtest").id(UUID.randomUUID()).build("{}"));
        fc.publish(Fact.builder().ns("followtest").id(UUID.randomUUID()).build("{}"));
        // needs to fail
        assertFalse(l.get().await(500, TimeUnit.MILLISECONDS));
        fc.publish(Fact.builder().ns("followtest").id(UUID.randomUUID()).build("{}"));

        assertTrue(l.get().await(10, TimeUnit.SECONDS),
                "failed to see all the facts published within 10 seconds.");
    }

    @Test
    void testLatestFactFor() {

        UUID aggId = UUID.randomUUID();
        Fact f1 = Fact.builder().aggId(aggId).build("{}");
        Fact f2 = Fact.builder().aggId(aggId).build("{}");
        Fact f3 = Fact.builder().build("{}");
        store.publish(Arrays.asList(f1, f2, f3));
        assertEquals(f2.id(), store.latestFactFor(aggId).get());

        createStoreToTest();
        store.publish(Arrays.asList(f1, f3, f2));
        assertEquals(f2.id(), store.latestFactFor(aggId).get());

        createStoreToTest();
        store.publish(Arrays.asList(f3, f2, f1));
        assertEquals(f1.id(), store.latestFactFor(aggId).get());

        createStoreToTest();
        store.publish(Arrays.asList(f3));
        assertFalse(store.latestFactFor(aggId).isPresent());

    }

    @Test
    void testConditionalPublish_empty() {
        UUID aggId = UUID.randomUUID();
        Fact f1 = Fact.builder().aggId(aggId).build("{}");
        Fact f2 = Fact.builder().aggId(aggId).build("{}");

        StateToken t = store.stateFor(aggId);
        assertTrue(store.publishIfUnchanged(t, Arrays.asList(f1)));

    }

    @Test
    void testConditionalPublish_notEmpty() {
        UUID aggId = UUID.randomUUID();
        Fact f1 = Fact.builder().aggId(aggId).build("{}");
        Fact f2 = Fact.builder().aggId(aggId).build("{}");

        store.publish(Arrays.asList(f1));

        StateToken t = store.stateFor(aggId);
        assertTrue(store.publishIfUnchanged(t, Arrays.asList(f2)));
    }

    @Test
    void testConditionalPublish_changedInBetween() {
        UUID aggId = UUID.randomUUID();
        Fact f1 = Fact.builder().aggId(aggId).build("{}");
        Fact f2 = Fact.builder().aggId(aggId).build("{}");
        Fact f3 = Fact.builder().aggId(aggId).build("{}");

        store.publish(Arrays.asList(f1));
        StateToken t = store.stateFor(aggId);

        store.publish(Arrays.asList(f2));
        assertFalse(store.publishIfUnchanged(t, Arrays.asList(f3)));
        assertEquals(f2.id(), store.latestFactFor(aggId).get());
    }

}
