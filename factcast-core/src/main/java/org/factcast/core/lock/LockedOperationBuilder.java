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
package org.factcast.core.lock;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.factcast.core.lock.opt.WithOptimisticLock;
import org.factcast.core.lock.opt.WithOptimisticLock.OptimisticRetriesExceededException;
import org.factcast.core.store.FactStore;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public final class LockedOperationBuilder {
    @NonNull
    final FactStore store;

    @NonNull
    final String ns;

    public final OnBuilderStep on(@NonNull UUID aggId, UUID... otherAggIds) {
        LinkedList<UUID> ids = new LinkedList<>();
        ids.add(aggId);
        if (otherAggIds != null)
            ids.addAll(Arrays.asList(otherAggIds));

        return new OnBuilderStep(ids);
    }

    public final class OnBuilderStep {
        protected final List<UUID> ids;

        private OnBuilderStep(LinkedList<UUID> ids) {
            this.ids = ids;
        }

        public WithOptimisticLock optimistic() {
            return new WithOptimisticLock(store, ns, ids);
        }

        // we MIGHT add pessimistic if we REALLY REALLY have to

        /**
         * convenience method that uses optimistic locking with defaults. Alternatively,
         * you can call optimistic() to get control over the optimistic settings.
         * 
         * @param operation
         * @return id of the last fact published
         * @throws OptimisticRetriesExceededException
         * @throws ExceptionAfterPublish
         * @throws AttemptAbortedException
         */
        public @NonNull UUID attempt(@NonNull Attempt operation)
                throws OptimisticRetriesExceededException,
                ExceptionAfterPublish, AttemptAbortedException {
            return optimistic().attempt(operation);
        }

    }
}