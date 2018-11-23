package org.factcast.core.store;

import java.util.UUID;

import lombok.RequiredArgsConstructor;
import lombok.Value;

/**
 * represents a state for one or more aggregates in a lock. Used to validate, if
 * the state has changed between acquiring and using the token (optimistic lock)
 */
@Value
@RequiredArgsConstructor
public class StateToken {
    UUID uuid;

    public StateToken() {
        this(UUID.randomUUID());
    }
}
