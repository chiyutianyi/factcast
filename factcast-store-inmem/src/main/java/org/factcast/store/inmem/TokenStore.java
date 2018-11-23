package org.factcast.store.inmem;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.factcast.core.store.StateToken;

class TokenStore {

    final Map<StateToken, Map<UUID, Optional<UUID>>> tokens = new HashMap<>();

    StateToken create(Map<UUID, Optional<UUID>> state) {
        StateToken token = new StateToken();
        tokens.put(token, state);
        return token;
    }

    void invalidate(StateToken token) {
        tokens.remove(token);
    }

    Map<UUID, Optional<UUID>> get(StateToken token) {
        return tokens.get(token);
    }
}
