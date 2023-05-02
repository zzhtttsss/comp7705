package org.comp7705.common;

import lombok.Getter;

public enum GetStage {
    CHECK_ARGS(1),
    GET_DATA_NODES(2);

    @Getter
    private final int stage;

    GetStage(int stage) {
        this.stage = stage;
    }
}
