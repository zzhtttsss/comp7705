package org.comp7705.common;

import lombok.Getter;

public enum AddStage {
    CHECK_ARGS(1),
    GET_DATA_NODES(2),
    APPLY_RESULT(3);

    @Getter
    private final int stage;

    AddStage(int stage) {
        this.stage = stage;
    }
}
