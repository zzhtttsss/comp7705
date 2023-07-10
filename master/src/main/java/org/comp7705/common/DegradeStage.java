package org.comp7705.common;

import lombok.Getter;

public enum DegradeStage {
    BE_WAITING(1),
    BE_DEAD(2);

    @Getter
    private final int stage;

    DegradeStage(int stage) {
        this.stage = stage;
    }
}
