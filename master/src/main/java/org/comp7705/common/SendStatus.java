package org.comp7705.common;

import lombok.Getter;

public enum SendStatus {
    TO_BE_INFORMED(0),
    TO_BE_SENT(1);

    @Getter
    private final int type;

    SendStatus(int type) {
        this.type = type;
    }
}
