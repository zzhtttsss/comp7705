package org.comp7705.common;

import lombok.Getter;

public enum RequestType {

    CHECK_ARGS_4_ADD_REQUEST("CheckArgs4AddRequest");

    @Getter
    private final String name;

    RequestType(String name) {
        this.name = name;
    }

    public static RequestType getRequestType(String name) {
        for (RequestType requestType : RequestType.values()) {
            if (requestType.getName().equals(name)) {
                return requestType;
            }
        }
        return null;
    }
}
