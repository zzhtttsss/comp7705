package org.comp7705.common;

import lombok.Getter;

public enum RequestType {

    CHECK_ARGS_4_ADD("CheckArgs4AddRequest"),

    GET_DATA_NODES_4_ADD("GetDataNodes4AddRequest"),

    CALLBACK_4_ADD("Callback4AddRequest"),

    CHECK_ARGS_4_GET("CheckArgs4GetRequest"),

    GET_DATA_NODES_4_GET("GetDataNodes4GetRequest"),

    LIST("ListRequest"),

    MKDIR("MkdirRequest"),

    MOVE("MoveRequest"),

    REMOVE("RemoveRequest"),

    RENAME("RenameRequest"),

    STAT("StatRequest");

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
