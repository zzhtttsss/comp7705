package org.comp7705.common;

public enum OperationType {
    ADD("AddOperation"),
    GET("GetOperation"),
    LIST("ListOperation"),
    MKDIR("MkdirOperation"),
    MOVE("MoveOperation"),
    REMOVE("RemoveOperation"),
    RENAME("RenameOperation"),
    STAT("StatOperation");

    private final String name;

    OperationType(String name) {
        this.name = name;
    }
}
