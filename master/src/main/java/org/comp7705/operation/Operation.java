package org.comp7705.operation;

import com.google.protobuf.Message;

import java.io.Serializable;

public interface Operation extends Serializable {

    public Message apply() throws Exception;
}
