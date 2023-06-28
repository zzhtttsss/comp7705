package org.comp7705.client.entity;

import com.google.common.base.Joiner;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.comp7705.client.utils.PathUtil;

import java.util.Arrays;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public class CmdParam {
    private String src = "";
    private String des = "";
    private boolean isLatest = true;

    public String getDesPath(){
        return PathUtil.getParentPath(des);
    }

    public String getDesName(){
        return PathUtil.getCurrentName(des);
    }

    public String getSrcPath(){
        return PathUtil.getParentPath(src);
    }

    public String getSrcName(){
        return PathUtil.getCurrentName(src);
    }
}
