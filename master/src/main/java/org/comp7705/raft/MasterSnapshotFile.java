package org.comp7705.raft;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class MasterSnapshotFile {

    private static final Logger logger = LoggerFactory.getLogger(MasterSnapshotFile.class);

    private String              path;

    public MasterSnapshotFile(String path) {
        super();
        this.path = path;
    }

    public String getPath() {
        return this.path;
    }

    /**
     * Save value to snapshot file.
     */
    public boolean save(final long value) {
        try {
            FileUtils.writeStringToFile(new File(path), String.valueOf(value));
            return true;
        } catch (IOException e) {
            logger.error("Fail to save snapshot", e);
            return false;
        }
    }

    public long load() throws IOException {
        final String s = FileUtils.readFileToString(new File(path));
        if (!StringUtils.isBlank(s)) {
            return Long.parseLong(s);
        }
        throw new IOException("Fail to load snapshot from " + path + ",content: " + s);
    }
}
