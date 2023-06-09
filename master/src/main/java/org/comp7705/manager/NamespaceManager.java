package org.comp7705.manager;

import org.comp7705.common.FileStatus;
import org.comp7705.common.FileType;
import org.comp7705.metadata.FileNode;
import org.comp7705.raft.MasterStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class NamespaceManager {

    private static final Logger logger = LoggerFactory.getLogger(NamespaceManager.class);

    public static final String ROOT_PATH = "/";

    public final FileNode root = new FileNode("root", null, 0, FileType.DIRECTORY);

    public Set<String> fileNodeIds = new HashSet<>();

    private NamespaceManager() {
    }

    public static NamespaceManager getInstance() {
        return NamespaceManagerEnum.INSTANCE.getInstance();
    }

    public enum NamespaceManagerEnum {
        INSTANCE;

        private final NamespaceManager namespaceManager;

        NamespaceManagerEnum() {
            namespaceManager = new NamespaceManager();
        }

        public NamespaceManager getInstance() {
            return namespaceManager;
        }
    }

    public FileNode getFileNode(String path) {
        FileNode currentNode = root;
        if (path.equals(ROOT_PATH)) {
            return currentNode;
        }
//        path = path.trim();
        if (path.length() > 0 && path.charAt(0) == '/') {
            path = path.substring(1);
        }
        if (path.length() > 0 && path.charAt(path.length() - 1) == '/') {
            path = path.substring(0, path.length() - 1);
        }
        String[] fileNames = path.split("/");

        for (String name : fileNames) {
            FileNode nextNode = currentNode.getChildNodes().get(name);
            if (nextNode == null) {
                return null;
            }
            if (nextNode.getStatus() == FileStatus.DELETED) {
                return null;
            }
            currentNode = nextNode;
        }
        return currentNode;
    }

    public FileNode addFileNode(String path, String filename, FileType type, long size) {
        logger.info("addFileNode: path={}, filename={}, type={}, size={}", path, filename, type, size);
        FileNode fileNode = getFileNode(path);
        logger.info("addFileNode: fileNode={}", fileNode.getName());
        if (fileNode == null || fileNode.getType() != FileType.DIRECTORY) {
            return null;
        }
        if (fileNode.getChildNodes().containsKey(filename)) {
            return null;
        }
        FileNode newNode = new FileNode(filename, fileNode, size, type);
        fileNode.getChildNodes().put(filename, newNode);
        fileNodeIds.add(newNode.getId());
        return newNode;
    }

    public boolean moveFileNode(String currentPath, String targetPath) {
        FileNode fileNode = getFileNode(currentPath);
        FileNode newParentNode = getFileNode(targetPath);
        if (fileNode == null || newParentNode == null) {
            return false;
        }
        if (newParentNode.getChildNodes().containsKey(fileNode.getName())) {
            return false;
        }
        newParentNode.getChildNodes().put(fileNode.getName(), fileNode);
        fileNode.getParentNode().getChildNodes().remove(fileNode.getName());
        fileNode.setParentNode(newParentNode);
        return true;
    }

    public boolean renameFileNode(String path, String newName) {
        FileNode fileNode = getFileNode(path);
        if (fileNode == null) {
            return false;
        }
        if (fileNode.getParentNode().getChildNodes().containsKey(newName)) {
            return false;
        }
        fileNode.getParentNode().getChildNodes().remove(fileNode.getName());
        fileNode.setName(newName);
        fileNode.getParentNode().getChildNodes().put(newName, fileNode);
        if (fileNode.getStatus() == FileStatus.DELETED) {
            fileNode.setStatus(FileStatus.ALIVE);
            fileNode.setDelTime(0);
        }
        return true;
    }

    public boolean removeFileNode(String path) {
        FileNode fileNode = getFileNode(path);
        if (fileNode == null) {
            return false;
        }
        fileNode.setStatus(FileStatus.DELETED);
        fileNode.setDelTime(System.currentTimeMillis());
        return true;
    }

    public boolean eraseFileNode(String path) {
        FileNode fileNode = getFileNode(path);
        if (fileNode == null) {
            return false;
        }
        fileNode.getParentNode().getChildNodes().remove(fileNode.getName());
        fileNodeIds.remove(fileNode.getId());
        return true;
    }

    public FileNode statFileNode(String path) {
        return getFileNode(path);
    }

    public ArrayList<FileNode> listFileNode(String path) {
        FileNode fileNode = getFileNode(path);
        if (fileNode == null || fileNode.getType() != FileType.DIRECTORY) {
            return null;
        }
        ArrayList<FileNode> fileNodes = new ArrayList<>();
        for (FileNode node : fileNode.getChildNodes().values()) {
            if (node.getStatus() == FileStatus.DELETED) {
                continue;
            }
            fileNodes.add(node);
        }
        return fileNodes;
    }




}
