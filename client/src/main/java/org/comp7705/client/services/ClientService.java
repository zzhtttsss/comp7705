package org.comp7705.client.services;


public interface ClientService {
    void mkdir(String path, String name);

    void list(String path, boolean isLatest);

    void add(String src, String des);

    void get(String src, String des);

    void move(String src, String des);

    void remove(String src);

    void rename(String src, String des);

    void stat(String src, boolean isLatest);

    void close();

}
