package com.zy.dp.htool;

import java.util.concurrent.CountDownLatch;

/**
 * 合并任务
 *
 * @author zhou
 * @date 2021/3/2
 */
public class MergeTask {

    private Integer files;
    private String table;
    private String cols;
    private String tableLocation;
    private String part;

    public MergeTask(Integer files, String table, String cols, String tableLocation, String part) {
        this.files = files;
        this.table = table;
        this.cols = cols;
        this.tableLocation = tableLocation;
        this.part = part;
    }

    public Integer getFiles() {
        return files;
    }

    public String getTable() {
        return table;
    }

    public String getCols() {
        return cols;
    }

    public String getTableLocation() {
        return tableLocation;
    }

    public String getPart() {
        return part;
    }
}
