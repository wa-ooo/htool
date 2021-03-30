package com.zy.dp.htool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 记录一优化的表分区信息
 *
 * @author zhou
 * @date 2021/3/2
 */
public class DataFileManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataFileManager.class);

    private BufferedWriter writer;
    private boolean incremental;
    private String dataPath;
    private Map<String, Set<String>> dataMap;

    public DataFileManager(boolean incremental, String dataPath) throws IOException {
        this.incremental = incremental;
        this.dataPath = dataPath;
        loadData();
    }

    public void persistOpt(String table, String part) throws IOException {
        if (incremental) {
            if (null == part) {
                part = "null";
            }
            synchronized (this) {
                writer.write(table + ":" + part);
                writer.newLine();
                writer.flush();
            }
        }
    }

    /**
     * 判断此分区是否已优化
     *
     * @param table
     * @param part
     * @return
     */
    public boolean alreadyOpt(String table, String part) {
        if (!incremental) {
            return false;
        }
        final Set<String> tableParts = dataMap.get(table);
        if (null == tableParts) {
            return false;
        }
        if (null == part) {
            part = "null";
        }
        return tableParts.contains(part);
    }

    /**
     * 加载数据
     */
    public void loadData() throws IOException {
        this.dataMap = new ConcurrentHashMap<>();
        if (!incremental) {
            return;
        }
        File dir = new File(dataPath);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalArgumentException("path:" + dataPath + "  is not exist or not a directory!");
        }
        File dFile = new File(dataPath, "htool.data");
        if (!dFile.exists()) {
            dFile.createNewFile();
        } else {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(dFile), "UTF-8"))) {
                String line;
                String table;
                String part;
                while ((line = reader.readLine()) != null) {
                    final String[] d = line.split(":");
                    if (d.length != 2) {
                        LOGGER.warn(" can't parse data:{} ", line);
                        continue;
                    }
                    table = d[0];
                    part = d[1];
                    dataMap.computeIfAbsent(table, (t) -> new HashSet<>()).add(part);
                }
            }
        }
        writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(dFile, true), "UTF-8"));
    }

    public void close() {
        if (writer != null) {
            try {
                writer.flush();
                writer.close();
            } catch (IOException e) {
                LOGGER.warn("close error!", e);
            }
        }
    }
}
