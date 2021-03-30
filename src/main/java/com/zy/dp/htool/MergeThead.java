package com.zy.dp.htool;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 合并任务
 *
 * @author zhou
 * @date 2021/3/2
 */
public class MergeThead extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(MergeThead.class);

    private HTableSverice hTableSverice;
    private DataFileManager dataFileManager;
    private final int profit;

    public MergeThead(int i, HTableSverice hTableSverice, DataFileManager dataFileManager, int profit) {
        super("opt-" + i);
        this.hTableSverice = hTableSverice;
        this.dataFileManager = dataFileManager;
        this.profit = profit;
    }

    @Override
    public void run() {
        MergeTask mergeTask;
        while (!HTool.isShutdown()) {
            try {
                mergeTask = HTool.takeTask();
                if (null == mergeTask) {
                    continue;
                }
            } catch (InterruptedException e) {
                //ignore
                continue;
            }
            try {
                if (doMerge(mergeTask)) {
                    dataFileManager.persistOpt(mergeTask.getTable(), mergeTask.getPart());
                }
            } catch (Exception e) {
                LOGGER.warn("merge task error!table:{},part:{},files:{}", mergeTask.getTable(), mergeTask.getPart(), mergeTask.getFiles(), e);
            }
        }
    }

    private boolean doMerge(MergeTask mergeTask) throws Exception {
        Integer fs = mergeTask.getFiles();
        String table = mergeTask.getTable();
        String part = mergeTask.getPart();
        if (dataFileManager.alreadyOpt(mergeTask.getTable(), part)) {
            return false;
        }
        String path = mergeTask.getTableLocation();
        if (null != part) {
            path = path + "/" + part;
        }
        List<HdfsFileStatus> fileStatusList = HDFSUtil.getInstance().listFiles(path);
        if (CollectionUtils.isEmpty(fileStatusList) || 1 == fileStatusList.size()) {
            LOGGER.info("there are no more files in this directory!dir:{}", path);
            return true;
        }
        long totalSize = 0;
        for (HdfsFileStatus hdfsFileStatus : fileStatusList) {
            totalSize += hdfsFileStatus.getLen();
        }
        //如果未指定合并成的文件个数，则将会根据hdfs block size来确定最后多少个文件
        if (null == fs || fs < 1) {
            long blockSize = HDFSUtil.getInstance().getBlockSize();
            fs = Long.valueOf(Math.max(1, totalSize / blockSize)).intValue();
        }
        if (fs >= fileStatusList.size()) {
            return true;
        }
        if (fileStatusList.size() - fs < profit) {
            LOGGER.info("less than min-profit in path:{}", path);
            return false;
        }
        LOGGER.info("path:{}  files :{}  total size:{} ,merge into {} files", path, fileStatusList.size(), totalSize, fs);
        return hTableSverice.mergeTable(table, part, fs, mergeTask.getCols());
    }

}
