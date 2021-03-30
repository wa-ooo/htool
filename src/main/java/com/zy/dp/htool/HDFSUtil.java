package com.zy.dp.htool;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.server.namenode.NameNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author zhou
 * @date 2021/3/2
 */
public class HDFSUtil {

    private static volatile HDFSUtil hdfsUtil;
    private Configuration conf;
    private DFSClient dfsClient;

    private HDFSUtil() {
        conf = new HdfsConfiguration();
        try {
            dfsClient = new DFSClient(NameNode.getAddress(conf), conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static HDFSUtil getInstance() {
        if (null == hdfsUtil) {
            synchronized (HDFSUtil.class) {
                if (null == hdfsUtil) {
                    hdfsUtil = new HDFSUtil();
                }
            }
        }
        return hdfsUtil;
    }


    public List<HdfsFileStatus> listFiles(String path) throws IOException {
        if (!dfsClient.exists(path)) {
            throw new IllegalArgumentException("unknown path:" + path);
        }
        DirectoryListing directoryListing = null;
        List<HdfsFileStatus> fileStatusList = new ArrayList<>();
        do {
            directoryListing = dfsClient.listPaths(path, null == directoryListing ? HdfsFileStatus.EMPTY_NAME : directoryListing.getLastName(), false);
            HdfsFileStatus[] partialListing = directoryListing.getPartialListing();
            if (partialListing == null || 0 == partialListing.length) {
                break;
            }
            fileStatusList.addAll(Arrays.asList(partialListing));
        } while (directoryListing.hasMore());
        return fileStatusList;
    }

    public static void close() {
        if (null != hdfsUtil && null != hdfsUtil.dfsClient) {
            IOUtils.closeQuietly(hdfsUtil.dfsClient);
        }
    }

    public long getBlockSize() {
        return conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
    }
}
