package com.zy.dp.htool;

import com.google.common.collect.Lists;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zhou
 * @date 2021/2/26
 */
public class HTool extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(HTool.class);

    private static final AtomicBoolean SHUTDOWN = new AtomicBoolean(false);
    private final String usagePrefix = "Usage: htool -d <database> [options] ";
    /**
     * task 队列的缓冲大小
     */
    private final int TASK_BUFFER = 100;

    private String database;
    private String table;
    private String partition;

    /**
     * 并行度
     */
    private Integer parallel = 1;
    /**
     * 合并成多少个文件，默认使用hdfs block size切分
     */
    private Integer files;
    private String dataPath;
    /**
     * Spark thrift server的jdbc地址
     */
    private String sts;
    private String user;
    private String password;
    private boolean incremental;
    /**
     * 利润值，默认1，只有利润大于此值才会进行合并。目的是只合并收益大的文件
     */
    private int profit = 1;

    private HTableSverice hTableSverice;
    private ConnectionManger connectionManger;
    private DataFileManager dataFileManager;
    private List<MergeThead> mergeThreads;
    private static BlockingQueue<MergeTask> taskQueue;
    Options hToolOpts = new Options();

    {
        hToolOpts.addOption(OptionBuilder.hasArg().withLongOpt("database").withDescription("specify database to merge").isRequired().create("d"))
                .addOption(OptionBuilder.hasArg().withLongOpt("table").withDescription("specify table to merge.if it is not set,htool will use all table in the database").create("t"))
                .addOption(OptionBuilder.hasArg().withLongOpt("thriftserver").withDescription("specify JDBC connect string of spark thrift server").isRequired().create("sts"))
                .addOption(OptionBuilder.hasArg().withLongOpt("user").withDescription("jdbc connection user").create("u"))
                .addOption(OptionBuilder.hasArg().withLongOpt("password").withDescription("jdbc connection password").create("p"))
                .addOption(OptionBuilder.hasArg().withLongOpt("part").withDescription("set partition to merge").create("r"))
                .addOption(OptionBuilder.hasArg().withArgName("n").withLongOpt("parallel").withDescription("use 'n' tasks to merge in parallel").create("m"))
                //最小收益值，只有收益大于此值才会进行合并
                .addOption(OptionBuilder.hasArg().withArgName("n").withLongOpt("min-profit").withDescription("min-profit,default 1,").create("x"))
                .addOption(OptionBuilder.withType(Integer.class).hasArg().withLongOpt("files").withDescription("how many files will merged into").create("fs"))
                .addOption(OptionBuilder.withLongOpt("incremental").withDescription("incremental merge data ").create("i"))
                .addOption(OptionBuilder.hasArg().withLongOpt("dataPath").withDescription("store the metadata of merged data.the dpath is required, when the -i option are specified").create("dp"));
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new HTool(), args));
    }

    @Override
    public int run(String[] args) throws Exception {

        try {
            CommandLineParser parser = new GnuParser();
            CommandLine cmdLine;
            try {
                cmdLine = parser.parse(hToolOpts, args);
                validation(cmdLine);
            } catch (Exception e) {
                System.err.println(e.getMessage());
                System.err.println(usagePrefix);
                System.err.println(printHelp());
                return 1;
            }
            connectionManger = new ConnectionManger(sts, user, password);
            hTableSverice = new HTableSverice(connectionManger);
            dataFileManager = new DataFileManager(incremental, dataPath);
            taskQueue = new LinkedBlockingQueue<>(TASK_BUFFER);
            mergeThreads = new ArrayList<>();
            MergeThead mergeThread;
            for (int i = 0; i < parallel; i++) {
                mergeThread = new MergeThead(i, hTableSverice, dataFileManager, profit);
                mergeThreads.add(mergeThread);
                mergeThread.start();
            }
            addShutdownHook();
            List<String> tables = getTables();
            if (CollectionUtils.isEmpty(tables)) {
                LOGGER.info("database has no table! db:{}", database);
                return 0;
            }
            Table hiveTable;
            for (String tbl : tables) {
                if (isShutdown()) {
                    break;
                }
                LOGGER.info("TABLE :{}", tbl);
                hiveTable = hTableSverice.getTable(tbl);
                if (hiveTable.getNumBuckets() > 0) {
                    LOGGER.warn("this is a bucket table!table:{}", tbl);
                    continue;
                }
                String tableLocation = hiveTable.getDataLocation().toUri().getPath();
                String cols = getCols(hiveTable);
                if (StringUtils.isEmpty(cols)) {
                    continue;
                }
                if (hiveTable.isPartitioned()) {
                    List<String> parts;
                    if (null != partition) {
                        parts = Lists.newArrayList(partition);
                    } else {
                        parts = hTableSverice.getTableParts(tbl);
                    }
                    if (CollectionUtils.isEmpty(parts)) {
                        LOGGER.info("this table has no partitions!table:{}", tbl);
                        continue;
                    }
                    for (String part : parts) {
                        if (isShutdown()) {
                            break;
                        }
                        if (null != part) {
                            taskQueue.put(new MergeTask(files, tbl, cols, tableLocation, part));
                        }
                    }
                } else {
                    taskQueue.put(new MergeTask(files, tbl, cols, tableLocation, null));
                }
            }
            while (!isShutdown() && !taskQueue.isEmpty()) {
                Thread.sleep(500);
            }
        } finally {
            SHUTDOWN.set(true);
            if (null != mergeThreads) {
                for (MergeThead thread : mergeThreads) {
                    thread.join();
                }
            }
            HDFSUtil.close();
            if (connectionManger != null) {
                connectionManger.close();
            }
            if (dataFileManager != null) {
                dataFileManager.close();
            }
        }

        return 0;
    }

    public String printHelp() {
        StringWriter sw = new StringWriter();
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(sw);
        formatter.printOptions(pw, formatter.getWidth(), hToolOpts, 0, 4);
        sw.flush();
        return sw.getBuffer().toString();
    }

    private void validation(CommandLine cmdLine) {
        incremental = cmdLine.hasOption("i");
        if (incremental && StringUtils.isEmpty(cmdLine.getOptionValue("dp"))) {
            throw new IllegalArgumentException("the dpath is required, when the -i option is specified");
        }
        dataPath = cmdLine.getOptionValue("dp");
        database = cmdLine.getOptionValue("d");
        table = cmdLine.getOptionValue("t");
        partition = cmdLine.getOptionValue("r");
        parallel = Integer.valueOf(cmdLine.getOptionValue("m", "1"));
        files = Integer.valueOf(cmdLine.getOptionValue("fs", "0"));
        sts = cmdLine.getOptionValue("sts");
        user = cmdLine.getOptionValue("u");
        password = cmdLine.getOptionValue("p");
        profit = Integer.valueOf(cmdLine.getOptionValue("x", "1"));
    }


    private String getCols(Table table) {
        StringJoiner cols = new StringJoiner(",");
        table.getCols().forEach(f -> cols.add(f.getName()));
        return cols.toString();
    }


    private List<String> getTables() throws SQLException {
        if (StringUtils.isNotEmpty(table)) {
            if (table.contains(".")) {
                return Lists.newArrayList(table);
            }
            return Lists.newArrayList(database + "." + table);
        }
        return hTableSverice.listTables(database);
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("execute shutdown hook");
            SHUTDOWN.set(true);
        }));
    }

    public static boolean isShutdown() {
        return SHUTDOWN.get();
    }

    public static MergeTask takeTask() throws InterruptedException {
        return taskQueue.poll(200, TimeUnit.MILLISECONDS);
    }

}
