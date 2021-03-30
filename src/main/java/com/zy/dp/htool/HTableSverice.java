package com.zy.dp.htool;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhou
 * @date 2021/3/1
 */
public class HTableSverice {

    private static final Logger LOGGER = LoggerFactory.getLogger(HTableSverice.class);

    private HiveConf hiveConf = new HiveConf(HTableSverice.class);
    private ConnectionManger connectionManger;


    public HTableSverice(ConnectionManger connectionManger) {
        this.connectionManger = connectionManger;
    }

    public List<String> listTables(String db) throws SQLException {
        List<String> tables = new ArrayList<>();
        ResultSet resultSet = null;
        try (Connection connection = connectionManger.getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute("use " + db);
            resultSet = statement.executeQuery("show tables ");
            while (resultSet.next()) {
                tables.add(db + "." + resultSet.getString(2));
            }
        } finally {
            if (null != resultSet) {
                resultSet.close();
            }
        }
        tables.sort(String::compareTo);
        LOGGER.info("ALL TABLES:{}", tables);
        return tables;
    }

    public List<String> getTableParts(String table) throws SQLException {
        List<String> parts = new ArrayList<>();
        try (final Connection connection = connectionManger.getConnection();
                final ResultSet resultSet = connection.createStatement().executeQuery("show partitions " + table)) {
            while (resultSet.next()) {
                parts.add(resultSet.getString(1));
            }
        }
        return parts;
    }

    public Table getTable(String dbtable) throws HiveException {
        Hive hive = Hive.get(hiveConf);
        String[] names = dbtable.split("\\.");
        return hive.getTable(names[0], names[1]);
    }

    public boolean mergeTable(String table, String part, int parts, String cols) throws SQLException {
        StringBuilder sqlBuilder = new StringBuilder("insert overwrite table ").append(table);
        String[] patKvs = null;
        if (null != part) {
            sqlBuilder.append(" partition(");
            patKvs = part.split("/");
            for (String patKv : patKvs) {
                final String[] kv = patKv.split("=");
                if (2 != kv.length) {
                    return false;
                }
                sqlBuilder.append(kv[0]).append("='").append(kv[1]).append("',");
            }
            sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
            sqlBuilder.append(")");
        }
        sqlBuilder.append(" select /*+ COALESCE(").append(parts).append(") */ ").append(cols).append(" from ").append(table);
        if (null != part) {
            sqlBuilder.append(" where ");
            for (String patKv : patKvs) {
                final String[] kv = patKv.split("=");
                sqlBuilder.append(kv[0]).append("='").append(kv[1]).append("' and ");
            }
            sqlBuilder.delete(sqlBuilder.length() - 5, sqlBuilder.length());
        }
        String sql = sqlBuilder.toString();
        LOGGER.debug("sql=={}", sql);
        try (Connection connection = connectionManger.getConnection();
                Statement statement = connection.createStatement()) {
            return statement.execute(sql);
        }
    }
}
