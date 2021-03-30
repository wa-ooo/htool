package com.zy.dp.htool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.commons.io.IOUtils;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author zhou
 * @date 2021/2/26
 */
public class ConnectionManger implements Closeable {

    HikariDataSource hikariDataSource;

    public ConnectionManger(String url, String user, String password) {
        HikariConfig defaultHiveConnectionConfig = getDefaultHiveConnectionConfig();
        defaultHiveConnectionConfig.setJdbcUrl(url);
        defaultHiveConnectionConfig.setUsername(user);
        defaultHiveConnectionConfig.setPassword(password);
        this.hikariDataSource = new HikariDataSource(defaultHiveConnectionConfig);
    }

    private HikariConfig getDefaultHiveConnectionConfig() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
        hikariConfig.setMaximumPoolSize(100);
        hikariConfig.setMinimumIdle(1);
        hikariConfig.setIdleTimeout(60000);
        hikariConfig.setMaxLifetime(180000);
        hikariConfig.setConnectionTestQuery("select 1");
        return hikariConfig;
    }

    public Connection getConnection() throws SQLException {
        return this.hikariDataSource.getConnection();
    }

    @Override
    public void close() {
        if (null != this.hikariDataSource) {
            IOUtils.closeQuietly(hikariDataSource);
        }
    }
}
