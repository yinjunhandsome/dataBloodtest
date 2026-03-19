package org.example.hive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Hive MetaStore 元数据提供者
 * 直接从Hive MetaStore获取表结构，无需手动配置
 */
public class HiveMetaStoreMetadataProvider implements TableMetadataProvider {

    private static final Logger logger = Logger.getLogger(HiveMetaStoreMetadataProvider.class);

    private final HiveMetaStoreClient metaStoreClient;
    private final Map<String, List<String>> tableCache = new ConcurrentHashMap<>();

    /**
     * 构造函数
     * @param metaStoreUris MetaStore的Thrift URI，例如: thrift://localhost:9083
     */
    public HiveMetaStoreMetadataProvider(String metaStoreUris) {
        this.metaStoreClient = createMetaStoreClient(metaStoreUris);
    }

    /**
     * 使用默认URI构造
     */
    public HiveMetaStoreMetadataProvider() {
        this("thrift://localhost:9083");
    }

    private HiveMetaStoreClient createMetaStoreClient(String uris) {
        try {
            HiveConf hiveConf = new HiveConf();
            hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, uris);
            HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
            logger.info("Connected to Hive MetaStore: " + uris);
            return client;
        } catch (Exception e) {
            logger.error("Failed to connect to Hive MetaStore: " + uris, e);
            throw new RuntimeException("Failed to connect to Hive MetaStore", e);
        }
    }

    @Override
    public List<String> getTableColumns(String tableFullName) {
        // 先从缓存获取
        if (tableCache.containsKey(tableFullName)) {
            return tableCache.get(tableFullName);
        }

        // 从MetaStore获取
        try {
            String[] parts = parseTableName(tableFullName);
            String dbName = parts[0];
            String tableName = parts[1];

            Table table = metaStoreClient.getTable(dbName, tableName);
            List<String> columns = new ArrayList<>();

            // 获取所有字段
            for (FieldSchema field : table.getSd().getCols()) {
                columns.add(field.getName());
            }

            // 获取分区字段（如果有）
            if (table.getPartitionKeys() != null) {
                for (FieldSchema partitionKey : table.getPartitionKeys()) {
                    columns.add(partitionKey.getName());
                }
            }

            // 缓存结果
            tableCache.put(tableFullName, columns);
            logger.debug("Got columns for " + tableFullName + ": " + columns);

            return columns;
        } catch (Exception e) {
            logger.error("Failed to get columns for table: " + tableFullName, e);
            return new ArrayList<>();
        }
    }

    @Override
    public boolean tableExists(String tableFullName) {
        try {
            String[] parts = parseTableName(tableFullName);
            return metaStoreClient.tableExists(parts[0], parts[1]);
        } catch (Exception e) {
            logger.error("Failed to check table existence: " + tableFullName, e);
            return false;
        }
    }

    /**
     * 解析表名
     * @param tableFullName 完整表名，格式: db.table 或 table
     * @return [dbName, tableName]
     */
    private String[] parseTableName(String tableFullName) {
        String[] parts = tableFullName.split("\\.");
        if (parts.length == 2) {
            return new String[]{parts[0], parts[1]};
        } else if (parts.length == 1) {
            return new String[]{"default", parts[0]};
        } else {
            throw new IllegalArgumentException("Invalid table name format: " + tableFullName);
        }
    }

    /**
     * 清空缓存
     */
    public void clearCache() {
        tableCache.clear();
    }

    /**
     * 关闭MetaStore连接
     */
    public void close() {
        if (metaStoreClient != null) {
            try {
                metaStoreClient.close();
                logger.info("Closed Hive MetaStore connection");
            } catch (Exception e) {
                logger.error("Failed to close MetaStore connection", e);
            }
        }
    }

    /**
     * 获取所有数据库
     */
    public List<String> getAllDatabases() {
        try {
            return metaStoreClient.getAllDatabases();
        } catch (Exception e) {
            logger.error("Failed to get all databases", e);
            return new ArrayList<>();
        }
    }

    /**
     * 获取指定数据库的所有表
     */
    public List<String> getAllTables(String dbName) {
        try {
            return metaStoreClient.getAllTables(dbName);
        } catch (Exception e) {
            logger.error("Failed to get all tables for database: " + dbName, e);
            return new ArrayList<>();
        }
    }
}
