package org.example.utils;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;

/**
 * Spark 配置类
 * 配置 SparkSession 以支持远程 Hive 元数据连接
 */
@Configuration
public class SparkConfig {

    @Bean
    public SparkSession getSparkSession() {
        SparkSession spark = SparkSession.builder()
                .appName("LocalHiveLineageParser")
                .master("local[*]")
                .enableHiveSupport()

                // ========== 远程 Hive Metastore 配置 ==========
                // 连接到远程 Hive 元数据服务，获取表结构信息
                .config("hive.metastore.uris", "thrift://hdp-metastore-etl.58dns.org:9083")

                // ========== ViewFs 配置（解决远程集群 ViewFs 挂载表问题） ==========
                // 将 viewfs://58-cluster/ 路径映射到本地目录，避免访问远程 HDFS
                .config("spark.hadoop.fs.viewfs.mounttable.58-cluster.linkMergeSlash", "/tmp/58-cluster/")
                // 允许空的挂载表，避免初始化失败
                .config("spark.hadoop.fs.viewfs.mount.table.ignore.empty", "true")

                // ========== Hive 临时目录配置 ==========
                // 配置 Hive 执行过程中的临时目录，使用本地路径
                .config("spark.hadoop.hive.exec.stagingdir", "/tmp/hive-staging")
                .config("spark.hadoop.hive.exec.scratchdir", "/tmp/hive-scratch")
                .config("hive.exec.stagingdir", "/tmp/hive-staging")
                .config("hive.exec.scratchdir", "/tmp/hive-scratch")
                // 允许动态分区模式（用于 INSERT INTO 操作）
                .config("hive.exec.dynamic.partition.mode", "nonstrict")

                // ========== 文件系统访问相关配置 ==========
                // 禁用分区路径验证，避免触发文件系统访问
                .config("spark.sql.hive.verifyPartitionPath", "false")
                // 不验证表的位置是否有效（仅用于 SQL 解析，不需要实际访问数据）
                .config("spark.sql.hive.manageFilesourcePartitions", "false")
                // 允许在无法访问文件系统时仍然获取表元数据
                .config("spark.sql.files.ignoreCorruptFiles", "true")
                .config("spark.sql.files.ignoreMissingFiles", "true")
                // 禁用表统计信息收集，避免访问文件系统
                .config("spark.sql.statistics.histogram.enabled", "false")
                .config("spark.sql.statistics.size.autoUpdate.enabled", "false")

                // ========== 类型检查配置 ==========
                // 设置为 LEGACY，允许类型提升（如 string -> bigint, string -> int）
                .config("spark.sql.storeAssignmentPolicy", "LEGACY")
                // 禁用 ANSI 模式，进一步放宽类型限制
                .config("spark.sql.ansi.enabled", "false")

                // ========== 性能优化配置 ==========
                // 禁用优化器（用于 SQL 解析，不需要优化）
                .config("spark.sql.optimizer.enabled", "false")
                // 禁用广播连接
                .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                // 禁用自适应查询执行
                .config("spark.sql.adaptive.enabled", "false")

                // ========== 网络配置 ==========
                // Hive metastore 客户端连接超时时间（5分钟）
                .config("hive.metastore.client.socket.timeout", "300000")

                .getOrCreate();

        // 屏蔽 Spark 冗余日志，只看关键输出
        spark.sparkContext().setLogLevel("ERROR");

        System.out.println("===== SparkSession 创建成功 =====");

        return spark;
    }
}
