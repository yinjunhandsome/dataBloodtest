package org.example.utils;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;

/**
 * Spark 配置类
 * 配置 SparkSession 以支持远程 Hive 元数据连接
 */
@Configuration
public class SparkConfig {

    // 常见的自定义 Hive UDF 列表（可根据实际情况扩展）
    // 注意：不要包含 Spark 内置函数（如 get_json_object, parse_url 等）
    private static final List<String> CUSTOM_HIVE_UDFS = Arrays.asList(
            "get_biz_name",
            "get_biz_id",
            "get_dept_name",
            "get_user_name"
            // 添加更多自定义 UDF 名称...
    );

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

                // ========== Parquet/Hive 兼容性配置 ==========
                // 禁用 Hive 表到 Parquet 的转换规则，避免 ParquetOptions 初始化失败
                .config("spark.sql.hive.convertMetastoreParquet", "false")
                // 禁用其他文件格式转换
                .config("spark.sql.hive.convertMetastoreOrc", "false")
                // 使用原生 Hive 表扫描，避免触发 Parquet 相关类
                .config("spark.sql.hive.filesourcePartitionPruning", "false")
                // 禁用 Cascading 优化（可能与旧版 Hadoop 不兼容）
                .config("spark.sql.parquet.enableVectorizedReader", "false")
                .config("spark.sql.orc.enableVectorizedReader", "false")
                // 禁用 Parquet schema 合并，避免访问 LZ4 字段
                .config("spark.sql.parquet.mergeSchema", "false")
                // 禁用所有文件格式的转换规则
                .config("spark.sql.hive.convertInsertDontTouchSchema", "true")
                // 强制使用 Hive SerDe，避免 Spark 的 Parquet/Orc 处理器
                .config("spark.sql.hive.useHiveSerDe", "true")
                // 禁用持续的分区发现
                .config("spark.sql.hive.manageFilesourcePartitions", "false")
                .config("spark.sql.streaming.fileSource.compression.codec", "uncompressed")

                // ========== 网络配置 ==========
                // Hive metastore 客户端连接超时时间（5分钟）
                .config("hive.metastore.client.socket.timeout", "300000")

                .getOrCreate();

        // 注册自定义 Hive UDF 的占位符函数
        // 这些 UDF 只用于 SQL 解析和血缘分析，不需要实际执行
        registerPlaceholderUDFs(spark);

        // 屏蔽 Spark 冗余日志，只看关键输出
        spark.sparkContext().setLogLevel("ERROR");

        System.out.println("===== SparkSession 创建成功 =====");

        return spark;

    }

    /**
     * 注册自定义 Hive UDF 的占位符函数
     * 这些函数用于 SQL 解析和血缘分析，不实际执行业务逻辑
     *
     * 使用 Spark 3.x 的 UDF 注册方式，支持可变参数
     *
     * @param spark SparkSession
     */
    private void registerPlaceholderUDFs(SparkSession spark) {
        for (String udfName : CUSTOM_HIVE_UDFS) {
            try {
                // 注册支持可变参数的 UDF
                // 使用 varargs 方式接受任意数量的参数
                spark.udf().register(udfName, (Object... args) -> {
                    // 返回占位符值，实际血缘分析不需要执行真正的 UDF 逻辑
                    return "PLACEHOLDER";
                }, DataTypes.StringType);

                System.out.println("已注册占位符 UDF: " + udfName);
            } catch (Exception e) {
                // 如果 varargs 注册失败，尝试注册单参数版本
                try {
                    spark.udf().register(udfName, (Object input) -> "PLACEHOLDER", DataTypes.StringType);
                    System.out.println("已注册占位符 UDF (单参数版本): " + udfName);
                } catch (Exception e2) {
                    System.err.println("注册 UDF 失败 [" + udfName + "]: " + e2.getMessage());
                }
            }
        }
    }
}
