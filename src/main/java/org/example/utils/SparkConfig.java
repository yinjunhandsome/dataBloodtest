package org.example.utils;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Bean
    public SparkSession getSparkSession(){
        SparkSession spark = SparkSession.builder()
                .appName("LocalHiveLineageParser")
                .master("local[*]")
                .enableHiveSupport()
                // ========== 新增：显式绑定远程Hive Metastore地址（核心） ==========
                .config("hive.metastore.uris", "thrift://115.191.22.177:9083")
                // ========== 原有辅助配置保留 ==========
                .config("spark.sql.optimizer.enabled", "false")
                .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                .config("spark.sql.adaptive.enabled", "false")
                .config("hive.metastore.client.socket.timeout", "300000")
                .config("log4j.logger.org.apache.hive", "ERROR")
                .config("log4j.logger.org.apache.hadoop", "ERROR")
                .getOrCreate();
        // 屏蔽Spark冗余日志，只看关键输出
        spark.sparkContext().setLogLevel("ERROR");
      return spark;
    }
}
