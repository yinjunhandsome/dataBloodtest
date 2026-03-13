package org.example.utils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ParseException;

import java.util.*;

/**
 * 基于 Hive Metastore 的字段级别血缘解析器
 * 支持绑定元数据，验证字段存在性，获取字段类型等信息
 * 返回与 LogicalPlanLineageParser 统一的 FieldLineage 结构
 */
public class MetaStoreBasedLineageParser extends HiveLineageParser {

    private HiveConf hiveConf;
    private HiveMetaStoreClient metaStoreClient;
    private boolean connected = false;

    /**
     * 解析结果类，与 LogicalPlanLineageParser 保持一致的结构
     */
    public static class ParseResult {
        private String sqlType;
        private String targetTable;
        private Map<String, org.example.utils.FieldLineage> fieldLineages;  // 使用统一的 FieldLineage
        private Set<String> sourceTables;
        private boolean validated;

        public ParseResult(String sqlType, String targetTable,
                          Map<String, org.example.utils.FieldLineage> fieldLineages,
                          Set<String> sourceTables, boolean validated) {
            this.sqlType = sqlType;
            this.targetTable = targetTable;
            this.fieldLineages = fieldLineages;
            this.sourceTables = sourceTables;
            this.validated = validated;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("SQL Type: ").append(sqlType).append("\n");
            sb.append("Target Table: ").append(targetTable).append("\n");
            sb.append("Source Tables: ").append(sourceTables).append("\n");
            sb.append("Validated: ").append(validated ? "Yes" : "No").append("\n");
            sb.append("Field Lineages:\n");
            for (Map.Entry<String, org.example.utils.FieldLineage> entry : fieldLineages.entrySet()) {
                sb.append("  ").append(entry.getKey()).append(":\n");
                sb.append("    ").append(entry.getValue().getLineageDescription());
            }
            return sb.toString();
        }

        public String getSqlType() { return sqlType; }
        public String getTargetTable() { return targetTable; }
        public Map<String, org.example.utils.FieldLineage> getFieldLineagesMap() { return fieldLineages; }
        public Collection<org.example.utils.FieldLineage> getFieldLineages() { return fieldLineages.values(); }
        public Set<String> getSourceTables() { return sourceTables; }
        public boolean isValidated() { return validated; }
    }

    /**
     * 连接到 Hive Metastore
     *
     * @param metastoreUrl Metastore URIs，例如：thrift://localhost:9083
     * @throws Exception 连接失败时抛出
     */
    public void connect(String metastoreUrl) throws Exception {
        hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", metastoreUrl);
        // 启用对反引号引用标识符的支持
        hiveConf.setBoolean("hive.support.quoted.identifiers", true);
        // 禁用语义检查以提高解析器的兼容性
        hiveConf.setBoolean("hive.semantic.analyzer.execute", false);

        try {
            // 尝试连接到 Metastore
            metaStoreClient = new HiveMetaStoreClient(hiveConf);
            connected = true;
            System.out.println("Successfully connected to Hive Metastore: " + metastoreUrl);
        } catch (Exception e) {
            connected = false;
            throw new Exception("Failed to connect to Hive Metastore: " + e.getMessage(), e);
        }
    }

    /**
     * 使用本地 Hive 实例连接（用于测试或本地模式）
     */
    public void connectLocal() throws Exception {
        try {
            Hive hive = Hive.get();
            hiveConf = hive.getConf();
            connected = true;
            System.out.println("Connected to local Hive instance");
        } catch (HiveException e) {
            throw new Exception("Failed to connect to local Hive: " + e.getMessage(), e);
        }
    }

    /**
     * 断开连接
     */
    public void disconnect() {
        if (metaStoreClient != null) {
            metaStoreClient.close();
            metaStoreClient = null;
        }
        connected = false;
    }

    /**
     * 检查是否已连接
     */
    public boolean isConnected() {
        return connected && metaStoreClient != null;
    }

    /**
     * 获取表的字段信息
     *
     * @param tableName 表名，格式：database.table 或 table
     * @return 字段名到字段信息的映射
     */
    public Map<String, FieldSchema> getTableFields(String tableName) throws Exception {
        if (!isConnected()) {
            throw new IllegalStateException("Not connected to Metastore");
        }

        try {
            String dbName = "default";
            String tblName = tableName;

            // 解析 database.table 格式
            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.", 2);
                dbName = parts[0];
                tblName = parts[1];
            }

            Table table = metaStoreClient.getTable(dbName, tblName);
            if (table == null) {
                throw new IllegalArgumentException("Table not found: " + tableName);
            }

            Map<String, FieldSchema> fields = new HashMap<>();
            for (FieldSchema field : table.getSd().getCols()) {
                fields.put(field.getName(), field);
            }

            // 分区字段
            if (table.getPartitionKeys() != null) {
                for (FieldSchema field : table.getPartitionKeys()) {
                    fields.put(field.getName(), field);
                }
            }

            return fields;
        } catch (Exception e) {
            throw new Exception("Failed to get fields for table " + tableName + ": " + e.getMessage(), e);
        }
    }

    /**
     * 验证字段是否存在
     *
     * @param tableName 表名
     * @param fieldName 字段名
     * @return 字段信息，如果不存在返回 null
     */
    public FieldSchema validateField(String tableName, String fieldName) throws Exception {
        Map<String, FieldSchema> fields = getTableFields(tableName);
        return fields.get(fieldName);
    }

    /**
     * 解析字段血缘并绑定元数据（返回统一的 FieldLineage 结构）
     *
     * @param sql Hive SQL 语句
     * @return ParseResult 包含字段血缘信息的解析结果
     */
    public ParseResult parseFieldLineageWithMetadata(String sql) throws ParseException, Exception {
        // 先进行基础解析
        HiveLineageParser.ParseResult baseResult = parseFieldLineage(sql);

        Map<String, org.example.utils.FieldLineage> fieldLineages = new HashMap<>();
        boolean allValidated = true;

        if (!isConnected()) {
            System.out.println("Warning: Not connected to Metastore, returning basic lineage without metadata");
            // 转换为 FieldLineage 格式（没有元数据）
            for (HiveLineageParser.FieldLineage lineage : baseResult.getFieldLineages()) {
                org.example.utils.FieldLineage fieldLineage = convertToFieldLineage(lineage, null);
                fieldLineages.put(lineage.getTargetField(), fieldLineage);
            }
            return new ParseResult(
                baseResult.getSqlType(), baseResult.getTargetTable(),
                fieldLineages, baseResult.getSourceTables(), false
            );
        }

        // 绑定元数据并转换为 FieldLineage 结构
        for (HiveLineageParser.FieldLineage lineage : baseResult.getFieldLineages()) {
            try {
                // 创建目标字段血缘
                org.example.utils.FieldLineage targetField = new org.example.utils.FieldLineage();
                targetField.setFieldName(lineage.getTargetField());
                targetField.setTableName(baseResult.getTargetTable() != null ? baseResult.getTargetTable() : "QUERY_RESULT");

                // 判断字段类型
                org.example.utils.FieldLineage.FieldType fieldType = determineFieldType(lineage);
                targetField.setFieldType(fieldType);

                targetField.setExpression(lineage.getTransformExpr());

                // 创建源字段血缘
                org.example.utils.FieldLineage sourceField = new org.example.utils.FieldLineage();
                sourceField.setFieldName(lineage.getSourceField());
                sourceField.setTableName(lineage.getSourceTable());
                sourceField.setSourceTableName(lineage.getSourceTable()); // 直接源表

                // 验证并获取源字段信息
                FieldSchema sourceFieldSchema = validateField(lineage.getSourceTable(), lineage.getSourceField());
                if (sourceFieldSchema != null) {
                    sourceField.setExpression(null); // 源字段没有表达式
                } else {
                    allValidated = false;
                    System.out.println("Warning: Source field not found in Metastore: " +
                        lineage.getSourceTable() + "." + lineage.getSourceField());
                }

                // 设置依赖关系
                targetField.getDependencies().add(sourceField);

                fieldLineages.put(lineage.getTargetField(), targetField);

            } catch (Exception e) {
                allValidated = false;
                System.err.println("Error validating field " + lineage.getTargetTable() + "." +
                    lineage.getTargetField() + ": " + e.getMessage());
                // 添加未验证的血缘记录
                org.example.utils.FieldLineage targetField = new org.example.utils.FieldLineage();
                targetField.setFieldName(lineage.getTargetField());
                targetField.setTableName(baseResult.getTargetTable());
                targetField.setFieldType(org.example.utils.FieldLineage.FieldType.ERROR);
                targetField.setExpression(lineage.getTransformExpr());
                fieldLineages.put(lineage.getTargetField(), targetField);
            }
        }

        return new ParseResult(
            baseResult.getSqlType(), baseResult.getTargetTable(),
            fieldLineages, baseResult.getSourceTables(), allValidated
        );
    }

    /**
     * 将旧的 FieldLineage 转换为新的 FieldLineage 结构
     */
    private org.example.utils.FieldLineage convertToFieldLineage(HiveLineageParser.FieldLineage oldLineage, FieldSchema fieldSchema) {
        org.example.utils.FieldLineage newLineage = new org.example.utils.FieldLineage();
        newLineage.setFieldName(oldLineage.getTargetField());
        newLineage.setTableName(oldLineage.getTargetTable());
        newLineage.setExpression(oldLineage.getTransformExpr());

        // 判断字段类型
        org.example.utils.FieldLineage.FieldType fieldType = determineFieldType(oldLineage);
        newLineage.setFieldType(fieldType);

        // 如果有元数据，可以设置更多信息
        if (fieldSchema != null) {
            // 可以在这里添加元数据相关的处理
        }

        // 创建源字段依赖
        if (oldLineage.getSourceTable() != null && oldLineage.getSourceField() != null) {
            org.example.utils.FieldLineage sourceField = new org.example.utils.FieldLineage();
            sourceField.setFieldName(oldLineage.getSourceField());
            sourceField.setTableName(oldLineage.getSourceTable());
            sourceField.setSourceTableName(oldLineage.getSourceTable());
            sourceField.setFieldType(org.example.utils.FieldLineage.FieldType.COLUMN);
            newLineage.getDependencies().add(sourceField);
        }

        return newLineage;
    }

    /**
     * 根据血缘信息判断字段类型
     */
    private org.example.utils.FieldLineage.FieldType determineFieldType(HiveLineageParser.FieldLineage lineage) {
        String expression = lineage.getTransformExpr();
        String sourceField = lineage.getSourceField();
        String targetField = lineage.getTargetField();

        // 如果目标字段和源字段不同，说明是别名或计算字段
        if (!targetField.equals(sourceField) && sourceField != null && !sourceField.isEmpty()) {
            // 如果表达式包含函数或操作符，是计算字段
            if (expression != null && (expression.contains("(") || expression.contains("+") ||
                expression.contains("-") || expression.contains("*") || expression.contains("/") ||
                expression.contains("CASE"))) {
                return org.example.utils.FieldLineage.FieldType.CALCULATED;
            }
            return org.example.utils.FieldLineage.FieldType.ALIAS;
        }

        // 如果表达式为空或就是字段名本身，是普通列
        if (expression == null || expression.equals(targetField)) {
            return org.example.utils.FieldLineage.FieldType.COLUMN;
        }

        // 如果包含聚合函数，是聚合字段
        if (expression != null) {
            String upperExpr = expression.toUpperCase();
            if (upperExpr.contains("COUNT(") || upperExpr.contains("SUM(") ||
                upperExpr.contains("AVG(") || upperExpr.contains("MIN(") ||
                upperExpr.contains("MAX(") || upperExpr.contains("GROUP_CONCAT(")) {
                return org.example.utils.FieldLineage.FieldType.AGGREGATE;
            }

            // 窗口函数
            if (upperExpr.contains("OVER(")) {
                return org.example.utils.FieldLineage.FieldType.WINDOW_FUNCTION;
            }

            // 其他情况视为计算字段
            if (expression.contains("(") || expression.matches(".*[+\\-*/].*")) {
                return org.example.utils.FieldLineage.FieldType.CALCULATED;
            }
        }

        return org.example.utils.FieldLineage.FieldType.COLUMN;
    }

    /**
     * 获取所有源字段的集合（与 LogicalPlanLineageParser 的方法签名一致）
     *
     * @param fieldLineages 字段血缘映射
     * @return 字段全名 -> 源字段集合
     */
    public static Map<String, Set<String>> getAllSourceFields(Map<String, org.example.utils.FieldLineage> fieldLineages) {
        Map<String, Set<String>> result = new HashMap<>();

        for (Map.Entry<String, org.example.utils.FieldLineage> entry : fieldLineages.entrySet()) {
            String fieldName = entry.getKey();
            org.example.utils.FieldLineage lineage = entry.getValue();

            // 获取该字段的所有源头字段
            Set<String> sourceFields = lineage.getAllSourceFields();

            // 构建完整字段名
            String fullName = lineage.getTableName() != null ?
                lineage.getTableName() + "." + fieldName : fieldName;

            result.put(fullName, sourceFields);
        }

        return result;
    }

    /**
     * 测试方法
     */
    public static void main(String[] args) {
        MetaStoreBasedLineageParser parser = new MetaStoreBasedLineageParser();

        // 测试 SQL
        String[] testSqls = {
            "insert overwrite table hdp_teu_dpd_feature_db.hbg_user_action_log partition(dt = '${dateSuffix}') select imei, userid, pagetype, actiontype, wuxian_data from hdp_teu_dpd_wx_flow.dwd_wx_flow_58app_hbg_and_58local_action_view where dt = '${#date(0,0,-1):yyyyMMdd#}' and cate1 = '1' and actiontype in ('200000006713008400000100','200000006941031200000001','200000005529000100000100','200000005781000100000100')",
            // 完整的 CTE 示例
            "with entry_data as (select dt,case when actiontype = 'NMF5645' then '奢品馆-精选-秒杀栏目' when actiontype = 'FMF5404' and datapool['refpagetype'] = 'G1002' then '奢品馆-包袋-秒杀栏目' when actiontype = 'CMF4799' and datapool['refpagetype'] = 'G1002' then '奢品馆-腕表-秒杀栏目' when actiontype = 'FMF5404' and datapool['refpagetype'] = 'F5143' then '包袋频道页-秒杀' when actiontype = 'FMF4721' then '首饰频道页-秒杀' when actiontype = 'CMF4799' and datapool['refpagetype'] = 'F5143' then '腕表频道页-秒杀' when actiontype = 'FMF5404' and datapool['refpagetype'] = 'G1001' then '包袋TAB页-秒杀' when actiontype = 'WMF7002' then '首饰TAB页-秒杀' when actiontype = 'CMF4799' and datapool['refpagetype'] = 'G1001' then '腕表TAB页-秒杀' when actiontype = 'FMF5404' then '包袋_秒杀_其它' when actiontype = 'CMF4799' then '腕表_秒杀_其它' else '其它' end entry,token from hdp_zhuanzhuan_dw_global.dw_log_lego_action_1d) select dt, entry, count(*) as cnt from entry_data group by dt, entry",
            // 测试反引号标识符的完整 SQL
            "SELECT user_id, user_name `name`, age, salary/1000 `salary/k`, bonus+100 `bonus+100` FROM employee_table WHERE dept = 'IT'"
        };

        // 方式1：不连接 Metastore（基础血缘）
        System.out.println("===== Testing without Metastore connection =====");
        for (String sql : testSqls) {
            System.out.println("\nSQL: " + sql);
            try {
                ParseResult result = parser.parseFieldLineageWithMetadata(sql);
                System.out.println(result);

                // 额外展示源字段集合
                System.out.println("Source Fields Mapping:");
                Map<String, Set<String>> sourceFieldsMap = getAllSourceFields(result.getFieldLineagesMap());
                for (Map.Entry<String, Set<String>> entry : sourceFieldsMap.entrySet()) {
                    System.out.println("  " + entry.getKey() + " → " + entry.getValue());
                }
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            }
        }

        // 方式2：连接到 Metastore（需要配置正确的 Metastore 地址）
        System.out.println("\n===== Testing with Metastore connection =====");
        try {
            // 替换为你的 Metastore 地址
            String metastoreUrl = "thrift://hdp-metastore-etl.58dns.org:9083";
            parser.connect(metastoreUrl);

            for (String sql : testSqls) {
                System.out.println("\nSQL: " + sql);
                try {
                    ParseResult result = parser.parseFieldLineageWithMetadata(sql);
                    System.out.println(result);

                    // 额外展示源字段集合
                    System.out.println("Source Fields Mapping:");
                    Map<String, Set<String>> sourceFieldsMap = getAllSourceFields(result.getFieldLineagesMap());
                    for (Map.Entry<String, Set<String>> entry : sourceFieldsMap.entrySet()) {
                        System.out.println("  " + entry.getKey() + " → " + entry.getValue());
                    }
                } catch (Exception e) {
                    System.err.println("Error: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to connect to Metastore: " + e.getMessage());
            System.err.println("Please check your Metastore configuration and try again.");
        } finally {
            parser.disconnect();
        }
    }
}
