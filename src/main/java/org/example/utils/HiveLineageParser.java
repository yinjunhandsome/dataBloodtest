package org.example.utils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.lib.Node;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Hive SQL 字段血缘解析器
 * 基于 Hive Metastore 的字段级别血缘解析，支持绑定元数据验证字段存在性
 */
public class HiveLineageParser {

    private HiveConf hiveConf;
    private HiveMetaStoreClient metaStoreClient;
    private boolean connected = false;

    // CTE 字段血缘缓存
    private Map<String, Map<String, FieldLineage>> cteFieldCache = new HashMap<>();

    /**
     * SQL 解析结果类
     */
    public static class ParseResult {
        private String sqlType;
        private String targetTable;
        private Map<String, FieldLineage> fieldLineages;
        private Set<String> sourceTables;
        private boolean validated;

        public ParseResult(String sqlType, String targetTable,
                          Map<String, FieldLineage> fieldLineages,
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
            for (Map.Entry<String, FieldLineage> entry : fieldLineages.entrySet()) {
                sb.append("  ").append(entry.getKey()).append(":\n");
                sb.append("    ").append(entry.getValue().getLineageDescription());
            }
            return sb.toString();
        }

        public String getSqlType() { return sqlType; }
        public String getTargetTable() { return targetTable; }
        public Map<String, FieldLineage> getFieldLineagesMap() { return fieldLineages; }
        public Collection<FieldLineage> getFieldLineages() { return fieldLineages.values(); }
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
        hiveConf.setBoolean("hive.support.quoted.identifiers", true);
        hiveConf.setBoolean("hive.semantic.analyzer.execute", false);

        try {
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
     * 解析 Hive SQL 并返回字段级别血缘关系（必须连接元数据）
     *
     * @param sql Hive SQL 语句
     * @return ParseResult 包含字段血缘信息的解析结果
     * @throws IllegalStateException 如果未连接到 Metastore
     */
    public ParseResult parseFieldLineage(String sql) throws ParseException, Exception {
        if (!isConnected()) {
            throw new IllegalStateException("必须先连接到 Hive Metastore 才能进行解析。请调用 connect() 方法。");
        }

        // 清空 CTE 缓存
        cteFieldCache.clear();

        // 预处理SQL：处理反引号引用的标识符
        String processedSql = preprocessSql(sql);

        ParseDriver parseDriver = new ParseDriver();
        ASTNode astTree = parseDriver.parse(processedSql);

        // 先解析 CTE 定义并缓存其字段血缘
        parseAndCacheCTEs(astTree);

        // 获取 SQL 类型
        String sqlType = "UNKNOWN";
        List<? extends Node> children = astTree.getChildren();
        if (children != null && !children.isEmpty()) {
            Node firstChild = children.get(0);
            if (firstChild instanceof ASTNode) {
                ASTNode childNode = (ASTNode) firstChild;
                sqlType = childNode.getToken() != null ? childNode.getToken().getText() : "UNKNOWN";
            }
        }

        String targetTable = null;
        List<FieldLineage> fieldLineagesList = new ArrayList<>();
        Set<String> sourceTables = new HashSet<>();

        // 提取 CTE 表名（需要排除，因为它们不是真实表）
        Set<String> cteTableNames = extractCTETableNames(astTree);

        // 检查是否有 INSERT 操作（优先级最高，因为 WITH + INSERT 会先识别为 WITH）
        boolean hasInsert = findNode(astTree, "TOK_INSERT_INTO") != null ||
                          findNode(astTree, "TOK_INSERT") != null;

        // 根据 SQL 类型处理
        if (hasInsert) {
            // INSERT 语句优先处理
            targetTable = extractInsertTargetTable(astTree);
            sourceTables = extractSourceTables(astTree, cteTableNames);
            fieldLineagesList = extractFieldLineages(astTree, targetTable, cteTableNames);
        } else {
            switch (sqlType) {
                case "TOK_CREATETABLE":
                case "TOK_CREATE_TABLE":
                    targetTable = extractTargetTable(astTree);
                    sourceTables = extractSourceTables(astTree, cteTableNames);
                    fieldLineagesList = extractFieldLineages(astTree, targetTable, cteTableNames);
                    break;
                case "TOK_QUERY":
                    targetTable = "QUERY_RESULT";
                    sourceTables = extractSourceTables(astTree, cteTableNames);
                    fieldLineagesList = extractFieldLineages(astTree, targetTable, cteTableNames);
                    break;
                case "TOK_UNION":
                    targetTable = "UNION_RESULT";
                    sourceTables = extractSourceTables(astTree, cteTableNames);
                    fieldLineagesList = extractFieldLineages(astTree, targetTable, cteTableNames);
                    break;
                case "TOK_WITH_CLAUSE":
                case "TOK_CTE":
                    targetTable = "CTE_RESULT";
                    sourceTables = extractSourceTables(astTree, cteTableNames);
                    fieldLineagesList = extractFieldLineages(astTree, targetTable, cteTableNames);
                    break;
                default:
                    sourceTables = extractSourceTables(astTree, cteTableNames);
                    fieldLineagesList = extractFieldLineages(astTree, targetTable != null ? targetTable : "UNKNOWN", cteTableNames);
                    break;
            }
        }

        extractJoinInformation(astTree, sourceTables);

        // 从 sourceTables 中移除 CTE 表名（CTE 不是真实表）
        System.out.println("DEBUG: Original source tables: " + sourceTables);
        System.out.println("DEBUG: Extracted CTE table names: " + cteTableNames);
        System.out.println("DEBUG: Cached CTEs: " + cteFieldCache.keySet());
        Set<String> realSourceTables = new HashSet<>(sourceTables);
        realSourceTables.removeAll(cteTableNames);
        // 更新 sourceTables 为不包含 CTE 的版本
        sourceTables = realSourceTables;
        System.out.println("DEBUG: Final source tables (excluding CTEs): " + sourceTables);

        // 转换为 Map 格式并验证元数据
        Map<String, FieldLineage> fieldLineages = new HashMap<>();
        boolean allValidated = true;

        for (FieldLineage lineage : fieldLineagesList) {
            try {
                // 展开 CTE 字段的血缘
                expandCTEFieldLineage(lineage);

                // 过滤掉仍然指向 CTE 表的依赖（这些没有被正确展开的）
                List<FieldLineage> filteredDependencies = new ArrayList<>();
                for (FieldLineage dep : lineage.getDependencies()) {
                    if (!cteFieldCache.containsKey(dep.getTableName())) {
                        filteredDependencies.add(dep);
                    }
                }
                lineage.getDependencies().clear();
                lineage.getDependencies().addAll(filteredDependencies);

                // 验证依赖字段
                for (FieldLineage dependency : lineage.getDependencies()) {
                    FieldSchema sourceFieldSchema = validateField(
                        dependency.getTableName(),
                        dependency.getFieldName()
                    );
                    if (sourceFieldSchema == null) {
                        allValidated = false;
                        System.out.println("Warning: Source field not found in Metastore: " +
                            dependency.getTableName() + "." + dependency.getFieldName());
                    }
                }
                // 只添加非 UNKNOWN 字段名的血缘
                if (!"UNKNOWN".equals(lineage.getFieldName())) {
                    fieldLineages.put(lineage.getFieldName(), lineage);
                }
            } catch (Exception e) {
                allValidated = false;
                System.err.println("Error validating field " + lineage.getTableName() + "." +
                    lineage.getFieldName() + ": " + e.getMessage());
                lineage.setFieldType(FieldLineage.FieldType.ERROR);
                if (!"UNKNOWN".equals(lineage.getFieldName())) {
                    fieldLineages.put(lineage.getFieldName(), lineage);
                }
            }
        }

        return new ParseResult(sqlType, targetTable, fieldLineages, sourceTables, allValidated);
    }

    /**
     * 获取所有源字段的集合
     *
     * @param fieldLineages 字段血缘映射
     * @return 字段全名 -> 源字段集合
     */
    public static Map<String, Set<String>> getAllSourceFields(Map<String, FieldLineage> fieldLineages) {
        Map<String, Set<String>> result = new HashMap<>();

        for (Map.Entry<String, FieldLineage> entry : fieldLineages.entrySet()) {
            String fieldName = entry.getKey();
            FieldLineage lineage = entry.getValue();

            Set<String> sourceFields = lineage.getAllSourceFields();

            String fullName = lineage.getTableName() != null ?
                lineage.getTableName() + "." + fieldName : fieldName;

            result.put(fullName, sourceFields);
        }

        return result;
    }

    // ==================== 私有辅助方法 ====================

    /**
     * 解析并缓存 CTE 的字段血缘
     */
    private void parseAndCacheCTEs(ASTNode astTree) {
        System.out.println("===== DEBUG: Parsing CTEs =====");
        findNode(astTree, "TOK_CTE", node -> {
            ASTNode cteNode = (ASTNode) node;
            System.out.println("TOK_CTE has " + cteNode.getChildCount() + " children");

            // 一个 TOK_CTE 可能包含多个子节点，每个子节点可能是一个独立的 CTE 定义
            // 遍历所有子节点，查找 CTE 定义（每个 TOK_SUBQUERY 可能是一个 CTE）
            for (int i = 0; i < cteNode.getChildCount(); i++) {
                Object child = cteNode.getChild(i);
                if (!(child instanceof ASTNode)) continue;

                ASTNode childNode = (ASTNode) child;
                String tokenText = childNode.getToken() != null ? childNode.getToken().getText() : "";

                System.out.println("Child " + i + ": type=" + tokenText + ", text=" + childNode.getText() +
                    ", children=" + childNode.getChildCount());

                // 如果是 TOK_SUBQUERY，可能是一个完整的 CTE 定义
                if ("TOK_SUBQUERY".equals(tokenText)) {
                    String cteTableName = null;
                    ASTNode queryNode = null;

                    System.out.println("  Processing as potential CTE definition");

                    // 遍历 TOK_SUBQUERY 的子节点
                    for (int j = 0; j < childNode.getChildCount(); j++) {
                        Object subChild = childNode.getChild(j);
                        if (!(subChild instanceof ASTNode)) continue;

                        ASTNode subChildNode = (ASTNode) subChild;
                        String subToken = subChildNode.getToken() != null ? subChildNode.getToken().getText() : "";

                        System.out.println("    SubChild " + j + ": type=" + subToken + ", text=" + subChildNode.getText());

                        // TOK_QUERY 是查询内容
                        if ("TOK_QUERY".equals(subToken)) {
                            queryNode = subChildNode;
                        }
                        // 表名/别名（不是 TOK_ 开头）
                        else if (!subToken.startsWith("TOK_") && cteTableName == null) {
                            cteTableName = subChildNode.getText();
                            System.out.println("      -> Found CTE table name: " + cteTableName);
                        }
                        // 也可能是 TOK_TABNAME
                        else if ("TOK_TABNAME".equals(subToken) && cteTableName == null) {
                            cteTableName = extractTableNameFromNode(subChildNode);
                            System.out.println("      -> Found CTE table name (TOK_TABNAME): " + cteTableName);
                        }
                    }

                    // 如果找到了表名和查询，缓存这个 CTE
                    if (cteTableName != null && !cteTableName.isEmpty() && queryNode != null) {
                        if ("UNKNOWN".equals(cteTableName)) {
                            System.err.println("    Warning: CTE table name is UNKNOWN, skipping");
                            continue;
                        }

                        System.out.println("  Caching CTE: " + cteTableName);

                        // 递归解析 CTE 查询（支持嵌套 CTE）
                        Map<String, FieldLineage> cteFields = new HashMap<>();
                        Set<String> nestedCteTables = extractCTETableNames(queryNode);

                        List<FieldLineage> fieldLineagesList = extractFieldLineages(queryNode, cteTableName, nestedCteTables);
                        System.out.println("  CTE " + cteTableName + " has " + fieldLineagesList.size() + " fields:");

                        for (FieldLineage lineage : fieldLineagesList) {
                            System.out.println("    - " + lineage.getFieldName() + " with " + lineage.getDependencies().size() + " deps");
                            // 展开 CTE 字段的血缘（处理嵌套 CTE）
                            expandCTEFieldLineage(lineage);
                            cteFields.put(lineage.getFieldName(), lineage);
                        }

                        cteFieldCache.put(cteTableName, cteFields);
                        System.out.println("  Cached " + cteFields.size() + " fields for CTE: " + cteTableName);
                    } else {
                        if (cteTableName == null) {
                            System.err.println("    Warning: Could not extract table name from TOK_SUBQUERY");
                        }
                        if (queryNode == null) {
                            System.err.println("    Warning: Could not find TOK_QUERY in TOK_SUBQUERY");
                        }
                    }
                }
            }
        });
        System.out.println("===== DEBUG: CTE Parsing Complete =====");
    }

    /**
     * 展开字段的 CTE 血缘
     * 如果字段的依赖指向 CTE 表，则用 CTE 的字段血缘替换
     */
    private void expandCTEFieldLineage(FieldLineage lineage) {
        expandCTEFieldLineage(lineage, new HashSet<>());
    }

    /**
     * 展开字段的 CTE 血缘（带循环检测）
     * @param lineage 要展开的字段血缘
     * @param expanding 正在展开的字段集合（用于循环检测）
     */
    private void expandCTEFieldLineage(FieldLineage lineage, Set<String> expanding) {
        if (lineage == null || lineage.getDependencies() == null) {
            return;
        }

        // 循环检测：如果这个字段正在被展开，说明存在循环引用
        String fieldKey = lineage.getTableName() + "." + lineage.getFieldName();
        if (expanding.contains(fieldKey)) {
            System.err.println("Warning: Circular reference detected in CTE: " + fieldKey);
            return;
        }

        List<FieldLineage> originalDependencies = new ArrayList<>(lineage.getDependencies());
        lineage.getDependencies().clear();

        for (FieldLineage dependency : originalDependencies) {
            String depTableName = dependency.getTableName();
            if (cteFieldCache.containsKey(depTableName)) {
                // 这个依赖指向 CTE 表，需要展开
                Map<String, FieldLineage> cteFields = cteFieldCache.get(depTableName);
                FieldLineage cteField = cteFields.get(dependency.getFieldName());

                if (cteField != null) {
                    // 递归展开（CTE 可能依赖其他 CTE）
                    expanding.add(fieldKey);
                    expandCTEFieldLineage(cteField, expanding);
                    expanding.remove(fieldKey);

                    // 合并 CTE 字段的依赖到当前字段
                    for (FieldLineage cteDependency : cteField.getDependencies()) {
                        lineage.addDependency(cloneFieldLineage(cteDependency));
                    }
                } else {
                    // CTE 中找不到该字段，保留原依赖
                    lineage.addDependency(dependency);
                }
            } else {
                // 不是 CTE 表，保留原依赖
                lineage.addDependency(dependency);
            }
        }
    }

    /**
     * 克隆字段血缘对象
     */
    private FieldLineage cloneFieldLineage(FieldLineage original) {
        FieldLineage cloned = new FieldLineage();
        cloned.setFieldName(original.getFieldName());
        cloned.setTableName(original.getTableName());
        cloned.setSourceTableName(original.getSourceTableName());
        cloned.setFieldType(original.getFieldType());
        cloned.setExpression(original.getExpression());

        for (FieldLineage dep : original.getDependencies()) {
            cloned.addDependency(cloneFieldLineage(dep));
        }

        return cloned;
    }

    /**
     * 提取 CTE 表名（WITH 子句中定义的临时表）
     */
    private static Set<String> extractCTETableNames(ASTNode astTree) {
        Set<String> cteTables = new HashSet<>();
        findNode(astTree, "TOK_CTE", node -> {
            ASTNode cteNode = (ASTNode) node;

            // 一个 TOK_CTE 可能包含多个子节点，每个子节点可能是一个独立的 CTE 定义
            // 遍历所有子节点，查找 CTE 定义（每个 TOK_SUBQUERY 可能是一个 CTE）
            for (int i = 0; i < cteNode.getChildCount(); i++) {
                Object child = cteNode.getChild(i);
                if (!(child instanceof ASTNode)) continue;

                ASTNode childNode = (ASTNode) child;
                String tokenText = childNode.getToken() != null ? childNode.getToken().getText() : "";

                // 如果是 TOK_SUBQUERY，可能是一个完整的 CTE 定义
                if ("TOK_SUBQUERY".equals(tokenText)) {
                    String cteTableName = null;

                    // 遍历 TOK_SUBQUERY 的子节点
                    for (int j = 0; j < childNode.getChildCount(); j++) {
                        Object subChild = childNode.getChild(j);
                        if (!(subChild instanceof ASTNode)) continue;

                        ASTNode subChildNode = (ASTNode) subChild;
                        String subToken = subChildNode.getToken() != null ? subChildNode.getToken().getText() : "";

                        // 表名/别名（不是 TOK_ 开头）
                        if (!subToken.startsWith("TOK_") && cteTableName == null) {
                            cteTableName = subChildNode.getText();
                        }
                        // 也可能是 TOK_TABNAME
                        else if ("TOK_TABNAME".equals(subToken) && cteTableName == null) {
                            cteTableName = extractTableNameFromNode(subChildNode);
                        }
                    }

                    if (cteTableName != null && !cteTableName.isEmpty() && !"UNKNOWN".equals(cteTableName)) {
                        cteTables.add(cteTableName);
                    }
                }
            }
        });
        return cteTables;
    }

    private static String extractTargetTable(ASTNode astTree) {
        List<String> tableNameList = new ArrayList<>();
        findNode(astTree, "TOK_TABNAME", node -> {
            if (tableNameList.isEmpty()) {
                ASTNode tabNameNode = (ASTNode) node;
                if (tabNameNode.getChildCount() > 0) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < tabNameNode.getChildCount(); i++) {
                        if (i > 0) sb.append(".");
                        sb.append(tabNameNode.getChild(i).toString());
                    }
                    tableNameList.add(sb.toString());
                }
            }
        });
        return tableNameList.isEmpty() ? "UNKNOWN" : tableNameList.get(0);
    }

    private static String extractInsertTargetTable(ASTNode astTree) {
        // 尝试多种方式查找目标表名

        // 方法1: 查找 TOK_INSERT_INTO (INSERT INTO)
        ASTNode insertIntoNode = findNode(astTree, "TOK_INSERT_INTO");
        if (insertIntoNode != null) {
            String tableName = extractTableNameFromNode(insertIntoNode);
            if (!"UNKNOWN".equals(tableName)) {
                return tableName;
            }
        }

        // 方法2: 查找 TOK_INSERT (INSERT OVERWRITE)
        ASTNode insertNode = findNode(astTree, "TOK_INSERT");
        if (insertNode != null) {
            // 遍历 TOK_INSERT 的子节点，查找第一个 TOK_TABNAME
            List<ASTNode> tabNameNodes = findAllNodes(insertNode, "TOK_TABNAME");
            if (!tabNameNodes.isEmpty()) {
                String tableName = extractTableNameFromNode(tabNameNodes.get(0));
                if (!"UNKNOWN".equals(tableName)) {
                    return tableName;
                }
            }

            // 如果没找到 TOK_TABNAME，尝试从子节点中提取
            if (insertNode.getChildCount() > 0) {
                for (int i = 0; i < insertNode.getChildCount(); i++) {
                    Object child = insertNode.getChild(i);
                    if (child instanceof ASTNode) {
                        ASTNode childNode = (ASTNode) child;
                        String childToken = childNode.getToken() != null ? childNode.getToken().getText() : "";

                        // 可能是 TOK_TAB 或 TOK_TABNAME
                        if ("TOK_TAB".equals(childToken) || "TOK_TABNAME".equals(childToken)) {
                            String tableName = extractTableNameFromNode(childNode);
                            if (!"UNKNOWN".equals(tableName)) {
                                return tableName;
                            }
                        }
                        // 如果是 TOK_TABREF
                        if ("TOK_TABREF".equals(childToken)) {
                            String tableName = extractTableNameFromNode(childNode);
                            if (!"UNKNOWN".equals(tableName)) {
                                return tableName;
                            }
                        }
                    }
                }
            }
        }

        // 方法3: 在整个 AST 中查找 TOK_TABNAME（排除 CTE 和 FROM 子句中的）
        List<ASTNode> allTabNameNodes = findAllNodes(astTree, "TOK_TABNAME");
        Set<String> cteTableNames = extractCTETableNames(astTree);
        Set<String> sourceTables = extractSourceTables(astTree, new HashSet<>());

        // 查找不在 CTE 也不是源表的 TOK_TABNAME
        for (ASTNode tabNameNode : allTabNameNodes) {
            String tableName = extractTableNameFromNode(tabNameNode);
            if (!"UNKNOWN".equals(tableName) &&
                !cteTableNames.contains(tableName) &&
                !sourceTables.contains(tableName)) {
                return tableName;
            }
        }

        // 如果还是找不到，使用第一个非 CTE 的 TOK_TABNAME
        for (ASTNode tabNameNode : allTabNameNodes) {
            String tableName = extractTableNameFromNode(tabNameNode);
            if (!"UNKNOWN".equals(tableName) && !cteTableNames.contains(tableName)) {
                return tableName;
            }
        }

        return "UNKNOWN";
    }

    /**
     * 从节点中提取表名
     */
    private static String extractTableNameFromNode(ASTNode node) {
        if (node == null) {
            return "UNKNOWN";
        }

        String tokenText = node.getToken() != null ? node.getToken().getText() : "";

        // 如果已经是 TOK_TABNAME，提取子节点
        if ("TOK_TABNAME".equals(tokenText)) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < node.getChildCount(); i++) {
                if (i > 0) sb.append(".");
                sb.append(node.getChild(i).toString());
            }
            return sb.length() > 0 ? sb.toString() : "UNKNOWN";
        }

        // 如果是 TOK_TAB，递归处理第一个子节点
        if ("TOK_TAB".equals(tokenText) && node.getChildCount() > 0) {
            Object child = node.getChild(0);
            if (child instanceof ASTNode) {
                return extractTableNameFromNode((ASTNode) child);
            }
        }

        // 如果是 TOK_TABREF，处理 TOK_TABNAME 子节点
        if ("TOK_TABREF".equals(tokenText) && node.getChildCount() > 0) {
            Object child = node.getChild(0);
            if (child instanceof ASTNode) {
                return extractTableNameFromNode((ASTNode) child);
            }
        }

        return "UNKNOWN";
    }

    private static Set<String> extractSourceTables(ASTNode astTree, Set<String> excludeTables) {
        Set<String> tables = new HashSet<>();
        findNode(astTree, "TOK_TABREF", node -> {
            ASTNode tabRefNode = (ASTNode) node;
            if (tabRefNode.getChildCount() > 0) {
                Object child = tabRefNode.getChild(0);
                if (child instanceof ASTNode) {
                    ASTNode tabNameNode = (ASTNode) child;
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < tabNameNode.getChildCount(); i++) {
                        if (i > 0) sb.append(".");
                        sb.append(tabNameNode.getChild(i).toString());
                    }
                    String tableName = sb.toString();
                    // 排除 CTE 表名
                    if (!excludeTables.contains(tableName)) {
                        tables.add(tableName);
                    }
                }
            }
        });
        return tables;
    }

    private static List<FieldLineage> extractFieldLineages(ASTNode astTree, String targetTable, Set<String> cteTableNames) {
        List<FieldLineage> lineages = new ArrayList<>();

        // 检查目标表是否是 CTE 表，如果是，则不排除任何表（因为 CTE 查询需要引用其源表）
        Set<String> excludeTables = cteTableNames.contains(targetTable) ?
            new HashSet<>() : cteTableNames;

        Map<String, String> tableAliasMap = buildTableAliasMap(astTree, excludeTables);

        // 处理所有 SELECT 节点（支持 UNION、CTE、子查询等）
        List<ASTNode> selectNodes = findAllNodes(astTree, "TOK_SELECT");

        // 使用 Set 去重，避免 UNION 等场景重复添加相同字段
        Set<String> processedFields = new HashSet<>();

        for (ASTNode selectNode : selectNodes) {
            for (int i = 0; i < selectNode.getChildCount(); i++) {
                Object child = selectNode.getChild(i);
                if (child instanceof ASTNode) {
                    ASTNode selExprNode = (ASTNode) child;
                    String nodeType = selExprNode.getToken() != null ? selExprNode.getToken().getText() : "";
                    if ("TOK_SELEXPR".equals(nodeType)) {
                        // 提取字段名用于去重
                        String fieldName = extractFieldName(selExprNode);
                        String key = targetTable + "." + fieldName;

                        // 只处理未重复的字段
                        if (processedFields.add(key)) {
                            processSelectExpr(selExprNode, targetTable, tableAliasMap, lineages);
                        }
                    }
                }
            }
        }

        return lineages;
    }

    /**
     * 从 TOK_SELEXPR 节点提取字段名
     */
    private static String extractFieldName(ASTNode selExprNode) {
        if (selExprNode.getChildCount() > 0) {
            Object lastChild = selExprNode.getChild(selExprNode.getChildCount() - 1);
            if (lastChild instanceof ASTNode) {
                ASTNode childNode = (ASTNode) lastChild;
                String childText = childNode.getToken() != null ? childNode.getToken().getText() : "";
                // 如果是别名（无子节点且不以 TOK_ 开头）
                if (childNode.getChildCount() == 0 &&
                    !childText.startsWith("TOK_") &&
                    !isOperator(childText)) {
                    return childText;
                }
            }
        }
        // 没有别名，使用表达式作为字段名
        return buildExpressionStringFromNode(selExprNode);
    }

    /**
     * 从节点构建表达式字符串（简化版）
     */
    private static String buildExpressionStringFromNode(ASTNode node) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < node.getChildCount(); i++) {
            Object child = node.getChild(i);
            if (child instanceof ASTNode) {
                ASTNode childNode = (ASTNode) child;
                String tokenText = childNode.getToken() != null ? childNode.getToken().getText() : "";
                if (".".equals(tokenText) && childNode.getChildCount() >= 2) {
                    String left = getNodeText(childNode.getChild(0));
                    String right = getNodeText(childNode.getChild(1));
                    sb.append(left).append(".").append(right);
                } else if ("TOK_TABLE_OR_COL".equals(tokenText) && childNode.getChildCount() > 0) {
                    sb.append(childNode.getChild(0).toString());
                } else if (!tokenText.startsWith("TOK_") && childNode.getChildCount() == 0) {
                    sb.append(tokenText);
                }
            }
        }
        return sb.toString();
    }

    private static Map<String, String> buildTableAliasMap(ASTNode astTree, Set<String> excludeCteTables) {
        Map<String, String> aliasMap = new HashMap<>();

        // 先收集所有 TOK_TABREF 节点
        List<ASTNode> tabRefNodes = new ArrayList<>();
        findNode(astTree, "TOK_TABREF", node -> {
            ASTNode tabRefNode = (ASTNode) node;
            tabRefNodes.add(tabRefNode);
        });

        // 第一步：收集所有可能的表名（包括多级表名的各部分）
        Map<String, String> fullNameMap = new HashMap<>();
        for (ASTNode tabRefNode : tabRefNodes) {
            String fullName = extractTableNameFromNode(tabRefNode);

            // 如果表名包含点（数据库.表格式），添加完整表名和各部分
            if (fullName.contains(".")) {
                String[] parts = fullName.split("\\.");
                String lastPart = parts[parts.length - 1];

                // 添加完整表名
                fullNameMap.put(fullName, fullName);

                // 添加最后一部分（表名）的引用
                if (!fullNameMap.containsKey(lastPart)) {
                    fullNameMap.put(lastPart, fullName);
                }
            } else {
                // 简单表名
                fullNameMap.put(fullName, fullName);
            }
        }

        // 第二步：为每个 TOK_TABREF 构建别名映射
        for (ASTNode tabRefNode : tabRefNodes) {
            // 获取完整表名
            String fullName = null;
            if (tabRefNode.getChildCount() > 0) {
                Object child0 = tabRefNode.getChild(0);
                if (child0 instanceof ASTNode) {
                    ASTNode tabNameNode = (ASTNode) child0;
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < tabNameNode.getChildCount(); i++) {
                        if (i > 0) sb.append(".");
                        sb.append(tabNameNode.getChild(i).toString());
                    }
                    fullName = sb.toString();
                }
            }

            // 如果是 CTE 表，跳过
            if (fullName != null && excludeCteTables.contains(fullName)) {
                continue;
            }

            // 确定别名
            String alias;
            if (tabRefNode.getChildCount() >= 2) {
                Object aliasChild = tabRefNode.getChild(tabRefNode.getChildCount() - 1);
                alias = aliasChild.toString();
            } else if (fullName != null && fullName.contains(".")) {
                // 对于带数据库前缀的表名，使用最后一部分作为别名
                alias = fullName.substring(fullName.lastIndexOf('.') + 1);
            } else {
                alias = fullName != null ? fullName : "UNKNOWN";
            }

            // 只有当别名不在 fullNameMap 中时才添加，避免覆盖
            //（ fullNameMap 中存储的是表名映射，如：表名部分 -> 完整表名）
            if (!fullNameMap.containsKey(alias) || fullNameMap.get(alias).equals(fullName)) {
                // 如果别名就是表名本身，或者别名指向的完整表名就是当前表名
                if (alias.equals(fullName) || (fullName != null && fullNameMap.get(alias) != null && fullNameMap.get(alias).equals(fullName))) {
                    aliasMap.put(alias, fullName);
                }
            }
        }

        return aliasMap;
    }

    private static void processSelectExpr(ASTNode selExprNode, String targetTable,
                                        Map<String, String> tableAliasMap,
                                        List<FieldLineage> lineages) {
        String alias = null;
        List<Object> expressionParts = new ArrayList<>();

        for (int i = 0; i < selExprNode.getChildCount(); i++) {
            Object child = selExprNode.getChild(i);
            if (i == selExprNode.getChildCount() - 1 && child instanceof ASTNode) {
                ASTNode childNode = (ASTNode) child;
                String childText = childNode.getToken() != null ? childNode.getToken().getText() : "";
                if (childNode.getChildCount() == 0 &&
                    !childText.startsWith("TOK_") &&
                    !isOperator(childText)) {
                    alias = childText;
                    continue;
                }
            }
            expressionParts.add(child);
        }

        String expression = buildExpressionString(expressionParts);
        String targetField = alias != null ? alias : (expression.isEmpty() ? "UNKNOWN" : expression);

        Set<FieldReference> sourceFields = extractFieldReferencesFromParts(expressionParts, tableAliasMap);

        FieldLineage targetFieldLineage = new FieldLineage();
        targetFieldLineage.setFieldName(targetField);
        targetFieldLineage.setTableName(targetTable);
        targetFieldLineage.setExpression(expression);
        targetFieldLineage.setFieldType(determineFieldType(expression, sourceFields));

        for (FieldReference sourceField : sourceFields) {
            FieldLineage dependency = new FieldLineage();
            dependency.setFieldName(sourceField.field);
            dependency.setTableName(sourceField.table);
            dependency.setSourceTableName(sourceField.table);
            dependency.setFieldType(FieldLineage.FieldType.COLUMN);
            targetFieldLineage.addDependency(dependency);
        }

        lineages.add(targetFieldLineage);
    }

    private static FieldLineage.FieldType determineFieldType(String expression, Set<FieldReference> sourceFields) {
        if (sourceFields.isEmpty()) {
            return FieldLineage.FieldType.LITERAL;
        }

        if (expression == null || expression.isEmpty()) {
            return FieldLineage.FieldType.COLUMN;
        }

        String upperExpr = expression.toUpperCase();

        if (upperExpr.contains("COUNT(") || upperExpr.contains("SUM(") ||
            upperExpr.contains("AVG(") || upperExpr.contains("MIN(") ||
            upperExpr.contains("MAX(") || upperExpr.contains("GROUP_CONCAT(")) {
            return FieldLineage.FieldType.AGGREGATE;
        }

        if (upperExpr.contains("OVER(")) {
            return FieldLineage.FieldType.WINDOW_FUNCTION;
        }

        if (upperExpr.contains("CASE")) {
            return FieldLineage.FieldType.CALCULATED;
        }

        if (expression.matches(".*[+\\-*/%=<>!&|^~].*")) {
            return FieldLineage.FieldType.CALCULATED;
        }

        if (sourceFields.size() == 1) {
            FieldReference ref = sourceFields.iterator().next();
            if (expression.equals(ref.table + "." + ref.field) || expression.equals(ref.field)) {
                return FieldLineage.FieldType.COLUMN;
            }
        }

        return FieldLineage.FieldType.ALIAS;
    }

    private static String buildExpressionString(List<Object> parts) {
        StringBuilder sb = new StringBuilder();
        for (Object part : parts) {
            if (part instanceof ASTNode) {
                ASTNode node = (ASTNode) part;
                String tokenText = node.getToken() != null ? node.getToken().getText() : "";
                if (".".equals(tokenText)) {
                    if (node.getChildCount() >= 2) {
                        Object left = node.getChild(0);
                        Object right = node.getChild(1);
                        String leftStr = getNodeText(left);
                        String rightStr = getNodeText(right);
                        sb.append(leftStr).append(".").append(rightStr);
                    }
                } else if ("TOK_TABLE_OR_COL".equals(tokenText)) {
                    if (node.getChildCount() > 0) {
                        sb.append(node.getChild(0).toString());
                    }
                } else if ("*".equals(tokenText) || "+".equals(tokenText) || "-".equals(tokenText)) {
                    sb.append(" ").append(tokenText).append(" ");
                } else if ("TOK_FUNCTIONSTAR".equals(tokenText)) {
                    sb.append(tokenText.replace("TOK_", "").toLowerCase()).append("(*)");
                } else if (!tokenText.startsWith("TOK_")) {
                    sb.append(node.toString());
                }
            } else {
                sb.append(part.toString());
            }
        }
        return sb.toString().trim();
    }

    private static String getNodeText(Object node) {
        if (node instanceof ASTNode) {
            ASTNode astNode = (ASTNode) node;
            String tokenText = astNode.getToken() != null ? astNode.getToken().getText() : "";
            if ("TOK_TABLE_OR_COL".equals(tokenText) && astNode.getChildCount() > 0) {
                return astNode.getChild(0).toString();
            }
            return astNode.toString();
        }
        return node.toString();
    }

    private static Set<FieldReference> extractFieldReferencesFromParts(List<Object> parts, Map<String, String> tableAliasMap) {
        Set<FieldReference> references = new HashSet<>();
        for (Object part : parts) {
            if (part instanceof ASTNode) {
                extractFieldReferencesFromNode((ASTNode) part, tableAliasMap, references);
            }
        }
        return references;
    }

    private static void extractFieldReferencesFromNode(ASTNode node, Map<String, String> tableAliasMap, Set<FieldReference> references) {
        if (node == null) {
            return;
        }

        String tokenText = node.getToken() != null ? node.getToken().getText() : "";

        if (".".equals(tokenText) && node.getChildCount() >= 2) {
            String qualifier = null;
            String identifier;

            Object left = node.getChild(0);
            Object right = node.getChild(1);

            if (left instanceof ASTNode) {
                ASTNode leftNode = (ASTNode) left;
                String leftToken = leftNode.getToken() != null ? leftNode.getToken().getText() : "";
                if ("TOK_TABLE_OR_COL".equals(leftToken) && leftNode.getChildCount() > 0) {
                    qualifier = leftNode.getChild(0).toString();
                }
            }

            identifier = getNodeText(right);

            if (identifier != null) {
                String table = qualifier != null ? tableAliasMap.getOrDefault(qualifier, qualifier) : "UNKNOWN";
                if (!"UNKNOWN".equals(table)) {
                    references.add(new FieldReference(table, identifier));
                }
            }
            return;
        }

        if ("TOK_TABLE_OR_COL".equals(tokenText) && node.getChildCount() > 0) {
            String identifier = node.getChild(0).toString();
            if (tableAliasMap.size() == 1) {
                String table = tableAliasMap.values().iterator().next();
                references.add(new FieldReference(table, identifier));
            } else {
                references.add(new FieldReference("UNKNOWN", identifier));
            }
            return;
        }

        if ("TOK_ALLCOLREF".equals(tokenText)) {
            if (node.getChildCount() > 0) {
                Object child = node.getChild(0);
                if (child instanceof ASTNode) {
                    ASTNode childNode = (ASTNode) child;
                    String qualifier = childNode.getText();
                    String table = tableAliasMap.getOrDefault(qualifier, qualifier);
                    if (!"UNKNOWN".equals(table)) {
                        references.add(new FieldReference(table, "*"));
                    }
                }
            }
            return;
        }

        if ("TOK_FUNCTION".equals(tokenText) || "TOK_FUNCTIONSTAR".equals(tokenText) ||
            "TOK_FUNCTIONDI".equals(tokenText) || "TOK_FUNCTIONNZ".equals(tokenText)) {
            List<? extends Node> children = node.getChildren();
            if (children != null) {
                for (int i = 1; i < children.size(); i++) {
                    if (children.get(i) instanceof ASTNode) {
                        extractFieldReferencesFromNode((ASTNode) children.get(i), tableAliasMap, references);
                    }
                }
            }
            return;
        }

        if ("TOK_CASE_EXPR".equals(tokenText)) {
            List<? extends Node> children = node.getChildren();
            if (children != null) {
                for (Node child : children) {
                    if (child instanceof ASTNode) {
                        ASTNode childNode = (ASTNode) child;
                        String childType = childNode.getToken() != null ? childNode.getToken().getText() : "";
                        if ("TOK_WHEN".equals(childType) || "TOK_ELSE".equals(childType)) {
                            extractFieldReferencesFromNode(childNode, tableAliasMap, references);
                        }
                    }
                }
            }
            return;
        }

        if ("TOK_CAST".equals(tokenText)) {
            if (node.getChildCount() > 0) {
                Object child = node.getChild(0);
                if (child instanceof ASTNode) {
                    extractFieldReferencesFromNode((ASTNode) child, tableAliasMap, references);
                }
            }
            return;
        }

        if ("TOK_SUBQUERY".equals(tokenText) || "TOK_QUERY".equals(tokenText)) {
            extractFieldReferencesFromNode(node, tableAliasMap, references);
            return;
        }

        if ("TOK_DI".equals(tokenText) || "TOK_ALL".equals(tokenText)) {
            List<? extends Node> children = node.getChildren();
            if (children != null) {
                for (Node child : children) {
                    if (child instanceof ASTNode) {
                        extractFieldReferencesFromNode((ASTNode) child, tableAliasMap, references);
                    }
                }
            }
            return;
        }

        if (isOperator(tokenText)) {
            List<? extends Node> children = node.getChildren();
            if (children != null) {
                for (Node child : children) {
                    if (child instanceof ASTNode) {
                        extractFieldReferencesFromNode((ASTNode) child, tableAliasMap, references);
                    }
                }
            }
            return;
        }

        if ("TOK_IS_NULL".equals(tokenText) || "TOK_IS_NOT_NULL".equals(tokenText)) {
            if (node.getChildCount() > 0) {
                Object child = node.getChild(0);
                if (child instanceof ASTNode) {
                    extractFieldReferencesFromNode((ASTNode) child, tableAliasMap, references);
                }
            }
            return;
        }

        if ("TOK_IN".equals(tokenText) || "TOK_NOT_IN".equals(tokenText) ||
            "TOK_EXISTS".equals(tokenText) || "TOK_NOT_EXISTS".equals(tokenText)) {
            List<? extends Node> children = node.getChildren();
            if (children != null) {
                for (Node child : children) {
                    if (child instanceof ASTNode) {
                        extractFieldReferencesFromNode((ASTNode) child, tableAliasMap, references);
                    }
                }
            }
            return;
        }

        if ("TOK_BETWEEN".equals(tokenText)) {
            List<? extends Node> children = node.getChildren();
            if (children != null) {
                for (Node child : children) {
                    if (child instanceof ASTNode) {
                        extractFieldReferencesFromNode((ASTNode) child, tableAliasMap, references);
                    }
                }
            }
            return;
        }

        if ("TOK_LIKE".equals(tokenText) || "TOK_RLIKE".equals(tokenText)) {
            List<? extends Node> children = node.getChildren();
            if (children != null) {
                for (Node child : children) {
                    if (child instanceof ASTNode) {
                        extractFieldReferencesFromNode((ASTNode) child, tableAliasMap, references);
                    }
                }
            }
            return;
        }

        List<? extends Node> children = node.getChildren();
        if (children != null) {
            for (Node child : children) {
                if (child instanceof ASTNode) {
                    extractFieldReferencesFromNode((ASTNode) child, tableAliasMap, references);
                }
            }
        }
    }

    private static class FieldReference {
        String table;
        String field;

        FieldReference(String table, String field) {
            this.table = table;
            this.field = field;
        }
    }

    private static boolean isOperator(String text) {
        return text.matches("[+\\-*/=<>!&|^~%]+");
    }

    private static ASTNode findNode(ASTNode root, String nodeType) {
        if (root == null) {
            return null;
        }
        String currentType = root.getToken() != null ? root.getToken().getText() : "";
        if (nodeType.equals(currentType)) {
            return root;
        }
        List<? extends Node> children = root.getChildren();
        if (children != null) {
            for (Node child : children) {
                if (child instanceof ASTNode) {
                    ASTNode result = findNode((ASTNode) child, nodeType);
                    if (result != null) {
                        return result;
                    }
                }
            }
        }
        return null;
    }

    /**
     * 查找所有指定类型的节点并返回列表
     */
    private static List<ASTNode> findAllNodes(ASTNode root, String nodeType) {
        List<ASTNode> results = new ArrayList<>();
        findNode(root, nodeType, node -> {
            if (node instanceof ASTNode) {
                results.add((ASTNode) node);
            }
        });
        return results;
    }

    private static void findNode(ASTNode root, String nodeType, java.util.function.Consumer<Node> callback) {
        if (root == null) {
            return;
        }
        String currentType = root.getToken() != null ? root.getToken().getText() : "";
        if (nodeType.equals(currentType)) {
            callback.accept(root);
        }
        List<? extends Node> children = root.getChildren();
        if (children != null) {
            for (Node child : children) {
                if (child instanceof ASTNode) {
                    findNode((ASTNode) child, nodeType, callback);
                }
            }
        }
    }

    private static String preprocessSql(String sql) {
        StringBuilder result = new StringBuilder();
        int length = sql.length();
        int i = 0;

        while (i < length) {
            char c = sql.charAt(i);

            if (c == '`') {
                i++;
                StringBuilder identifier = new StringBuilder();

                while (i < length && sql.charAt(i) != '`') {
                    identifier.append(sql.charAt(i));
                    i++;
                }

                if (i < length && sql.charAt(i) == '`') {
                    i++;
                }

                String normalizedIdentifier = normalizeIdentifier(identifier.toString());
                result.append(normalizedIdentifier);
            } else {
                result.append(c);
                i++;
            }
        }

        return result.toString();
    }

    private static String normalizeIdentifier(String identifier) {
        StringBuilder normalized = new StringBuilder();
        for (int i = 0; i < identifier.length(); i++) {
            char c = identifier.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '_') {
                normalized.append(c);
            } else {
                normalized.append('_');
            }
        }
        return normalized.toString();
    }

    private static void extractJoinInformation(ASTNode astTree, Set<String> sourceTables) {
        String[] joinTypes = {
            "TOK_JOIN", "TOK_LEFTJOIN", "TOK_LEFTOUTERJOIN",
            "TOK_RIGHTJOIN", "TOK_RIGHTOUTERJOIN", "TOK_FULLOUTERJOIN",
            "TOK_CROSSJOIN", "TOK_LEFTSEMIJOIN",
            "TOK_LATERAL_VIEW", "TOK_LATERAL_VIEW_OUTER"
        };

        for (String joinType : joinTypes) {
            findNode(astTree, joinType, node -> {
                extractTablesFromNode((ASTNode) node, sourceTables);
            });
        }
    }

    private static void extractTablesFromNode(ASTNode node, Set<String> sourceTables) {
        if (node == null) {
            return;
        }

        String tokenText = node.getToken() != null ? node.getToken().getText() : "";

        if ("TOK_TABREF".equals(tokenText)) {
            if (node.getChildCount() > 0) {
                Object child = node.getChild(0);
                if (child instanceof ASTNode) {
                    ASTNode tabNameNode = (ASTNode) child;
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < tabNameNode.getChildCount(); i++) {
                        if (i > 0) sb.append(".");
                        sb.append(tabNameNode.getChild(i).toString());
                    }
                    sourceTables.add(sb.toString());
                }
            }
            return;
        }

        List<? extends Node> children = node.getChildren();
        if (children != null) {
            for (Node child : children) {
                if (child instanceof ASTNode) {
                    extractTablesFromNode((ASTNode) child, sourceTables);
                }
            }
        }
    }

    /**
     * 测试方法
     */
    public static void main(String[] args) {
        HiveLineageParser parser = new HiveLineageParser();

        String[] testSqls = {
            "insert overwrite table hdp_teu_dpd_feature_db.hbg_user_action_log partition(dt = '${dateSuffix}') select imei, userid, pagetype, actiontype, wuxian_data from hdp_teu_dpd_wx_flow.dwd_wx_flow_58app_hbg_and_58local_action_view where dt = '${#date(0,0,-1):yyyyMMdd#}' and cate1 = '1' and actiontype in ('200000006713008400000100','200000006941031200000001','200000005529000100000100','200000005781000100000100')",
            "with entry_data as (select dt,case when actiontype = 'NMF5645' then '奢品馆-精选-秒杀栏目' when actiontype = 'FMF5404' and datapool['refpagetype'] = 'G1002' then '奢品馆-包袋-秒杀栏目' when actiontype = 'CMF4799' and datapool['refpagetype'] = 'G1002' then '奢品馆-腕表-秒杀栏目' when actiontype = 'FMF5404' and datapool['refpagetype'] = 'F5143' then '包袋频道页-秒杀' when actiontype = 'FMF4721' then '首饰频道页-秒杀' when actiontype = 'CMF4799' and datapool['refpagetype'] = 'F5143' then '腕表频道页-秒杀' when actiontype = 'FMF5404' and datapool['refpagetype'] = 'G1001' then '包袋TAB页-秒杀' when actiontype = 'WMF7002' then '首饰TAB页-秒杀' when actiontype = 'CMF4799' and datapool['refpagetype'] = 'G1001' then '腕表TAB页-秒杀' when actiontype = 'FMF5404' then '包袋_秒杀_其它' when actiontype = 'CMF4799' then '腕表_秒杀_其它' else '其它' end entry,token from hdp_zhuanzhuan_dw_global.dw_log_lego_action_1d where dt=20260313 and pagetype = 'zpmshow' and actiontype in ('NMF5645','FMF5404','CMF4799','FMF4721','WMF7002') and region in ('n','f','c','w')),module_data as (select dt,case when actiontype = 'NMF5645' then '奢品馆-精选-秒杀栏目' when actiontype = 'FMF5404' and datapool['pagequery'] like 'init_from=G1002%' then '奢品馆-包袋-秒杀栏目' when actiontype = 'CMF4799' and datapool['pagequery'] like 'init_from=G1002%' then '奢品馆-腕表-秒杀栏目' when actiontype = 'FMF5404' and datapool['pagequery'] like 'init_from=2_6_18837_0%' then '包袋频道页-秒杀' when actiontype = 'FMF4721' then '首饰频道页-秒杀' when actiontype = 'CMF4799' and datapool['pagequery'] like 'init_from=2_4_18835_0%' then '腕表频道页-秒杀' when actiontype = 'FMF5404' and datapool['pagequery'] like 'init_from=G1001%' then '包袋TAB页-秒杀' when actiontype = 'WMF7002' then '首饰TAB页-秒杀' when actiontype = 'CMF4799' and datapool['pagequery'] like 'init_from=G1001%' then '腕表TAB页-秒杀' when actiontype = 'FMF5404' then '包袋_秒杀_其它' when actiontype = 'CMF4799' then '腕表_秒杀_其它' else '其它' end entry,case when actiontype in ('NMF5645','FMF5404','CMF4799','FMF4721','WMF7002') and datapool['sectionId'] in ('2027090','1963438','1966403','1966370','2008566') then '精选橱窗' else datapool['sortName'] end module,token from hdp_zhuanzhuan_dw_global.dw_log_lego_action_1d where dt=20260313 and pagetype = 'zpmclick' and actiontype in ('NMF5645','FMF5404','CMF4799','FMF4721','WMF7002') and region in ('n','f','c','w') and datapool['sortId'] != '0' and datapool['sectionId'] in ('2027090','1963438','1966403','1966370','2008566') union all select dt,case when actiontype = 'NMF5645' then '奢品馆-精选-秒杀栏目' when actiontype = 'FMF5404' and datapool['pagequery'] like 'init_from=G1002%' then '奢品馆-包袋-秒杀栏目' when actiontype = 'CMF4799' and datapool['pagequery'] like 'init_from=G1002%' then '奢品馆-腕表-秒杀栏目' when actiontype = 'FMF5404' and datapool['pagequery'] like 'init_from=2_6_18837_0%' then '包袋频道页-秒杀' when actiontype = 'FMF4721' then '首饰频道页-秒杀' when actiontype = 'CMF4799' and datapool['pagequery'] like 'init_from=2_4_18835_0%' then '腕表频道页-秒杀' when actiontype = 'FMF5404' and datapool['pagequery'] like 'init_from=G1001%' then '包袋TAB页-秒杀' when actiontype = 'WMF7002' then '首饰TAB页-秒杀' when actiontype = 'CMF4799' and datapool['pagequery'] like 'init_from=G1001%' then '腕表TAB页-秒杀' when actiontype = 'FMF5404' then '包袋_秒杀_其它' when actiontype = 'CMF4799' then '腕表_秒杀_其它' else '其它' end entry,case when actiontype in ('NMF5645','FMF5404','CMF4799','FMF4721','WMF7002') and datapool['sectionId'] in ('2027090','1963438','1966403','1966370','2008566') then '精选橱窗' else datapool['sortName'] end module,token from hdp_zhuanzhuan_dw_global.dw_log_lego_action_1d where dt=20260313 and pagetype = 'zpmclick' and actiontype in ('NMF5645','FMF5404','CMF4799','FMF4721','WMF7002') and region in ('n','f','c','w') and datapool['sortId'] in ('00','01','02','03','04','05','06','07','08') and datapool['sectionId'] not in ('2027090','1963438','1966403','1966370','2008566')) insert OVERWRITE table hdp_ubu_zhuanzhuan_ads_lux.ads_lux_zz_seckill_detail_inc_1d PARTITION(dt='20260313') select dt stat_date,'入口' type,entry,0 module,token from entry_data union all select dt stat_date,'入口模块' type,entry,module,token from module_data",
            "INSERT INTO target_table SELECT col1, col2 * 2 as double_col2 FROM source_table"
        };

        try {
            // 连接到 Metastore
            String metastoreUrl = "thrift://hdp-metastore-etl.58dns.org:9083";
            parser.connect(metastoreUrl);

            for (String sql : testSqls) {
                System.out.println("========================================");
                System.out.println("SQL: " + sql);
                System.out.println("========================================");
                try {
                    ParseResult result = parser.parseFieldLineage(sql);
                    System.out.println(result);

                    System.out.println("Source Fields Mapping:");
                    Map<String, Set<String>> sourceFieldsMap = getAllSourceFields(result.getFieldLineagesMap());
                    for (Map.Entry<String, Set<String>> entry : sourceFieldsMap.entrySet()) {
                        System.out.println("  " + entry.getKey() + " → " + entry.getValue());
                    }
                } catch (Exception e) {
                    System.err.println("Error: " + e.getMessage());
                }
                System.out.println();
            }
        } catch (Exception e) {
            System.err.println("Failed to connect to Metastore: " + e.getMessage());
            System.err.println("Please configure the correct Metastore URL.");
        } finally {
            parser.disconnect();
        }
    }
}
