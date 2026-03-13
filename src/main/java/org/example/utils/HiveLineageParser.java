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

        // 预处理SQL：处理反引号引用的标识符
        String processedSql = preprocessSql(sql);

        ParseDriver parseDriver = new ParseDriver();
        ASTNode astTree = parseDriver.parse(processedSql);

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

        // 根据 SQL 类型处理
        switch (sqlType) {
            case "TOK_CREATETABLE":
            case "TOK_CREATE_TABLE":
                targetTable = extractTargetTable(astTree);
                sourceTables = extractSourceTables(astTree);
                fieldLineagesList = extractFieldLineages(astTree, targetTable);
                break;
            case "TOK_QUERY":
                ASTNode insertIntoNode = findNode(astTree, "TOK_INSERT_INTO");
                if (insertIntoNode != null) {
                    targetTable = extractInsertTargetTable(astTree);
                } else {
                    targetTable = "QUERY_RESULT";
                }
                sourceTables = extractSourceTables(astTree);
                fieldLineagesList = extractFieldLineages(astTree, targetTable);
                break;
            case "TOK_INSERT":
                targetTable = extractInsertTargetTable(astTree);
                sourceTables = extractSourceTables(astTree);
                fieldLineagesList = extractFieldLineages(astTree, targetTable);
                break;
            case "TOK_UNION":
                targetTable = "UNION_RESULT";
                sourceTables = extractSourceTables(astTree);
                fieldLineagesList = extractFieldLineages(astTree, targetTable);
                break;
            case "TOK_WITH_CLAUSE":
            case "TOK_CTE":
                targetTable = "CTE_RESULT";
                sourceTables = extractSourceTables(astTree);
                fieldLineagesList = extractFieldLineages(astTree, targetTable);
                break;
            default:
                sourceTables = extractSourceTables(astTree);
                fieldLineagesList = extractFieldLineages(astTree, targetTable != null ? targetTable : "UNKNOWN");
                break;
        }

        extractJoinInformation(astTree, sourceTables);

        // 转换为 Map 格式并验证元数据
        Map<String, FieldLineage> fieldLineages = new HashMap<>();
        boolean allValidated = true;

        for (FieldLineage lineage : fieldLineagesList) {
            try {
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
                fieldLineages.put(lineage.getFieldName(), lineage);
            } catch (Exception e) {
                allValidated = false;
                System.err.println("Error validating field " + lineage.getTableName() + "." +
                    lineage.getFieldName() + ": " + e.getMessage());
                lineage.setFieldType(FieldLineage.FieldType.ERROR);
                fieldLineages.put(lineage.getFieldName(), lineage);
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
        String tableName = "UNKNOWN";
        ASTNode insertNode = findNode(astTree, "TOK_INSERT_INTO");
        if (insertNode != null && insertNode.getChildCount() > 0) {
            Object child = insertNode.getChild(0);
            if (child instanceof ASTNode) {
                ASTNode tabNode = (ASTNode) child;
                String tabToken = tabNode.getToken() != null ? tabNode.getToken().getText() : "";
                if ("TOK_TAB".equals(tabToken) && tabNode.getChildCount() > 0) {
                    Object tabNameChild = tabNode.getChild(0);
                    if (tabNameChild instanceof ASTNode) {
                        ASTNode tabNameNode = (ASTNode) tabNameChild;
                        String tabNameToken = tabNameNode.getToken() != null ? tabNameNode.getToken().getText() : "";
                        if ("TOK_TABNAME".equals(tabNameToken) && tabNameNode.getChildCount() > 0) {
                            tableName = tabNameNode.getChild(0).toString();
                        }
                    }
                }
            }
        }
        return tableName;
    }

    private static Set<String> extractSourceTables(ASTNode astTree) {
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
                    tables.add(sb.toString());
                }
            }
        });
        return tables;
    }

    private static List<FieldLineage> extractFieldLineages(ASTNode astTree, String targetTable) {
        List<FieldLineage> lineages = new ArrayList<>();
        Map<String, String> tableAliasMap = buildTableAliasMap(astTree);

        ASTNode selectNode = findNode(astTree, "TOK_SELECT");
        if (selectNode != null) {
            for (int i = 0; i < selectNode.getChildCount(); i++) {
                Object child = selectNode.getChild(i);
                if (child instanceof ASTNode) {
                    ASTNode selExprNode = (ASTNode) child;
                    String nodeType = selExprNode.getToken() != null ? selExprNode.getToken().getText() : "";
                    if ("TOK_SELEXPR".equals(nodeType)) {
                        processSelectExpr(selExprNode, targetTable, tableAliasMap, lineages);
                    }
                }
            }
        }

        return lineages;
    }

    private static Map<String, String> buildTableAliasMap(ASTNode astTree) {
        Map<String, String> aliasMap = new HashMap<>();
        findNode(astTree, "TOK_TABREF", node -> {
            ASTNode tabRefNode = (ASTNode) node;
            if (tabRefNode.getChildCount() >= 2) {
                Object child0 = tabRefNode.getChild(0);
                if (child0 instanceof ASTNode) {
                    ASTNode tabNameNode = (ASTNode) child0;
                    Object aliasChild = tabRefNode.getChild(tabRefNode.getChildCount() - 1);
                    String alias = aliasChild.toString();

                    StringBuilder fullName = new StringBuilder();
                    for (int i = 0; i < tabNameNode.getChildCount(); i++) {
                        if (i > 0) fullName.append(".");
                        fullName.append(tabNameNode.getChild(i).toString());
                    }
                    aliasMap.put(alias, fullName.toString());
                }
            } else if (tabRefNode.getChildCount() == 1) {
                Object child0 = tabRefNode.getChild(0);
                if (child0 instanceof ASTNode) {
                    ASTNode tabNameNode = (ASTNode) child0;
                    StringBuilder fullName = new StringBuilder();
                    for (int i = 0; i < tabNameNode.getChildCount(); i++) {
                        if (i > 0) fullName.append(".");
                        fullName.append(tabNameNode.getChild(i).toString());
                    }
                    String tableName = fullName.toString();
                    aliasMap.put(tableName, tableName);
                }
            }
        });
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
            "CREATE TABLE result AS SELECT user_id, count(*) as cnt FROM source_table GROUP BY user_id",
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
