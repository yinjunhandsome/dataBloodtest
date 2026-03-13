package org.example.utils;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.lib.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HiveLineageParser {

    /**
     * 字段血缘信息类
     */
    public static class FieldLineage {
        protected String targetTable;
        protected String targetField;
        protected String sourceTable;
        protected String sourceField;
        protected String transformExpr;

        public FieldLineage(String targetTable, String targetField, String sourceTable, String sourceField, String transformExpr) {
            this.targetTable = targetTable;
            this.targetField = targetField;
            this.sourceTable = sourceTable;
            this.sourceField = sourceField;
            this.transformExpr = transformExpr;
        }

        @Override
        public String toString() {
            return String.format("%s.%s <- %s.%s (expression: %s)",
                targetTable, targetField, sourceTable, sourceField, transformExpr);
        }

        public String getTargetTable() { return targetTable; }
        public String getTargetField() { return targetField; }
        public String getSourceTable() { return sourceTable; }
        public String getSourceField() { return sourceField; }
        public String getTransformExpr() { return transformExpr; }
    }

    /**
     * SQL 解析结果类
     */
    public static class ParseResult {
        protected String sqlType;
        protected String targetTable;
        protected List<FieldLineage> fieldLineages;
        protected Set<String> sourceTables;

        public ParseResult(String sqlType, String targetTable, List<FieldLineage> fieldLineages, Set<String> sourceTables) {
            this.sqlType = sqlType;
            this.targetTable = targetTable;
            this.fieldLineages = fieldLineages;
            this.sourceTables = sourceTables;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("SQL Type: ").append(sqlType).append("\n");
            sb.append("Target Table: ").append(targetTable).append("\n");
            sb.append("Source Tables: ").append(sourceTables).append("\n");
            sb.append("Field Lineages:\n");
            for (FieldLineage lineage : fieldLineages) {
                sb.append("  ").append(lineage).append("\n");
            }
            return sb.toString();
        }

        public String getSqlType() { return sqlType; }
        public String getTargetTable() { return targetTable; }
        public List<FieldLineage> getFieldLineages() { return fieldLineages; }
        public Set<String> getSourceTables() { return sourceTables; }
    }

    /**
     * 解析 Hive SQL 并返回字段级别血缘关系
     *
     * @param sql Hive SQL 语句
     * @return ParseResult 包含字段血缘信息的解析结果
     */
    public static ParseResult parseFieldLineage(String sql) throws ParseException {
        // 预处理SQL：处理反引号引用的标识符
        String processedSql = preprocessSql(sql);

        ParseDriver parseDriver = new ParseDriver();
        ASTNode astTree = parseDriver.parse(processedSql);

        // 获取 SQL 类型 - 根节点的 token 可能为 null，需要从子节点获取
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
        List<FieldLineage> fieldLineages = new ArrayList<>();
        Set<String> sourceTables = new HashSet<>();

        // 根据 SQL 类型处理
        switch (sqlType) {
            case "TOK_CREATETABLE":
            case "TOK_CREATE_TABLE":
                targetTable = extractTargetTable(astTree);
                sourceTables = extractSourceTables(astTree);
                fieldLineages = extractFieldLineages(astTree, targetTable);
                break;
            case "TOK_QUERY":
                // TOK_QUERY 可能包含 INSERT_INTO 或只是普通查询
                ASTNode insertIntoNode = findNode(astTree, "TOK_INSERT_INTO");
                if (insertIntoNode != null) {
                    targetTable = extractInsertTargetTable(astTree);
                } else {
                    targetTable = "QUERY_RESULT";
                }
                sourceTables = extractSourceTables(astTree);
                fieldLineages = extractFieldLineages(astTree, targetTable);
                break;
            case "TOK_INSERT":
                targetTable = extractInsertTargetTable(astTree);
                sourceTables = extractSourceTables(astTree);
                fieldLineages = extractFieldLineages(astTree, targetTable);
                break;
            case "TOK_UNION":
                // UNION 语句
                targetTable = "UNION_RESULT";
                sourceTables = extractSourceTables(astTree);
                fieldLineages = extractFieldLineages(astTree, targetTable);
                break;
            case "TOK_WITH_CLAUSE":  // CTE (WITH clause)
            case "TOK_CTE":
                targetTable = "CTE_RESULT";
                sourceTables = extractSourceTables(astTree);
                fieldLineages = extractFieldLineages(astTree, targetTable);
                break;
            case "TOK_ALTERTABLE":
            case "TOK_ALTERVIEW":
            case "TOK_DROP_TABLE":
            case "TOK_TRUNCATE_TABLE":
            case "TOK_DELETED":
            case "TOK_UPDATED":
                // DDL 和 DML 操作，只提取源表
                sourceTables = extractSourceTables(astTree);
                targetTable = "DML_OPERATION";
                fieldLineages = extractFieldLineages(astTree, targetTable);
                break;
            default:
                // 其他 SQL 类型，尝试直接处理
                sourceTables = extractSourceTables(astTree);
                fieldLineages = extractFieldLineages(astTree, targetTable != null ? targetTable : "UNKNOWN");
                break;
        }

        // 同时提取 JOIN 信息
        extractJoinInformation(astTree, sourceTables);

        return new ParseResult(sqlType, targetTable, fieldLineages, sourceTables);
    }

    /**
     * 提取目标表名（CREATE TABLE AS）
     */
    private static String extractTargetTable(ASTNode astTree) {
        // 使用列表来保存找到的表名，lambda 可以访问 final 的容器
        List<String> tableNameList = new ArrayList<>();
        findNode(astTree, "TOK_TABNAME", node -> {
            if (tableNameList.isEmpty()) {
                ASTNode tabNameNode = (ASTNode) node;
                // 通常第一个 TOK_TABNAME 是目标表
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

    /**
     * 提取 INSERT 目标表名
     */
    private static String extractInsertTargetTable(ASTNode astTree) {
        String tableName = "UNKNOWN";
        ASTNode insertNode = findNode(astTree, "TOK_INSERT_INTO");
        if (insertNode != null && insertNode.getChildCount() > 0) {
            // TOK_INSERT_INTO 的结构: (TOK_INSERT_INTO (TOK_TAB (TOK_TABNAME table_name)))
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

    /**
     * 提取源表集合
     */
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

    /**
     * 提取字段级别血缘关系
     */
    private static List<FieldLineage> extractFieldLineages(ASTNode astTree, String targetTable) {
        List<FieldLineage> lineages = new ArrayList<>();
        Map<String, String> tableAliasMap = buildTableAliasMap(astTree);

        // 查找 SELECT 子句中的字段
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

    /**
     * 构建表别名映射
     */
    private static Map<String, String> buildTableAliasMap(ASTNode astTree) {
        Map<String, String> aliasMap = new HashMap<>();
        findNode(astTree, "TOK_TABREF", node -> {
            ASTNode tabRefNode = (ASTNode) node;
            // TOK_TABREF 的结构通常是: (TOK_TABREF (TOK_TABNAME db table) alias)
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
                    // 使用表名本身作为别名
                    String tableName = fullName.toString();
                    aliasMap.put(tableName, tableName);
                }
            }
        });
        return aliasMap;
    }

    /**
     * 处理 SELECT 表达式节点
     */
    private static void processSelectExpr(ASTNode selExprNode, String targetTable,
                                        Map<String, String> tableAliasMap,
                                        List<FieldLineage> lineages) {
        // TOK_SELEXPR 的结构: (TOK_SELEXPR expression alias?)
        String alias = null;
        List<Object> expressionParts = new ArrayList<>();

        for (int i = 0; i < selExprNode.getChildCount(); i++) {
            Object child = selExprNode.getChild(i);
            if (i == selExprNode.getChildCount() - 1 && child instanceof ASTNode) {
                ASTNode childNode = (ASTNode) child;
                // 检查是否是别名（通常是简单的标识符）
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

        // 构建表达式字符串和目标字段名
        String expression = buildExpressionString(expressionParts);
        String targetField = alias != null ? alias : (expression.isEmpty() ? "UNKNOWN" : expression);

        // 提取字段引用
        Set<FieldReference> sourceFields = extractFieldReferencesFromParts(expressionParts, tableAliasMap);

        for (FieldReference sourceField : sourceFields) {
            lineages.add(new FieldLineage(
                targetTable,
                targetField,
                sourceField.table,
                sourceField.field,
                expression
            ));
        }
    }

    /**
     * 从表达式部分构建字符串
     */
    private static String buildExpressionString(List<Object> parts) {
        StringBuilder sb = new StringBuilder();
        for (Object part : parts) {
            if (part instanceof ASTNode) {
                ASTNode node = (ASTNode) part;
                String tokenText = node.getToken() != null ? node.getToken().getText() : "";
                if (".".equals(tokenText)) {
                    // 处理点操作符 - 构建 table.field 格式
                    if (node.getChildCount() >= 2) {
                        Object left = node.getChild(0);
                        Object right = node.getChild(1);
                        String leftStr = getNodeText(left);
                        String rightStr = getNodeText(right);
                        sb.append(leftStr).append(".").append(rightStr);
                    }
                } else if ("TOK_TABLE_OR_COL".equals(tokenText)) {
                    // 简单列引用
                    if (node.getChildCount() > 0) {
                        sb.append(node.getChild(0).toString());
                    }
                } else if ("*".equals(tokenText) || "+".equals(tokenText) || "-".equals(tokenText)) {
                    // 算术操作符
                    sb.append(" ").append(tokenText).append(" ");
                } else if ("TOK_FUNCTIONSTAR".equals(tokenText)) {
                    // count(*) 这样的函数
                    sb.append(tokenText.replace("TOK_", "").toLowerCase()).append("(*)");
                } else if (!tokenText.startsWith("TOK_")) {
                    // 字面值
                    sb.append(node.toString());
                }
            } else {
                sb.append(part.toString());
            }
        }
        return sb.toString().trim();
    }

    /**
     * 获取节点文本
     */
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

    /**
     * 从表达式部分提取字段引用
     */
    private static Set<FieldReference> extractFieldReferencesFromParts(List<Object> parts, Map<String, String> tableAliasMap) {
        Set<FieldReference> references = new HashSet<>();
        for (Object part : parts) {
            if (part instanceof ASTNode) {
                extractFieldReferencesFromNode((ASTNode) part, tableAliasMap, references);
            }
        }
        return references;
    }

    /**
     * 从单个节点提取字段引用（增强版，支持更多算子）
     */
    private static void extractFieldReferencesFromNode(ASTNode node, Map<String, String> tableAliasMap, Set<FieldReference> references) {
        if (node == null) {
            return;
        }

        String tokenText = node.getToken() != null ? node.getToken().getText() : "";

        // 处理点操作符 - table.field (这是最主要的字段引用方式)
        if (".".equals(tokenText) && node.getChildCount() >= 2) {
            String qualifier = null;
            String identifier;

            Object left = node.getChild(0);
            Object right = node.getChild(1);

            // 左边可能是表别名
            if (left instanceof ASTNode) {
                ASTNode leftNode = (ASTNode) left;
                String leftToken = leftNode.getToken() != null ? leftNode.getToken().getText() : "";
                if ("TOK_TABLE_OR_COL".equals(leftToken) && leftNode.getChildCount() > 0) {
                    qualifier = leftNode.getChild(0).toString();
                }
            }

            // 右边是字段名
            identifier = getNodeText(right);

            if (identifier != null) {
                String table = qualifier != null ? tableAliasMap.getOrDefault(qualifier, qualifier) : "UNKNOWN";
                // 只有成功解析到表别名时才添加
                if (!"UNKNOWN".equals(table)) {
                    references.add(new FieldReference(table, identifier));
                }
            }
            // 点操作符已经处理了，不需要继续递归
            return;
        }

        // 处理 TOK_TABLE_OR_COL - 简单字段引用（没有表别名的情况）
        if ("TOK_TABLE_OR_COL".equals(tokenText) && node.getChildCount() > 0) {
            String identifier = node.getChild(0).toString();
            // 对于没有表别名的字段，尝试从表别名映射中推断
            // 如果只有一个源表，可以使用它
            if (tableAliasMap.size() == 1) {
                String table = tableAliasMap.values().iterator().next();
                references.add(new FieldReference(table, identifier));
            } else {
                references.add(new FieldReference("UNKNOWN", identifier));
            }
            // TOK_TABLE_OR_COL 已经处理了，不需要继续递归
            return;
        }

        // 处理 TOK_ALLCOLREF - table.* 引用
        if ("TOK_ALLCOLREF".equals(tokenText)) {
            if (node.getChildCount() > 0) {
                Object child = node.getChild(0);
                if (child instanceof ASTNode) {
                    ASTNode childNode = (ASTNode) child;
                    String qualifier = childNode.getText();
                    String table = tableAliasMap.getOrDefault(qualifier, qualifier);
                    if (!"UNKNOWN".equals(table)) {
                        // 添加一个特殊的引用表示所有列
                        references.add(new FieldReference(table, "*"));
                    }
                }
            }
            return;
        }

        // 处理函数调用 - 递归提取函数参数中的字段引用
        if ("TOK_FUNCTION".equals(tokenText) || "TOK_FUNCTIONSTAR".equals(tokenText) ||
            "TOK_FUNCTIONDI".equals(tokenText) || "TOK_FUNCTIONNZ".equals(tokenText)) {
            // 递归处理函数参数
            List<? extends Node> children = node.getChildren();
            if (children != null) {
                for (int i = 1; i < children.size(); i++) {  // 跳过函数名
                    if (children.get(i) instanceof ASTNode) {
                        extractFieldReferencesFromNode((ASTNode) children.get(i), tableAliasMap, references);
                    }
                }
            }
            return;
        }

        // 处理 CASE WHEN 表达式 - 递归提取 WHEN 和 ELSE 部分的字段引用
        if ("TOK_CASE_EXPR".equals(tokenText)) {
            List<? extends Node> children = node.getChildren();
            if (children != null) {
                for (Node child : children) {
                    if (child instanceof ASTNode) {
                        ASTNode childNode = (ASTNode) child;
                        String childType = childNode.getToken() != null ? childNode.getToken().getText() : "";
                        // 递归处理 TOK_WHEN 和 TOK_ELSE 节点
                        if ("TOK_WHEN".equals(childType) || "TOK_ELSE".equals(childType)) {
                            extractFieldReferencesFromNode(childNode, tableAliasMap, references);
                        }
                    }
                }
            }
            return;
        }

        // 处理 CAST 表达式 - 递归提取被转换的表达式
        if ("TOK_CAST".equals(tokenText)) {
            if (node.getChildCount() > 0) {
                Object child = node.getChild(0);
                if (child instanceof ASTNode) {
                    extractFieldReferencesFromNode((ASTNode) child, tableAliasMap, references);
                }
            }
            return;
        }

        // 处理子查询 - 递归提取
        if ("TOK_SUBQUERY".equals(tokenText) || "TOK_QUERY".equals(tokenText)) {
            // 子查询有自己的字段引用，可以递归处理
            extractFieldReferencesFromNode(node, tableAliasMap, references);
            return;
        }

        // 处理聚合函数中的 DISTINCT
        if ("TOK_DI".equals(tokenText) || "TOK_ALL".equals(tokenText)) {
            // 递归处理 DISTINCT 后面的表达式
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

        // 处理算术和逻辑运算符 - 递归处理操作数
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

        // 处理 IS NULL / IS NOT NULL
        if ("TOK_IS_NULL".equals(tokenText) || "TOK_IS_NOT_NULL".equals(tokenText)) {
            if (node.getChildCount() > 0) {
                Object child = node.getChild(0);
                if (child instanceof ASTNode) {
                    extractFieldReferencesFromNode((ASTNode) child, tableAliasMap, references);
                }
            }
            return;
        }

        // 处理 IN 子查询 / EXISTS 子查询
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

        // 处理 BETWEEN 操作符
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

        // 处理 LIKE / RLIKE
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

        // 默认情况：递归处理子节点
        List<? extends Node> children = node.getChildren();
        if (children != null) {
            for (Node child : children) {
                if (child instanceof ASTNode) {
                    extractFieldReferencesFromNode((ASTNode) child, tableAliasMap, references);
                }
            }
        }
    }

    /**
     * 字段引用类
     */
    private static class FieldReference {
        String table;
        String field;

        FieldReference(String table, String field) {
            this.table = table;
            this.field = field;
        }
    }

    /**
     * 检查是否是操作符
     */
    private static boolean isOperator(String text) {
        return text.matches("[+\\-*/=<>!&|^~%]+");
    }

    /**
     * 查找指定类型的节点
     */
    private static ASTNode findNode(ASTNode root, String nodeType) {
        if (root == null) {
            return null;
        }
        // 使用 getToken().getText() 获取节点类型
        String currentType = root.getToken() != null ? root.getToken().getText() : "";
        if (nodeType.equals(currentType)) {
            return root;
        }
        // 安全获取子节点列表
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
     * 查找所有指定类型的节点并执行回调
     */
    private static void findNode(ASTNode root, String nodeType, java.util.function.Consumer<Node> callback) {
        if (root == null) {
            return;
        }
        // 使用 getToken().getText() 获取节点类型
        String currentType = root.getToken() != null ? root.getToken().getText() : "";
        if (nodeType.equals(currentType)) {
            callback.accept(root);
        }
        // 安全获取子节点列表
        List<? extends Node> children = root.getChildren();
        if (children != null) {
            for (Node child : children) {
                if (child instanceof ASTNode) {
                    findNode((ASTNode) child, nodeType, callback);
                }
            }
        }
    }

    public static void main(String[] args) throws ParseException {
        // 测试示例
        String[] testSqls = {
            "insert overwrite table hdp_teu_dpd_feature_db.hbg_user_action_log partition(dt = '${dateSuffix}') select imei, userid, pagetype, actiontype, wuxian_data from hdp_teu_dpd_wx_flow.dwd_wx_flow_58app_hbg_and_58local_action_view where dt = '${#date(0,0,-1):yyyyMMdd#}' and cate1 = '1' and actiontype in ('200000006713008400000100','200000006941031200000001','200000005529000100000100','200000005781000100000100')",
            "CREATE TABLE result AS SELECT user_id, count(*) as cnt FROM source_table GROUP BY user_id",
            "INSERT INTO target_table SELECT col1, col2 * 2 as double_col2 FROM source_table"
        };

        for (String sql : testSqls) {
            System.out.println("========================================");
            System.out.println("SQL: " + sql);
            System.out.println("========================================");
            try {
                ParseResult result = parseFieldLineage(sql);
                System.out.println(result);
            } catch (Exception e) {
                System.err.println("Error parsing SQL: " + e.getMessage());
                e.printStackTrace();
            }
            System.out.println();
        }
    }

    /**
     * 打印 AST 树结构（用于调试）
     */
    private static void printAST(ASTNode node, int level) {
        if (node == null) {
            return;
        }

        StringBuilder indentBuilder = new StringBuilder();
        for (int i = 0; i < level; i++) {
            indentBuilder.append("  ");
        }
        String indent = indentBuilder.toString();

        String tokenText = node.getToken() != null ? node.getToken().getText() : "null";
        String nodeText = node.getText();
        System.out.println(indent + "Token: " + tokenText + ", Text: '" + nodeText + "', Children: " + node.getChildCount());

        List<? extends Node> children = node.getChildren();
        if (children != null) {
            for (Node child : children) {
                if (child instanceof ASTNode) {
                    printAST((ASTNode) child, level + 1);
                } else {
                    System.out.println(indent + "  Child: " + child);
                }
            }
        }
    }

    /**
     * 预处理 SQL 语句，处理反引号引用的标识符
     *
     * 对于包含特殊字符的标识符（如 call/uv），我们将：
     * 1. 移除反引号
     * 2. 将特殊字符替换为下划线，使其成为有效的标识符
     *
     * @param sql 原始 SQL 语句
     * @return 预处理后的 SQL 语句
     */
    private static String preprocessSql(String sql) {
        // 处理反引号引用的标识符
        // 策略：移除反引号，并将标识符中的特殊字符替换为下划线
        StringBuilder result = new StringBuilder();
        int length = sql.length();
        int i = 0;

        while (i < length) {
            char c = sql.charAt(i);

            if (c == '`') {
                // 找到反引号开始的位置，处理反引号内的内容
                i++; // 跳过开始的反引号
                StringBuilder identifier = new StringBuilder();

                while (i < length && sql.charAt(i) != '`') {
                    identifier.append(sql.charAt(i));
                    i++;
                }

                // 跳过结束的反引号
                if (i < length && sql.charAt(i) == '`') {
                    i++;
                }

                // 将标识符中的特殊字符替换为下划线
                String normalizedIdentifier = normalizeIdentifier(identifier.toString());
                result.append(normalizedIdentifier);
            } else {
                result.append(c);
                i++;
            }
        }

        return result.toString();
    }

    /**
     * 规范化标识符，将特殊字符替换为下划线
     *
     * @param identifier 原始标识符
     * @return 规范化后的标识符
     */
    private static String normalizeIdentifier(String identifier) {
        // 将非字母数字下划线的字符替换为下划线
        // 保留字母、数字、下划线，其他字符都替换为下划线
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

    /**
     * 提取 JOIN 信息，识别所有参与的表
     * 支持: INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN, CROSS JOIN, SEMI JOIN
     */
    private static void extractJoinInformation(ASTNode astTree, Set<String> sourceTables) {
        // 查找所有类型的 JOIN
        String[] joinTypes = {
            "TOK_JOIN",           // INNER JOIN
            "TOK_LEFTJOIN",       // LEFT JOIN
            "TOK_LEFTOUTERJOIN",  // LEFT OUTER JOIN
            "TOK_RIGHTJOIN",      // RIGHT JOIN
            "TOK_RIGHTOUTERJOIN", // RIGHT OUTER JOIN
            "TOK_FULLOUTERJOIN",  // FULL OUTER JOIN
            "TOK_CROSSJOIN",      // CROSS JOIN
            "TOK_LEFTSEMIJOIN",   // LEFT SEMI JOIN (IN 子查询)
            "TOK_LATERAL_VIEW",   // LATERAL VIEW
            "TOK_LATERAL_VIEW_OUTER" // LATERAL VIEW OUTER
        };

        for (String joinType : joinTypes) {
            findNode(astTree, joinType, node -> {
                // 从 JOIN 节点提取表引用
                extractTablesFromNode((ASTNode) node, sourceTables);
            });
        }
    }

    /**
     * 从节点及其子节点递归提取表引用
     */
    private static void extractTablesFromNode(ASTNode node, Set<String> sourceTables) {
        if (node == null) {
            return;
        }

        String tokenText = node.getToken() != null ? node.getToken().getText() : "";

        // 如果是表引用节点
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
            return; // TOK_TABREF 已经处理，不需要继续递归
        }

        // 递归处理子节点
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
     * 增强的表达式字符串构建，支持更多操作符
     */
    private static String buildExpressionStringEnhanced(List<Object> parts) {
        StringBuilder sb = new StringBuilder();
        for (Object part : parts) {
            if (part instanceof ASTNode) {
                ASTNode node = (ASTNode) part;
                String tokenText = node.getToken() != null ? node.getToken().getText() : "";

                if (".".equals(tokenText)) {
                    // 处理点操作符 - 构建 table.field 格式
                    if (node.getChildCount() >= 2) {
                        Object left = node.getChild(0);
                        Object right = node.getChild(1);
                        String leftStr = getNodeText(left);
                        String rightStr = getNodeText(right);
                        sb.append(leftStr).append(".").append(rightStr);
                    }
                } else if ("TOK_TABLE_OR_COL".equals(tokenText)) {
                    // 简单列引用
                    if (node.getChildCount() > 0) {
                        sb.append(node.getChild(0).toString());
                    }
                } else if ("*".equals(tokenText) || "+".equals(tokenText) || "-".equals(tokenText) ||
                           "*".equals(tokenText) || "/".equals(tokenText) || "%".equals(tokenText) ||
                           "=".equals(tokenText) || "<".equals(tokenText) || ">".equals(tokenText) ||
                           "<=".equals(tokenText) || ">=".equals(tokenText) || "<>".equals(tokenText) ||
                           "&&".equals(tokenText) || "||".equals(tokenText) || "!".equals(tokenText)) {
                    // 操作符
                    sb.append(" ").append(tokenText).append(" ");
                } else if ("TOK_FUNCTION".equals(tokenText) || "TOK_FUNCTIONSTAR".equals(tokenText)) {
                    // 函数调用
                    sb.append(buildFunctionString(node));
                } else if ("TOK_CASE_EXPR".equals(tokenText)) {
                    // CASE WHEN 表达式
                    sb.append(buildCaseExpressionString(node));
                } else if ("TOK_CAST".equals(tokenText)) {
                    // CAST 表达式
                    sb.append(buildCastExpressionString(node));
                } else if ("TOK_IS_NULL".equals(tokenText) || "TOK_IS_NOT_NULL".equals(tokenText)) {
                    // IS NULL / IS NOT NULL
                    sb.append(tokenText.replace("TOK_", "").toLowerCase().replace("_", " "));
                    if (node.getChildCount() > 0) {
                        sb.append(" ").append(getNodeText(node.getChild(0)));
                    }
                } else if ("TOK_EXISTS".equals(tokenText) || "TOK_NOT_EXISTS".equals(tokenText)) {
                    // EXISTS / NOT EXISTS (子查询)
                    sb.append(tokenText.replace("TOK_", "").toLowerCase());
                } else if ("TOK_SUBQUERY".equals(tokenText)) {
                    // 子查询
                    sb.append("(SELECT ...)");
                } else if ("TOK_ALLCOLREF".equals(tokenText)) {
                    // table.* 引用
                    sb.append(buildAllColRefString(node));
                } else if ("TOK_ANONYMOUS".equals(tokenText)) {
                    // 匿名列
                    // 跳过
                } else if (!tokenText.startsWith("TOK_")) {
                    // 字面值
                    sb.append(node.toString());
                }
            } else {
                sb.append(part.toString());
            }
        }
        return sb.toString().trim();
    }

    /**
     * 构建函数调用字符串
     */
    private static String buildFunctionString(ASTNode functionNode) {
        StringBuilder sb = new StringBuilder();
        String tokenText = functionNode.getToken() != null ? functionNode.getToken().getText() : "";

        if ("TOK_FUNCTIONSTAR".equals(tokenText)) {
            // count(*) 这种情况
            sb.append("count(*)");
        } else if ("TOK_FUNCTION".equals(tokenText)) {
            // 普通函数：function_name(args)
            if (functionNode.getChildCount() > 0) {
                // 第一个子节点通常是函数名
                Object nameChild = functionNode.getChild(0);
                if (nameChild instanceof ASTNode) {
                    ASTNode nameNode = (ASTNode) nameChild;
                    String functionName = nameNode.getText();
                    sb.append(functionName).append("(");

                    // 后续子节点是参数
                    for (int i = 1; i < functionNode.getChildCount(); i++) {
                        if (i > 1) sb.append(", ");
                        sb.append(getNodeText(functionNode.getChild(i)));
                    }

                    sb.append(")");
                }
            }
        }

        return sb.toString();
    }

    /**
     * 构建 CASE WHEN 表达式字符串
     */
    private static String buildCaseExpressionString(ASTNode caseNode) {
        StringBuilder sb = new StringBuilder();
        sb.append("CASE ");

        for (int i = 0; i < caseNode.getChildCount(); i++) {
            Object child = caseNode.getChild(i);
            if (child instanceof ASTNode) {
                ASTNode childNode = (ASTNode) child;
                String childType = childNode.getToken() != null ? childNode.getToken().getText() : "";

                if ("TOK_WHEN".equals(childType)) {
                    // WHEN condition THEN value
                    sb.append("WHEN ");
                    if (childNode.getChildCount() >= 2) {
                        sb.append(getNodeText(childNode.getChild(0)));
                        sb.append(" THEN ");
                        sb.append(getNodeText(childNode.getChild(1)));
                    }
                } else if ("TOK_ELSE".equals(childType)) {
                    // ELSE value
                    sb.append(" ELSE ");
                    if (childNode.getChildCount() > 0) {
                        sb.append(getNodeText(childNode.getChild(0)));
                    }
                }
            }
        }

        sb.append(" END");
        return sb.toString();
    }

    /**
     * 构建 CAST 表达式字符串
     */
    private static String buildCastExpressionString(ASTNode castNode) {
        StringBuilder sb = new StringBuilder();
        sb.append("CAST(");

        // CAST(expression AS type)
        if (castNode.getChildCount() >= 2) {
            sb.append(getNodeText(castNode.getChild(0)));
            sb.append(" AS ");
            sb.append(getNodeText(castNode.getChild(1)));
        }

        sb.append(")");
        return sb.toString();
    }

    /**
     * 构建 table.* 引用字符串
     */
    private static String buildAllColRefString(ASTNode allColNode) {
        StringBuilder sb = new StringBuilder();
        if (allColNode.getChildCount() > 0) {
            sb.append(getNodeText(allColNode.getChild(0)));
        }
        sb.append(".*");
        return sb.toString();
    }
}
