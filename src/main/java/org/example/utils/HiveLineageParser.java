package org.example.utils;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.lib.Node;
import org.example.config.HiveConfig;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
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
@Component
public class HiveLineageParser {

    @Resource
    private HiveConfig hiveConfig;

    // CTE 字段血缘缓存
    private Map<String, Map<String, FieldLineage>> cteFieldCache = new HashMap<>();

    // 子查询字段血缘缓存（用于追踪嵌套子查询的字段来源）
    private Map<String, Map<String, FieldLineage>> subqueryFieldCache = new HashMap<>();

    // 子查询别名到真实表的映射（用于 SELECT * 展开）
    private Map<String, Set<String>> subqueryToRealTablesMap = new HashMap<>();

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
     * 解析 Hive SQL（公共方法，模仿 LogicalPlanLineageParser）
     *
     * @param sql Hive SQL 语句
     * @return 字段名 -> 源字段集合 映射
     */
    public Map<String, Set<String>> parse(String sql) {
        try {
            ParseResult result = parseFieldLineage(sql);
            Map<String, Set<String>> allSourceFields = getAllSourceFields(
                result.getFieldLineagesMap(),
                result.getSqlType()
            );
            for (Map.Entry<String, Set<String>> entry : allSourceFields.entrySet()) {
                System.out.println(entry.getKey() + " → " + entry.getValue());
            }
            return allSourceFields;
        } catch (Exception e) {
            System.err.println("===== Hive SQL 解析失败 =====");
            System.err.println("错误信息：" + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * 解析 Hive SQL 并返回字段级别血缘关系（必须连接元数据）
     *
     * @param sql Hive SQL 语句
     * @return ParseResult 包含字段血缘信息的解析结果
     * @throws IllegalStateException 如果未连接到 Metastore
     */
    public ParseResult parseFieldLineage(String sql) throws ParseException, Exception {
        if (!hiveConfig.isConnected()) {
            throw new IllegalStateException("必须先连接到 Hive Metastore 才能进行解析。");
        }

        // 清空 CTE 和子查询缓存
        cteFieldCache.clear();
        subqueryFieldCache.clear();
        subqueryToRealTablesMap.clear();

        // 预处理SQL：处理反引号引用的标识符
        String processedSql = preprocessSql(sql);

        ParseDriver parseDriver = new ParseDriver();
        ASTNode astTree = parseDriver.parse(processedSql);

        // 先解析 CTE 定义并缓存其字段血缘
        parseAndCacheCTEs(astTree);

        // 解析子查询并缓存其字段血缘（支持嵌套子查询）
        parseAndCacheSubqueries(astTree);

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
            // INSERT 语句（包括 WITH ... INSERT ...）
            targetTable = extractInsertTargetTable(astTree);
            sourceTables = extractSourceTables(astTree, cteTableNames);
            fieldLineagesList = extractFieldLineages(astTree, targetTable, cteTableNames);

            // 修正 sqlType：确保 INSERT 语句有正确的类型标识
            if ("TOK_QUERY".equals(sqlType)) {
                sqlType = "TOK_QUERY_INSERT";
            }
        } else if ("TOK_CREATETABLE".equals(sqlType) || "TOK_CREATE_TABLE".equals(sqlType)) {
            // CREATE TABLE AS SELECT
            targetTable = extractTargetTable(astTree);
            sourceTables = extractSourceTables(astTree, cteTableNames);
            fieldLineagesList = extractFieldLineages(astTree, targetTable, cteTableNames);
        } else {
            // 普通查询（包括 WITH ... SELECT ...）
            // 注意：TOK_WITH_CLAUSE、TOK_CTE、TOK_UNION 等不会作为顶层类型出现
            // 它们都会被识别为 TOK_QUERY，CTE 的处理已在前面完成
            targetTable = "QUERY_RESULT";
            sourceTables = extractSourceTables(astTree, cteTableNames);
            fieldLineagesList = extractFieldLineages(astTree, targetTable, cteTableNames);
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
                // 展开 CTE 和子查询字段的血缘
                expandCTEFieldLineage(lineage);
                expandSubqueryFieldLineage(lineage);

                // 第一步：过滤掉指向 CTE 表和子查询别名的依赖
                List<FieldLineage> filteredDependencies = new ArrayList<>();
                for (FieldLineage dep : lineage.getDependencies()) {
                    if (!cteFieldCache.containsKey(dep.getTableName()) &&
                        !subqueryFieldCache.containsKey(dep.getTableName())) {
                        filteredDependencies.add(dep);
                    }
                }

                // 第二步：验证每个依赖字段是否在 Metastore 中存在
                // 如果不存在，则从依赖列表中移除（不是抛出错误，而是静默移除）
                List<FieldLineage> validatedDependencies = new ArrayList<>();
                for (FieldLineage dependency : filteredDependencies) {
                    try {
                        FieldSchema sourceFieldSchema = hiveConfig.validateField(
                            dependency.getTableName(),
                            dependency.getFieldName()
                        );
                        if (sourceFieldSchema != null) {
                            // 字段存在，保留这个依赖
                            validatedDependencies.add(dependency);
                        } else {
                            // 字段不存在，移除这个依赖（不保留不存在的字段引用）
                            allValidated = false;
                            System.out.println("Info: Source field not found in Metastore, removed from lineage: " +
                                dependency.getTableName() + "." + dependency.getFieldName());
                        }
                    } catch (Exception e) {
                        // 验证失败（如表不存在），移除这个依赖
                        allValidated = false;
                        System.out.println("Info: Failed to validate field, removed from lineage: " +
                            dependency.getTableName() + "." + dependency.getFieldName() + " - " + e.getMessage());
                    }
                }

                // 更新依赖列表为只包含已验证存在的字段
                lineage.getDependencies().clear();
                lineage.getDependencies().addAll(validatedDependencies);

                // 只添加非 UNKNOWN 字段名的血缘
                if (!"UNKNOWN".equals(lineage.getFieldName())) {
                    fieldLineages.put(lineage.getFieldName(), lineage);
                }
            } catch (Exception e) {
                allValidated = false;
                System.err.println("Error processing field " + lineage.getTableName() + "." +
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
     * @param sqlType SQL 类型（TOK_QUERY 或 TOK_INSERT）
     * @return 字段全名 -> 源字段集合
     */
    public static Map<String, Set<String>> getAllSourceFields(Map<String, FieldLineage> fieldLineages, String sqlType) {
        Map<String, Set<String>> result = new HashMap<>();
        boolean isInsert = sqlType != null && sqlType.contains("INSERT");

        System.out.println("DEBUG getAllSourceFields: sqlType=" + sqlType + ", isInsert=" + isInsert);

        for (Map.Entry<String, FieldLineage> entry : fieldLineages.entrySet()) {
            String fieldName = entry.getKey();
            FieldLineage lineage = entry.getValue();

            Set<String> sourceFields = lineage.getAllSourceFields();

            // SELECT 语句：左边只显示字段名；INSERT 语句：左边显示 表名.字段名
            String displayName;
            if (isInsert) {
                displayName = lineage.getTableName() != null ?
                    lineage.getTableName() + "." + fieldName : fieldName;
            } else {
                displayName = fieldName;
            }

            result.put(displayName, sourceFields);
        }

        return result;
    }

    /**
     * 获取所有源字段的集合（保持向后兼容）
     *
     * @param fieldLineages 字段血缘映射
     * @return 字段全名 -> 源字段集合
     */
    public static Map<String, Set<String>> getAllSourceFields(Map<String, FieldLineage> fieldLineages) {
        return getAllSourceFields(fieldLineages, "TOK_QUERY");
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
     * 解析并缓存子查询的字段血缘（用于嵌套子查询的字段追踪）
     */
    private void parseAndCacheSubqueries(ASTNode astTree) {
        System.out.println("===== DEBUG: Parsing Subqueries =====");
        Set<String> processedSubqueries = new HashSet<>();
        parseAndCacheSubqueriesRecursive(astTree, processedSubqueries);
        System.out.println("===== DEBUG: Subquery Parsing Complete, cached " +
            subqueryFieldCache.size() + " subqueries =====");
    }

    /**
     * 递归解析并缓存子查询（从内到外）
     */
    private void parseAndCacheSubqueriesRecursive(ASTNode astTree, Set<String> processedSubqueries) {
        if (astTree == null) {
            return;
        }

        // 查找所有 TOK_SUBQUERY 节点
        List<ASTNode> subqueryNodes = findAllNodes(astTree, "TOK_SUBQUERY");

        for (ASTNode subqueryNode : subqueryNodes) {
            String subqueryAlias = null;
            ASTNode queryNode = null;

            // 遍历 TOK_SUBQUERY 的子节点
            for (int i = 0; i < subqueryNode.getChildCount(); i++) {
                Object child = subqueryNode.getChild(i);
                if (!(child instanceof ASTNode)) continue;

                ASTNode childNode = (ASTNode) child;
                String tokenText = childNode.getToken() != null ? childNode.getToken().getText() : "";

                // TOK_QUERY 是查询内容
                if ("TOK_QUERY".equals(tokenText)) {
                    queryNode = childNode;
                }
                // 表名/别名（不是 TOK_ 开头）
                else if (!tokenText.startsWith("TOK_") && subqueryAlias == null) {
                    subqueryAlias = childNode.getText();
                }
            }

            // 如果找到了查询节点，先递归解析其内部的子查询（从内到外）
            if (queryNode != null) {
                parseAndCacheSubqueriesRecursive(queryNode, processedSubqueries);
            }

            // 然后再处理当前子查询
            if (subqueryAlias != null && !subqueryAlias.isEmpty() &&
                queryNode != null && !"UNKNOWN".equals(subqueryAlias)) {

                // 使用节点对象作为唯一标识，避免重复处理
                String cacheKey = subqueryAlias + "@" + System.identityHashCode(subqueryNode);
                if (!processedSubqueries.contains(cacheKey)) {
                    processedSubqueries.add(cacheKey);

                    System.out.println("  Caching subquery: " + subqueryAlias + " (id=" +
                        Integer.toHexString(System.identityHashCode(subqueryNode)) + ")");

                    // 解析子查询的字段血缘（只使用子查询内部的表别名）
                    Set<String> emptyCteSet = new HashSet<>();
                    List<FieldLineage> fieldLineagesList = extractFieldLineages(
                        queryNode, subqueryAlias, emptyCteSet);

                    Map<String, FieldLineage> subqueryFields = new HashMap<>();
                    for (FieldLineage lineage : fieldLineagesList) {
                        // 展开子查询字段的血缘（包括 CTE 和其他子查询）
                        expandCTEFieldLineage(lineage);
                        expandSubqueryFieldLineage(lineage);
                        subqueryFields.put(lineage.getFieldName(), lineage);
                    }

                    // 使用别名作为 key，允许同名子查询覆盖（外层覆盖内层）
                    subqueryFieldCache.put(subqueryAlias, subqueryFields);
                    System.out.println("  Cached " + subqueryFields.size() +
                        " fields for subquery: " + subqueryAlias);
                }
            }
        }
    }

    /**
     * 展开字段的子查询血缘
     * 如果字段的依赖指向子查询别名，则用子查询的字段血缘替换
     */
    private void expandSubqueryFieldLineage(FieldLineage lineage) {
        expandSubqueryFieldLineage(lineage, new HashSet<>());
    }

    /**
     * 展开字段的子查询血缘（带循环检测）
     */
    private void expandSubqueryFieldLineage(FieldLineage lineage, Set<String> expanding) {
        if (lineage == null || lineage.getDependencies() == null) {
            return;
        }

        // 循环检测
        String fieldKey = lineage.getTableName() + "." + lineage.getFieldName();
        if (expanding.contains(fieldKey)) {
            System.err.println("Warning: Circular reference detected in subquery: " + fieldKey);
            return;
        }

        List<FieldLineage> originalDependencies = new ArrayList<>(lineage.getDependencies());
        lineage.getDependencies().clear();

        for (FieldLineage dependency : originalDependencies) {
            String depTableName = dependency.getTableName();

            // 检查是否是子查询别名
            if (subqueryFieldCache.containsKey(depTableName)) {
                // 这个依赖指向子查询，需要展开
                Map<String, FieldLineage> subqueryFields = subqueryFieldCache.get(depTableName);
                FieldLineage subqueryField = subqueryFields.get(dependency.getFieldName());

                if (subqueryField != null) {
                    // 递归展开（子查询可能依赖其他子查询或 CTE）
                    expanding.add(fieldKey);
                    expandSubqueryFieldLineage(subqueryField, expanding);
                    expandCTEFieldLineage(subqueryField, expanding);
                    expanding.remove(fieldKey);

                    // 合并子查询字段的依赖到当前字段
                    for (FieldLineage subqueryDependency : subqueryField.getDependencies()) {
                        lineage.addDependency(cloneFieldLineage(subqueryDependency));
                    }
                } else {
                    // 子查询中找不到该字段，保留原依赖
                    lineage.addDependency(dependency);
                }
            } else {
                // 不是子查询，保留原依赖
                lineage.addDependency(dependency);
            }
        }
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

    private List<FieldLineage> extractFieldLineages(ASTNode astTree, String targetTable, Set<String> cteTableNames) {
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
                        // 检查是否是 *，如果是则展开
                        if (isSelectStar(selExprNode)) {
                            // 展开 * 为所有字段
                            List<FieldLineage> expandedFields = expandSelectStar(selExprNode, targetTable, tableAliasMap);
                            for (FieldLineage field : expandedFields) {
                                String key = targetTable + "." + field.getFieldName();
                                if (processedFields.add(key)) {
                                    lineages.add(field);
                                }
                            }
                        } else {
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
        }

        return lineages;
    }

    /**
     * 检查 TOK_SELEXPR 是否是 SELECT *
     */
    private boolean isSelectStar(ASTNode selExprNode) {
        if (selExprNode.getChildCount() == 0) {
            return false;
        }

        // 调试：打印子节点信息
        System.out.println("DEBUG isSelectStar: node has " + selExprNode.getChildCount() + " children");
        for (int i = 0; i < selExprNode.getChildCount(); i++) {
            Object child = selExprNode.getChild(i);
            if (child instanceof ASTNode) {
                ASTNode childNode = (ASTNode) child;
                String tokenText = childNode.getToken() != null ? childNode.getToken().getText() : "";
                System.out.println("  Child " + i + ": type=" + tokenText + ", text=" + childNode.getText());
            }
        }

        Object firstChild = selExprNode.getChild(0);
        if (firstChild instanceof ASTNode) {
            ASTNode firstChildNode = (ASTNode) firstChild;
            String tokenText = firstChildNode.getToken() != null ? firstChildNode.getToken().getText() : "";
            // 可能是直接是 "*"
            if ("*".equals(tokenText)) {
                System.out.println("  -> Detected SELECT * (direct)");
                return true;
            }
            // 或者是 TOK_ALLCOLREF
            if ("TOK_ALLCOLREF".equals(tokenText)) {
                System.out.println("  -> Detected SELECT * (TOK_ALLCOLREF)");
                return true;
            }
        }
        System.out.println("  -> Not SELECT *");
        return false;
    }

    /**
     * 展开 SELECT * 为所有字段
     */
    private List<FieldLineage> expandSelectStar(ASTNode selExprNode, String targetTable,
                                                Map<String, String> tableAliasMap) {
        List<FieldLineage> expandedFields = new ArrayList<>();

        // 尝试从第一个子节点获取表限定符
        String qualifier = null;
        if (selExprNode.getChildCount() > 0) {
            Object firstChild = selExprNode.getChild(0);
            if (firstChild instanceof ASTNode) {
                ASTNode firstChildNode = (ASTNode) firstChild;
                String tokenText = firstChildNode.getToken() != null ? firstChildNode.getToken().getText() : "";

                // 如果是 TOK_ALLCOLREF，检查是否有表限定符
                if ("TOK_ALLCOLREF".equals(tokenText)) {
                    if (firstChildNode.getChildCount() > 0) {
                        Object qualifierChild = firstChildNode.getChild(0);
                        if (qualifierChild instanceof ASTNode) {
                            ASTNode qualifierNode = (ASTNode) qualifierChild;
                            String qualifierToken = qualifierNode.getToken() != null ?
                                qualifierNode.getToken().getText() : "";
                            // 如果是 TOK_TABNAME，需要提取子节点的文本
                            if ("TOK_TABNAME".equals(qualifierToken)) {
                                qualifier = extractTableNameFromNode(qualifierNode);
                            } else {
                                qualifier = qualifierNode.getText();
                            }
                        }
                    }
                }
            }
        }

        // 如果没有限定符，且有多个源表，展开所有表的字段
        String tableName = null;
        if (qualifier != null) {
            tableName = tableAliasMap.getOrDefault(qualifier, null);
        }

        // 检查是否是子查询别名
        if (qualifier != null && subqueryFieldCache.containsKey(qualifier)) {
            // 从子查询缓存中获取字段列表
            Map<String, FieldLineage> subqueryFields = subqueryFieldCache.get(qualifier);
            for (Map.Entry<String, FieldLineage> entry : subqueryFields.entrySet()) {
                String fieldName = entry.getKey();
                FieldLineage subqueryField = entry.getValue();

                FieldLineage fieldLineage = new FieldLineage();
                fieldLineage.setFieldName(fieldName);
                fieldLineage.setTableName(targetTable);
                fieldLineage.setExpression(qualifier + ".*");
                fieldLineage.setFieldType(FieldLineage.FieldType.COLUMN);

                // 复制子查询字段的依赖
                for (FieldLineage dep : subqueryField.getDependencies()) {
                    fieldLineage.addDependency(cloneFieldLineage(dep));
                }

                expandedFields.add(fieldLineage);
            }
            System.out.println("Expanded SELECT * from subquery " + qualifier + " to " +
                expandedFields.size() + " fields");
            return expandedFields;
        }

        // 如果没有限定符，但有表别名映射
        if (qualifier == null && !tableAliasMap.isEmpty()) {
            if (tableAliasMap.size() == 1) {
                // 只有一个表，使用它
                tableName = tableAliasMap.values().iterator().next();
            } else {
                // 有多个表但没有限定符，展开所有表的字段
                System.out.println("  Expanding SELECT * from all " + tableAliasMap.size() + " tables");
                for (Map.Entry<String, String> entry : tableAliasMap.entrySet()) {
                    String alias = entry.getKey();
                    String realTable = entry.getValue();

                    // 检查是否是子查询别名
                    if (subqueryFieldCache.containsKey(alias)) {
                        Map<String, FieldLineage> subqueryFields = subqueryFieldCache.get(alias);
                        for (Map.Entry<String, FieldLineage> subEntry : subqueryFields.entrySet()) {
                            String fieldName = subEntry.getKey();
                            FieldLineage subqueryField = subEntry.getValue();

                            FieldLineage fieldLineage = new FieldLineage();
                            fieldLineage.setFieldName(fieldName);
                            fieldLineage.setTableName(targetTable);
                            fieldLineage.setExpression(alias + ".*");
                            fieldLineage.setFieldType(FieldLineage.FieldType.COLUMN);

                            for (FieldLineage dep : subqueryField.getDependencies()) {
                                fieldLineage.addDependency(cloneFieldLineage(dep));
                            }

                            expandedFields.add(fieldLineage);
                        }
                    } else if (hiveConfig.isConnected()) {
                        // 从 Metastore 获取真实表的字段
                        try {
                            Map<String, FieldSchema> tableFields = hiveConfig.getTableFields(realTable);
                            for (Map.Entry<String, FieldSchema> fieldEntry : tableFields.entrySet()) {
                                String fieldName = fieldEntry.getKey();

                                FieldLineage fieldLineage = new FieldLineage();
                                fieldLineage.setFieldName(fieldName);
                                fieldLineage.setTableName(targetTable);
                                fieldLineage.setExpression(realTable + ".*");
                                fieldLineage.setFieldType(FieldLineage.FieldType.COLUMN);

                                FieldLineage dependency = new FieldLineage();
                                dependency.setFieldName(fieldName);
                                dependency.setTableName(realTable);
                                dependency.setSourceTableName(realTable);
                                dependency.setFieldType(FieldLineage.FieldType.COLUMN);
                                fieldLineage.addDependency(dependency);

                                expandedFields.add(fieldLineage);
                            }
                        } catch (Exception e) {
                            System.err.println("Failed to expand SELECT * for table " + realTable + ": " + e.getMessage());
                        }
                    }
                }
                System.out.println("Expanded SELECT * to " + expandedFields.size() + " fields from " +
                    tableAliasMap.size() + " tables");
                return expandedFields;
            }
        }

        // 如果找到了真实表名，从 Metastore 获取所有字段
        if (tableName != null && hiveConfig.isConnected()) {
            try {
                Map<String, FieldSchema> tableFields = hiveConfig.getTableFields(tableName);
                for (Map.Entry<String, FieldSchema> entry : tableFields.entrySet()) {
                    String fieldName = entry.getKey();

                    FieldLineage fieldLineage = new FieldLineage();
                    fieldLineage.setFieldName(fieldName);
                    fieldLineage.setTableName(targetTable);
                    fieldLineage.setExpression(tableName + ".*");
                    fieldLineage.setFieldType(FieldLineage.FieldType.COLUMN);

                    // 添加依赖：targetTable.field -> sourceTable.field
                    FieldLineage dependency = new FieldLineage();
                    dependency.setFieldName(fieldName);
                    dependency.setTableName(tableName);
                    dependency.setSourceTableName(tableName);  // 重要：设置 sourceTableName
                    dependency.setFieldType(FieldLineage.FieldType.COLUMN);
                    fieldLineage.addDependency(dependency);

                    expandedFields.add(fieldLineage);
                }
                System.out.println("Expanded SELECT * to " + expandedFields.size() + " fields from table: " + tableName);
                return expandedFields;
            } catch (Exception e) {
                System.err.println("Failed to expand SELECT * for table " + tableName + ": " + e.getMessage());
            }
        }

        // 如果无法展开，返回一个特殊的字段表示无法解析
        System.err.println("Could not expand SELECT * - qualifier=" + qualifier +
            ", tableName=" + tableName + ", tableAliasMap=" + tableAliasMap);
        FieldLineage fallbackField = new FieldLineage();
        fallbackField.setFieldName("*");
        fallbackField.setTableName(targetTable);
        fallbackField.setExpression("*");
        fallbackField.setFieldType(FieldLineage.FieldType.ERROR);
        expandedFields.add(fallbackField);
        return expandedFields;
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
     * 注意：main 方法不能使用 Spring 注入，仅用于演示
     * 实际使用时应该通过 Spring 容器获取 bean
     */
    public static void main(String[] args) {
        System.out.println("注意：HiveLineageParser 现在是 Spring Bean，需要通过 Spring 容器使用。");
        System.out.println("请在 application.yml 中配置 hive.metastore.uris");
        System.out.println();
        System.out.println("示例配置：");
        System.out.println("hive:");
        System.out.println("  metastore:");
        System.out.println("    uris: thrift://localhost:9083");
        System.out.println("    auto-connect: true");
    }
}
