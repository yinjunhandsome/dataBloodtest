package org.example.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.log4j.Logger;
import org.example.hive.bean.*;
import org.example.hive.process.*;

import java.util.*;

/**
 * HiveSQL血缘解析引擎 - 简化版
 * 输入：SQL字符串
 * 输出：字段级别血缘关系 Map<String, Set<String>> (目标字段 -> 源字段集合)
 */
public class HiveSqlLineageParser {
    private static final Logger logger = Logger.getLogger(HiveSqlLineageParser.class);

    // 元数据提供者
    private TableMetadataProvider metadataProvider;

    // 解析过程中的中间结果
    private List<ParseColumnResult> parseColumnResults = new ArrayList<>();
    // 临时存储聚合字段的索引，用于后续添加 GROUP BY 字段依赖
    private List<Integer> aggregateColumnIndexes = new ArrayList<>();
    private List<ParseTableResult> parseTableResults = new ArrayList<>();
    private List<ParseJoinResult> parseJoinResults = new ArrayList<>();
    private List<ParseSubQueryResult> parseSubQueryResults = new ArrayList<>();
    private List<ParseWithResult> parseWithResults = new ArrayList<>();
    // GROUP BY 字段（用于聚合函数血缘）
    private Set<String> groupByFields = new HashSet<>();

    // FROM tables
    private Set<String> fromTables = new HashSet<>();
    // INSERT/CREATE TABLE name
    private String targetTable;
    // INSERT/CREATE TABLE's column list
    private List<String> targetTableColumns = new ArrayList<>();

    private List<Map<String, ParseColumnResult>> parseQueryResults = new ArrayList<>();
    private Map<String, ParseColumnResult> parseSelectResults = new HashMap<>();
    private Map<String, ParseColumnResult> parseUnionColumnResults = new HashMap<>();
    private Map<String, ParseColumnResult> parseFromResult = new HashMap<>();
    private Map<String, ParseColumnResult> parseLateralViewResult = new HashMap<>();
    private Map<String, ParseColumnResult> parseAllColref = new HashMap<>();

    // 存储已处理的子查询结果，即使 parseSubQueryResults 被清空，这个 Map 也会保留子查询信息
    // Key: 子查询别名, Value: 子查询的字段信息
    private Map<String, Map<String, ParseColumnResult>> processedSubQueries = new HashMap<>();

    // 最终的血缘结果
    private Map<String, Set<String>> lineageData = new HashMap<>();

    /**
     * 默认构造函数，使用默认的元数据提供者（会尝试从MetaCacheUtil获取）
     */
    public HiveSqlLineageParser() {
        this(null);
    }

    /**
     * 带元数据提供者的构造函数
     * @param metadataProvider 表元数据提供者，如果为null则使用默认的MetaCacheUtil
     */
    public HiveSqlLineageParser(TableMetadataProvider metadataProvider) {
        this.metadataProvider = metadataProvider;
    }

    /**
     * 设置元数据提供者
     */
    public void setMetadataProvider(TableMetadataProvider metadataProvider) {
        this.metadataProvider = metadataProvider;
    }

    /**
     * 解析SQL并返回字段级别血缘
     * @param sql SQL语句
     * @return Map<目标字段, 源字段集合> 例如: {"db.table.column": ["source_db.source_table.source_column"]}
     */
    public Map<String, Set<String>> parse(String sql) {
        return parse(sql, null);
    }

    /**
     * 解析SQL并返回字段级别血缘
     * @param sql SQL语句
     * @param targetTableColumns 目标表的字段列表（如果是CREATE TABLE AS SELECT或INSERT，需要提供）
     * @return Map<目标字段, 源字段集合>
     */
    public Map<String, Set<String>> parse(String sql, List<String> targetTableColumns) {
        clear();
        this.targetTableColumns = targetTableColumns != null ? targetTableColumns : new ArrayList<>();

        try {
            ASTNode ast = getASTNode(sql);
            if (ast != null) {
                parseASTNode(ast);
            }
        } catch (Exception e) {
            logger.error("Parse SQL error: " + sql, e);
            System.out.println(e);
        }

        return new HashMap<>(lineageData);
    }

    private void clear() {
        parseColumnResults.clear();
        aggregateColumnIndexes.clear();
        parseTableResults.clear();
        parseJoinResults.clear();
        parseSubQueryResults.clear();
        parseWithResults.clear();
        groupByFields.clear();
        fromTables.clear();
        targetTable = null;
        targetTableColumns.clear();
        parseQueryResults.clear();
        parseSelectResults.clear();
        parseUnionColumnResults.clear();
        parseFromResult.clear();
        parseLateralViewResult.clear();
        parseAllColref.clear();
        processedSubQueries.clear();
        lineageData.clear();
    }

    private ASTNode getASTNode(String sql) throws Exception {
        HiveConf hiveConf = new HiveConf();
        Configuration conf = new Configuration(hiveConf);
        conf.set("_hive.hdfs.session.path", "/tmp");
        conf.set("_hive.local.session.path", "/tmp");
        Context context = new Context(conf);
        ParseDriver pd = new ParseDriver();
        try {
            ASTNode ast = pd.parse(sql, context);
            logger.debug("AST: " + ast.dump());
            return ast;
        } catch (Exception e) {
            logger.error("SQL->AST ERROR. Sql: " + sql, e);
            throw e;
        }
    }

    private Map<String, String> getAliasTableFromTable(List<ParseTableResult> parseTableResults) {
        Map<String, String> tableAliasMap = new HashMap<>();
        for (ParseTableResult parseTableResult : parseTableResults) {
            tableAliasMap.put(parseTableResult.getAliasName(), parseTableResult.getTableFullName());
        }
        return tableAliasMap;
    }

    private Map<String, String> getAliasTableFromJoin(List<ParseJoinResult> parseJoinResults) {
        Map<String, String> aliasTableMap = new HashMap<>();
        for (ParseJoinResult parseJoinResult : parseJoinResults) {
            aliasTableMap.putAll(getAliasTableFromTable(parseJoinResult.getParseTableResults()));
            List<ParseJoinResult> parseJoinResults1 = parseJoinResult.getParseJoinResults();
            aliasTableMap.putAll(getAliasTableFromJoin(parseJoinResults1));
        }
        return aliasTableMap;
    }

    private ParseColumnResult getIndexColumnResult(Map<String, ParseColumnResult> parseColumnMap, int childIndex) {
        for (Map.Entry<String, ParseColumnResult> entry : parseColumnMap.entrySet()) {
            ParseColumnResult parseColumnResult = entry.getValue();
            if (parseColumnResult.getIndex() == childIndex) {
                return parseColumnResult;
            }
        }
        return null;
    }

    /**
     * 处理 TOK_TABREF 节点，使用metadataProvider获取表字段
     */
    private ParseTableResult processTokTabref(ASTNode ast) {
        String dbName = "default";
        String tableName;
        if (ast.getChild(0).getChildCount() == 1) {
            tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(0));
            logger.debug("hive table has no db name: " + tableName);
        } else {
            dbName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(0));
            tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(1));
        }
        String tableFullName = dbName + "." + tableName;

        // 判断是否有别名
        String tableAliasName;
        if (ast.getChild(1) != null) {
            tableAliasName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
        } else {
            // 没有别名用库+表名
            tableAliasName = dbName + "__" + tableName;
        }

        // 生成 ParseTableResult
        ParseTableResult parseTableResult = new ParseTableResult();
        parseTableResult.setAliasName(tableAliasName);
        parseTableResult.setDbName(dbName);
        parseTableResult.setTableName(tableName);
        parseTableResult.setTableFullName(tableFullName);

        // 获取字段列表
        List<String> columnNameList;
        if (metadataProvider != null && metadataProvider.tableExists(tableFullName)) {
            columnNameList = metadataProvider.getTableColumns(tableFullName);
        } else {
                columnNameList = new ArrayList<>();
                logger.warn("No metadata provider found for table: " + tableFullName);
        }
        parseTableResult.setColumnNameList(columnNameList);

        return parseTableResult;
    }

    private void parseChildASTNode(ASTNode ast) {
        if (ast.getToken() != null && (ast.getToken().getType() == HiveParser.TOK_LATERAL_VIEW ||
                ast.getToken().getType() == HiveParser.TOK_LATERAL_VIEW_OUTER)) {
            parseASTNode((ASTNode) ast.getChild(1));
            parseASTNode((ASTNode) ast.getChild(0));
        } else if (ast.getToken() != null && ast.getToken().getType() == HiveParser.TOK_QUERY) {
            // 添加调试日志：打印 TOK_QUERY 的子节点信息
            logger.debug("parseChildASTNode: TOK_QUERY found, childCount=" + ast.getChildCount());
            for (int i = 0; i < ast.getChildCount(); i++) {
                ASTNode child = (ASTNode) ast.getChild(i);
                if (child != null) {
                    String tokenName = "";
                    try {
                        tokenName = HiveParser.tokenNames[child.getType()];
                    } catch (Exception e) {
                        tokenName = "UNKNOWN";
                    }
                    logger.debug("parseChildASTNode: TOK_QUERY child[" + i + "] type=" + child.getType() +
                               " (" + child.getText() + "), tokenName=" + tokenName);
                }
            }

            // Search for TOK_CTE in any child position
            ASTNode cteNode = null;
            for (int i = 0; i < ast.getChildCount(); i++) {
                if (ast.getChild(i) != null && ast.getChild(i).getType() == HiveParser.TOK_CTE) {
                    cteNode = (ASTNode) ast.getChild(i);
                    logger.debug("parseChildASTNode: Found TOK_CTE at child index " + i);
                    break;
                }
            }
            if (cteNode != null) {
                logger.debug("parseChildASTNode: Found TOK_QUERY with TOK_CTE, CTE count=" + cteNode.getChildCount());
                // 记录处理 CTE 前的子查询数量
                int initialSubQueryCount = parseSubQueryResults.size();
                logger.debug("parseChildASTNode: Initial parseSubQueryResults.size()=" + initialSubQueryCount);
                for (int i = 0; i < cteNode.getChildCount(); i++) {
                    logger.debug("parseChildASTNode: Processing CTE #" + i);
                    parseASTNode((ASTNode) cteNode.getChild(i));
                    logger.debug("parseChildASTNode: After parsing CTE #" + i + ", parseSubQueryResults.size()=" + parseSubQueryResults.size());
                    // 只添加新解析的子查询结果（避免重复添加）
                    for (int j = initialSubQueryCount; j < parseSubQueryResults.size(); j++) {
                        ParseWithResult parseWithResult = new ParseWithResult();
                        parseWithResult.setTableName(parseSubQueryResults.get(j).getAliasName());
                        Map<String, ParseColumnResult> parseSubQueryResultTmp = new HashMap<>();
                        parseSubQueryResultTmp.putAll(parseSubQueryResults.get(j).getParseSubQueryResults());
                        parseWithResult.setParseSubQueryResults(parseSubQueryResultTmp);
                        parseWithResults.add(parseWithResult);
                        logger.debug("Added CTE to parseWithResults: " + parseWithResult.getTableName() +
                                   " with " + parseSubQueryResultTmp.size() + " fields");
                    }
                    // 更新起始位置，为下一个 CTE 做准备
                    initialSubQueryCount = parseSubQueryResults.size();
                }
                logger.debug("parseChildASTNode: Finished processing all CTEs, parseWithResults.size()=" + parseWithResults.size());
                parseSubQueryResults.clear();
                logger.debug("parseChildASTNode: Cleared parseSubQueryResults");
                logger.debug("parseChildASTNode: Now parsing main query (non-CTE children)");
                // Parse all non-CTE children
                for (int i = 0; i < ast.getChildCount(); i++) {
                    if (ast.getChild(i) != null && ast.getChild(i).getType() != HiveParser.TOK_CTE) {
                        parseASTNode((ASTNode) ast.getChild(i));
                    }
                }
            } else {
                // No CTE found, parse all children normally
                int childCount = ast.getChildCount();
                for (int i = 0; i < childCount; i++) {
                    parseASTNode((ASTNode) ast.getChild(i));
                }
            }
        } else {
            int childCount = ast.getChildCount();
            for (int i = 0; i < childCount; i++) {
                parseASTNode((ASTNode) ast.getChild(i));
            }
        }
    }

    private Map<String, ParseColumnResult> genFromColumnData(ASTNode ast) {
        switch (ast.getToken().getType()) {
            case HiveParser.TOK_SUBQUERY:
                Map<String, ParseColumnResult> result = ProcessSubQueryData.process(parseSubQueryResults);
                parseSubQueryResults.clear();
                return result;

            case HiveParser.TOK_TABREF:
                // 获取表名或别名
                String tableNameOrAlias = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(0));

                // 获取别名（如果有的话）
                String tableAlias;
                if (ast.getChild(1) != null) {
                    tableAlias = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
                } else {
                    tableAlias = tableNameOrAlias;
                }

                // 检查是否是子查询别名（通过 tableNameOrAlias 或 tableAlias 匹配）
                ParseSubQueryResult matchedSubQuery = null;

                // 首先检查 parseSubQueryResults（当前正在处理的子查询）
                for (ParseSubQueryResult subQueryResult : parseSubQueryResults) {
                    if (subQueryResult.getAliasName().equals(tableNameOrAlias) ||
                        subQueryResult.getAliasName().equals(tableAlias)) {
                        matchedSubQuery = subQueryResult;
                        break;
                    }
                }

                // 如果在 parseSubQueryResults 中没找到，检查 processedSubQueries（已处理的子查询）
                if (matchedSubQuery == null) {
                    Map<String, ParseColumnResult> processedResult = processedSubQueries.get(tableNameOrAlias);
                    if (processedResult != null) {
                        matchedSubQuery = new ParseSubQueryResult();
                        matchedSubQuery.setAliasName(tableNameOrAlias);
                        matchedSubQuery.setParseSubQueryResults(new HashMap<>(processedResult));
                        logger.debug("TOK_TABREF: Found in processedSubQueries for " + tableNameOrAlias + " with " + processedResult.size() + " fields");
                    }
                }

                // 如果还是没找到，尝试检查 parseQueryResults（向后兼容）
                if (matchedSubQuery == null && !parseQueryResults.isEmpty()) {
                    logger.debug("TOK_TABREF: Not found in parseSubQueryResults or processedSubQueries, checking parseQueryResults for " + tableNameOrAlias);
                    for (int i = parseQueryResults.size() - 1; i >= 0; i--) {
                        Map<String, ParseColumnResult> queryResult = parseQueryResults.get(i);
                        if (!queryResult.isEmpty()) {
                            matchedSubQuery = new ParseSubQueryResult();
                            matchedSubQuery.setAliasName(tableNameOrAlias);
                            matchedSubQuery.setParseSubQueryResults(new HashMap<>(queryResult));
                            logger.debug("TOK_TABREF: Found in parseQueryResults[" + i + "], created temporary subquery with " + queryResult.size() + " fields");
                            break;
                        }
                    }
                }

                // 如果找到匹配的子查询，返回其字段信息
                if (matchedSubQuery != null) {
                    Map<String, ParseColumnResult> subQueryResultMap = new HashMap<>();
                    Map<String, ParseColumnResult> parseColumnResultMap = matchedSubQuery.getParseSubQueryResults();

                    for (Map.Entry<String, ParseColumnResult> entry : parseColumnResultMap.entrySet()) {
                        String columnAliasName = entry.getKey();
                        ParseColumnResult original = entry.getValue();

                        // 创建 ParseColumnResult 的副本
                        ParseColumnResult copy = new ParseColumnResult();
                        copy.setAliasName(original.getAliasName());
                        copy.setIndex(original.getIndex());
                        copy.setFromTableColumnSet(new HashSet<>(original.getFromTableColumnSet()));
                        copy.setAggregate(original.isAggregate());

                        // 使用 TOK_TABREF 的别名作为前缀
                        subQueryResultMap.put(tableAlias + "." + columnAliasName, copy);
                    }
                    return subQueryResultMap;
                }

                if (ast.getChild(0).getChildCount() == 1) {
                    // 无数据库名的情况，检查是否是 CTE
                    boolean found = false;

                    for (ParseWithResult parseWithResult : parseWithResults) {
                        if (parseWithResult.getTableName().equals(tableNameOrAlias)) {
                            // 只处理被引用的 CTE，并创建副本避免污染
                            Map<String, ParseColumnResult> cteResult = new HashMap<>();
                            String subQueryAliasName = parseWithResult.getAliasName();
                            if (subQueryAliasName == null) {
                                subQueryAliasName = parseWithResult.getTableName();
                            }
                            Map<String, ParseColumnResult> parseColumnResultMap = parseWithResult.getParseSubQueryResults();

                            for(Map.Entry<String, ParseColumnResult> entry : parseColumnResultMap.entrySet()){
                                String columnAliasName = entry.getKey();
                                ParseColumnResult original = entry.getValue();

                                // 创建 ParseColumnResult 的副本
                                ParseColumnResult copy = new ParseColumnResult();
                                copy.setAliasName(original.getAliasName());
                                copy.setIndex(original.getIndex());
                                copy.setFromTableColumnSet(new HashSet<>(original.getFromTableColumnSet()));
                                copy.setAggregate(original.isAggregate());

                                cteResult.put(subQueryAliasName + "." + columnAliasName, copy);
                            }
                            found = true;
                            return cteResult;
                        }
                    }

                    // 如果既不是 CTE 也不是子查询别名，按普通表处理
                    if (!found) {
                        Map<String, ParseColumnResult> tabrefResult = ProcessTabrefData.process(parseTableResults);
                        parseTableResults.clear();
                        return tabrefResult;
                    }
                } else {
                    Map<String, ParseColumnResult> tabrefResult = ProcessTabrefData.process(parseTableResults);
                    parseTableResults.clear();
                    return tabrefResult;
                }
                break;

            case HiveParser.TOK_RIGHTOUTERJOIN:
            case HiveParser.TOK_LEFTOUTERJOIN:
            case HiveParser.TOK_JOIN:
            case HiveParser.TOK_LEFTSEMIJOIN:
//            case HiveParser.TOK_MAPJOIN:
            case HiveParser.TOK_FULLOUTERJOIN:
            case HiveParser.TOK_UNIQUEJOIN:
                Map<String, ParseColumnResult> joinResult = ProcessJoinData.process(parseJoinResults);
                parseJoinResults.clear();
                return joinResult;

            case HiveParser.TOK_LATERAL_VIEW:
            case HiveParser.TOK_LATERAL_VIEW_OUTER:
                Map<String, ParseColumnResult> lateralViewResultTmp = new HashMap<>(parseLateralViewResult);
                parseLateralViewResult.clear();
                return lateralViewResultTmp;

            default:
                break;
        }
        return new HashMap<>();
    }

    private void parseCurrentASTNode(ASTNode ast) {
        if (ast.getToken() == null) {
            return;
        }

        switch (ast.getToken().getType()) {
            case HiveParser.TOK_CREATETABLE:
                targetTable = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                logger.debug("Target table: " + targetTable);
                if (parseSelectResults.size() > 0) {
                    buildLineageData();
                }
                break;

            case HiveParser.TOK_TAB:
                targetTable = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                logger.debug("Target table: " + targetTable);
                break;

            case HiveParser.TOK_UNIONALL:
                logger.debug("TOK_UNIONALL: found UNION node");
                // 添加调试信息：打印 AST 结构
                logger.debug("TOK_UNIONALL: AST childCount=" + ast.getChildCount());
                for (int i = 0; i < ast.getChildCount(); i++) {
                    ASTNode child = (ASTNode) ast.getChild(i);
                    if (child != null) {
                        String tokenName = "";
                        try {
                            tokenName = HiveParser.tokenNames[child.getType()];
                        } catch (Exception e) {
                            tokenName = "UNKNOWN";
                        }
                        logger.debug("TOK_UNIONALL: child[" + i + "] type=" + child.getType() +
                                   " (" + child.getText() + "), tokenName=" + tokenName);
                    }
                }

                // 添加调试信息：打印 parseQueryResults 的大小
                logger.debug("TOK_UNIONALL: parseQueryResults.size()=" + parseQueryResults.size());
                for (int i = 0; i < parseQueryResults.size(); i++) {
                    logger.debug("TOK_UNIONALL: parseQueryResults[" + i + "].size()=" + parseQueryResults.get(i).size() +
                               ", keys=" + parseQueryResults.get(i).keySet());
                }
                logger.debug("TOK_UNIONALL: parseUnionColumnResults.size()=" + parseUnionColumnResults.size());

                Map<String, ParseColumnResult> newParseColumnResultMap = new HashMap<>();
                Map<String, ParseColumnResult> leftParseColumnResultMap;
                Map<String, ParseColumnResult> RightParseColumnResultMap;

                if (ast.getChild(0).getType() == HiveParser.TOK_UNIONALL) {
                    leftParseColumnResultMap = parseUnionColumnResults;
                    RightParseColumnResultMap = parseQueryResults.get(0);
                    logger.debug("TOK_UNIONALL: left is previous UNION (" + leftParseColumnResultMap.size() + " fields), right is new query (" + RightParseColumnResultMap.size() + " fields)");
                } else {
                    leftParseColumnResultMap = parseQueryResults.get(0);
                    RightParseColumnResultMap = parseQueryResults.get(1);
                    logger.debug("TOK_UNIONALL: left is query 0 (" + leftParseColumnResultMap.size() + " fields), right is query 1 (" + RightParseColumnResultMap.size() + " fields)");
                }

                logger.debug("TOK_UNIONALL: left fields = " + leftParseColumnResultMap.keySet());
                logger.debug("TOK_UNIONALL: right fields = " + RightParseColumnResultMap.keySet());

                // 按照字段名（aliasName）来匹配，而不是 index
                for (Map.Entry<String, ParseColumnResult> entry : leftParseColumnResultMap.entrySet()) {
                    String columnAliasName = entry.getKey();
                    ParseColumnResult leftColumn = entry.getValue();

                    logger.debug("TOK_UNIONALL: processing field " + columnAliasName +
                               ", left lineage=" + leftColumn.getFromTableColumnSet() +
                               ", left isAggregate=" + leftColumn.isAggregate());

                    // 从右边查找同名字段
                    ParseColumnResult rightColumn = RightParseColumnResultMap.get(columnAliasName);
                    if (rightColumn != null) {
                        Set<String> rightFromColumnSet = rightColumn.getFromTableColumnSet();
                        boolean rightIsAggregate = rightColumn.isAggregate();
                        logger.debug("TOK_UNIONALL: found right field " + columnAliasName +
                                   " with lineage=" + rightFromColumnSet +
                                   ", right isAggregate=" + rightIsAggregate);

                        // 合并左右两边的血缘
                        Set<String> mergedFromColumnSet = new HashSet<>(leftColumn.getFromTableColumnSet());
                        mergedFromColumnSet.addAll(rightFromColumnSet);

                        // 如果任一边是聚合字段，合并后的字段也应该是聚合字段
                        boolean mergedIsAggregate = leftColumn.isAggregate() || rightIsAggregate;

                        // 创建新的 ParseColumnResult 对象，避免修改原始对象
                        ParseColumnResult mergedColumn = new ParseColumnResult();
                        mergedColumn.setAliasName(columnAliasName);
                        mergedColumn.setIndex(leftColumn.getIndex());
                        mergedColumn.setFromTableColumnSet(mergedFromColumnSet);
                        mergedColumn.setAggregate(mergedIsAggregate);

                        logger.debug("TOK_UNIONALL: merged lineage for " + columnAliasName +
                                   " = " + mergedFromColumnSet +
                                   ", merged isAggregate=" + mergedIsAggregate);
                        newParseColumnResultMap.put(columnAliasName, mergedColumn);
                    } else {
                        logger.warn("TOK_UNIONALL: right field " + columnAliasName + " not found in right fields: " + RightParseColumnResultMap.keySet());
                        // 右边没有该字段，直接使用左边的（创建副本）
                        ParseColumnResult copyColumn = new ParseColumnResult();
                        copyColumn.setAliasName(columnAliasName);
                        copyColumn.setIndex(leftColumn.getIndex());
                        copyColumn.setFromTableColumnSet(new HashSet<>(leftColumn.getFromTableColumnSet()));
                        copyColumn.setAggregate(leftColumn.isAggregate());
                        newParseColumnResultMap.put(columnAliasName, copyColumn);
                    }
                }

                // 处理右边有但左边没有的字段
                for (Map.Entry<String, ParseColumnResult> entry : RightParseColumnResultMap.entrySet()) {
                    String columnAliasName = entry.getKey();
                    if (!newParseColumnResultMap.containsKey(columnAliasName)) {
                        logger.debug("TOK_UNIONALL: processing right-only field " + columnAliasName +
                                   ", right lineage=" + entry.getValue().getFromTableColumnSet());
                        // 创建副本
                        ParseColumnResult copyColumn = new ParseColumnResult();
                        copyColumn.setAliasName(columnAliasName);
                        copyColumn.setIndex(entry.getValue().getIndex());
                        copyColumn.setFromTableColumnSet(new HashSet<>(entry.getValue().getFromTableColumnSet()));
                        copyColumn.setAggregate(entry.getValue().isAggregate());
                        newParseColumnResultMap.put(columnAliasName, copyColumn);
                    }
                }

                logger.debug("TOK_UNIONALL: final merged fields = " + newParseColumnResultMap.keySet());
                for (Map.Entry<String, ParseColumnResult> entry : newParseColumnResultMap.entrySet()) {
                    logger.debug("TOK_UNIONALL:   " + entry.getKey() + " -> " + entry.getValue().getFromTableColumnSet() +
                               " (isAggregate=" + entry.getValue().isAggregate() + ")");
                }

                parseUnionColumnResults.putAll(newParseColumnResultMap);
                parseQueryResults.clear();
                break;

            case HiveParser.TOK_QUERY:
                // 添加调试日志
                logger.debug("TOK_QUERY: processing, parseSelectResults.size()=" + parseSelectResults.size() +
                           ", keys=" + parseSelectResults.keySet());
                Map<String, ParseColumnResult> queryColumnMapTmp = new HashMap<>(parseSelectResults);
                parseQueryResults.add(queryColumnMapTmp);
                parseSelectResults.clear();
                logger.debug("TOK_QUERY: after adding to parseQueryResults, parseQueryResults.size()=" + parseQueryResults.size());
                break;

            case HiveParser.TOK_INSERT:
                // 添加调试日志
                logger.debug("TOK_INSERT: processing, parseColumnResults.size()=" + parseColumnResults.size() +
                           ", parseAllColref.size()=" + parseAllColref.size() +
                           ", parseSelectResults.size()=" + parseSelectResults.size());
                if (targetTableColumns.size() > 0) {
                    buildLineageData();
                } else {
                    Map<String, ParseColumnResult> selectResultsTmp = new HashMap<>();
                    if (parseColumnResults.size() != 0) {
                        for (ParseColumnResult parseColumnResult : parseColumnResults) {
                            selectResultsTmp.put(parseColumnResult.getAliasName(), parseColumnResult);
                        }
                        parseColumnResults.clear();
                        parseSelectResults.putAll(selectResultsTmp);
                    }
                    if (parseAllColref.size() != 0) {
                        for (Map.Entry<String, ParseColumnResult> entry : parseAllColref.entrySet()) {
                            selectResultsTmp.put(entry.getKey(), entry.getValue());
                        }
                        parseAllColref.clear();
                        parseSelectResults.putAll(selectResultsTmp);
                    }
                    logger.debug("TOK_INSERT: " + selectResultsTmp);
                }
                break;

            case HiveParser.TOK_LATERAL_VIEW:
            case HiveParser.TOK_LATERAL_VIEW_OUTER:
                parseLateralViewResult = genFromColumnData((ASTNode) ast.getChild(1));
                ProcessTokSelexpr laterViewTokSelexpr = new ProcessTokSelexpr();
                laterViewTokSelexpr.setParseFromResult(parseLateralViewResult);
                ParseColumnResult laterViewParseColumnResult = laterViewTokSelexpr.process((ASTNode) ast.getChild(0).getChild(0));

                String laterViewColumnPrefix;
                if (ast.getChild(0).getChild(0).getChild(2) != null &&
                        ast.getChild(0).getChild(0).getChild(2).getType() == HiveParser.TOK_TABALIAS) {
                    String latervalViewAliasName = ast.getChild(0).getChild(0).getChild(2).getChild(0).getText();
                    laterViewColumnPrefix = latervalViewAliasName + ".";
                } else {
                    laterViewColumnPrefix = ".";
                }
                parseLateralViewResult.put(laterViewColumnPrefix + laterViewParseColumnResult.getAliasName(), laterViewParseColumnResult);
                parseColumnResults.clear();
                break;

            case HiveParser.TOK_FROM:
                parseFromResult = genFromColumnData((ASTNode) ast.getChild(0));
                logger.debug("TOK_FROM: " + parseFromResult);
                break;

            case HiveParser.TOK_RIGHTOUTERJOIN:
            case HiveParser.TOK_LEFTOUTERJOIN:
            case HiveParser.TOK_JOIN:
            case HiveParser.TOK_LEFTSEMIJOIN:
//            case HiveParser.TOK_MAPJOIN:
            case HiveParser.TOK_FULLOUTERJOIN:
            case HiveParser.TOK_UNIQUEJOIN:
                ParseJoinResult parseJoinResult = new ParseJoinResult();

                logger.debug("TOK_JOIN: Starting JOIN processing, parseTableResults.size=" + parseTableResults.size() +
                           ", parseSubQueryResults.size()=" + parseSubQueryResults.size());

                List<ParseTableResult> tableResults = new ArrayList<>(parseTableResults);
                parseTableResults.clear();

                // 处理表结果：如果表名匹配子查询别名，则创建映射的子查询结果
                List<ParseSubQueryResult> mappedSubQueryResults = new ArrayList<>();

                for (ParseTableResult tableResult : tableResults) {
                    boolean isMapped = false;
                    String tableName = tableResult.getTableName();
                    String tableAlias = tableResult.getAliasName();
                    String subQueryRef = tableResult.getSubQueryRef();

                    logger.debug("TOK_JOIN: Processing tableResult - tableName=" + tableName + ", tableAlias=" + tableAlias + ", subQueryRef=" + subQueryRef);

                    // 首先检查是否有子查询引用（subQueryRef 字段）
                    if (subQueryRef != null && !subQueryRef.isEmpty()) {
                        // 首先检查是否是 CTE 引用
                        for (ParseWithResult parseWithResult : parseWithResults) {
                            if (parseWithResult.getTableName().equals(subQueryRef)) {
                                // 找到匹配的 CTE，创建一个新的子查询结果，使用 TOK_TABREF 的别名
                                ParseSubQueryResult mappedSubQuery = new ParseSubQueryResult();
                                mappedSubQuery.setAliasName(tableAlias);  // 使用 TOK_TABREF 的别名

                                // 复制字段信息
                                Map<String, ParseColumnResult> newParseSubQueryResults = new HashMap<>();
                                for (Map.Entry<String, ParseColumnResult> entry : parseWithResult.getParseSubQueryResults().entrySet()) {
                                    ParseColumnResult original = entry.getValue();
                                    ParseColumnResult copy = new ParseColumnResult();
                                    copy.setAliasName(original.getAliasName());
                                    copy.setIndex(original.getIndex());
                                    copy.setFromTableColumnSet(new HashSet<>(original.getFromTableColumnSet()));
                                    copy.setAggregate(original.isAggregate());
                                    newParseSubQueryResults.put(entry.getKey(), copy);
                                }
                                mappedSubQuery.setParseSubQueryResults(newParseSubQueryResults);

                                mappedSubQueryResults.add(mappedSubQuery);
                                isMapped = true;

                                logger.debug("TOK_JOIN: Mapped CTE via subQueryRef " + subQueryRef +
                                           " to TOK_TABREF alias " + tableAlias +
                                           ", created " + newParseSubQueryResults.size() + " field mappings");
                                break;
                            }
                        }

                        // 如果不是 CTE，检查是否是普通子查询引用
                        if (!isMapped) {
                            for (ParseSubQueryResult subQueryResult : parseSubQueryResults) {
                                if (subQueryResult.getAliasName().equals(subQueryRef)) {
                                    // 创建一个新的子查询结果，使用 TOK_TABREF 的别名
                                    ParseSubQueryResult mappedSubQuery = new ParseSubQueryResult();
                                    mappedSubQuery.setAliasName(tableAlias);  // 使用 TOK_TABREF 的别名

                                    // 复制字段信息
                                    Map<String, ParseColumnResult> newParseSubQueryResults = new HashMap<>();
                                    for (Map.Entry<String, ParseColumnResult> entry : subQueryResult.getParseSubQueryResults().entrySet()) {
                                        ParseColumnResult original = entry.getValue();
                                        ParseColumnResult copy = new ParseColumnResult();
                                        copy.setAliasName(original.getAliasName());
                                        copy.setIndex(original.getIndex());
                                        copy.setFromTableColumnSet(new HashSet<>(original.getFromTableColumnSet()));
                                        copy.setAggregate(original.isAggregate());
                                        newParseSubQueryResults.put(entry.getKey(), copy);
                                    }
                                    mappedSubQuery.setParseSubQueryResults(newParseSubQueryResults);

                                    mappedSubQueryResults.add(mappedSubQuery);
                                    isMapped = true;

                                    logger.debug("TOK_JOIN: Mapped subquery via subQueryRef " + subQueryRef +
                                               " to TOK_TABREF alias " + tableAlias +
                                               ", created " + newParseSubQueryResults.size() + " field mappings");
                                    break;
                                }
                            }
                        }
                    }

                    // 如果没有通过 subQueryRef 映射，检查是否表名/别名匹配子查询别名
                    if (!isMapped) {
                        for (ParseSubQueryResult subQueryResult : parseSubQueryResults) {
                            logger.debug("TOK_JOIN: Checking against subQueryResult - aliasName=" + subQueryResult.getAliasName() +
                                       ", equals(tableName)? " + subQueryResult.getAliasName().equals(tableName) +
                                       ", equals(tableAlias)? " + subQueryResult.getAliasName().equals(tableAlias));

                            if (subQueryResult.getAliasName().equals(tableName) ||
                                subQueryResult.getAliasName().equals(tableAlias)) {
                                // 找到匹配的子查询，创建一个新的子查询结果，使用 TOK_TABREF 的别名
                                ParseSubQueryResult mappedSubQuery = new ParseSubQueryResult();
                                mappedSubQuery.setAliasName(tableAlias);  // 使用 TOK_TABREF 的别名

                                // 复制字段信息
                                Map<String, ParseColumnResult> newParseSubQueryResults = new HashMap<>();
                                for (Map.Entry<String, ParseColumnResult> entry : subQueryResult.getParseSubQueryResults().entrySet()) {
                                    ParseColumnResult original = entry.getValue();
                                    ParseColumnResult copy = new ParseColumnResult();
                                    copy.setAliasName(original.getAliasName());
                                    copy.setIndex(original.getIndex());
                                    copy.setFromTableColumnSet(new HashSet<>(original.getFromTableColumnSet()));
                                    copy.setAggregate(original.isAggregate());
                                    newParseSubQueryResults.put(entry.getKey(), copy);
                                }
                                mappedSubQuery.setParseSubQueryResults(newParseSubQueryResults);

                                mappedSubQueryResults.add(mappedSubQuery);
                                isMapped = true;

                                logger.debug("TOK_JOIN: Mapped subquery alias " + subQueryResult.getAliasName() +
                                           " to TOK_TABREF alias " + tableAlias +
                                           ", created " + newParseSubQueryResults.size() + " field mappings");
                                break;
                            }
                        }
                    }

                    // 如果不是子查询别名，添加到表结果列表
                    if (!isMapped) {
                        parseJoinResult.getParseTableResults().add(tableResult);
                        logger.debug("TOK_JOIN: Added non-mapped table to parseTableResults - tableName=" + tableName);
                    }
                }

                logger.debug("TOK_JOIN: After processing tableResults, mappedSubQueryResults.size()=" + mappedSubQueryResults.size() +
                           ", parseJoinResult.getParseTableResults().size()=" + parseJoinResult.getParseTableResults().size());

                // 添加未映射的子查询结果
                for (ParseSubQueryResult subQueryResult : parseSubQueryResults) {
                    boolean alreadyMapped = false;
                    for (ParseSubQueryResult mapped : mappedSubQueryResults) {
                        if (mapped.getAliasName().equals(subQueryResult.getAliasName())) {
                            alreadyMapped = true;
                            break;
                        }
                    }
                    if (!alreadyMapped) {
                        mappedSubQueryResults.add(subQueryResult);
                        logger.debug("TOK_JOIN: Added unmapped subquery - aliasName=" + subQueryResult.getAliasName());
                    }
                }

                logger.debug("TOK_JOIN: Final mappedSubQueryResults.size()=" + mappedSubQueryResults.size());
                for (ParseSubQueryResult psr : mappedSubQueryResults) {
                    logger.debug("TOK_JOIN:   subquery alias=" + psr.getAliasName() +
                               ", fieldCount=" + psr.getParseSubQueryResults().size());
                }

                parseJoinResult.setParseSubQueryResults(mappedSubQueryResults);

                List<ParseJoinResult> joinResults = new ArrayList<>(parseJoinResults);
                parseJoinResults.clear();
                parseJoinResult.setParseJoinResults(joinResults);

                List<ParseWithResult> withResults = new ArrayList<>(parseWithResults);
                parseWithResults.clear();
                parseJoinResult.setParseWithResults(withResults);

                logger.debug("TOK_JOIN: " + parseJoinResult);
                parseJoinResults.add(parseJoinResult);
                break;

            case HiveParser.TOK_TABREF:
                // Null check for AST node structure
                if (ast.getChild(0) == null) {
                    logger.warn("TOK_TABREF: ast.getChild(0) is null, skipping");
                    break;
                }

                String fromTableName = null;
                try {
                    fromTableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                } catch (Exception e) {
                    logger.error("TOK_TABREF: Error getting fromTableName from ast.getChild(0)", e);
                    break;
                }

                // Null check for fromTableName
                if (fromTableName == null || fromTableName.trim().isEmpty()) {
                    logger.warn("TOK_TABREF: fromTableName is null or empty, skipping");
                    break;
                }

                Boolean isWithTable = Boolean.FALSE;
                Boolean isSubQueryAlias = Boolean.FALSE;
                String referencedSubQueryAlias = null;

                // 获取表名（第一个子节点的第一个子节点）
                // Null check for nested child node
                if (ast.getChild(0).getChild(0) == null) {
                    logger.warn("TOK_TABREF: ast.getChild(0).getChild(0) is null for table=" + fromTableName + ", skipping");
                    break;
                }

                String tableNameOrAlias = null;
                try {
                    tableNameOrAlias = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(0));
                } catch (Exception e) {
                    logger.error("TOK_TABREF: Error getting tableNameOrAlias from ast.getChild(0).getChild(0) for table=" + fromTableName, e);
                    break;
                }

                // Null check for tableNameOrAlias
                if (tableNameOrAlias == null || tableNameOrAlias.trim().isEmpty()) {
                    logger.warn("TOK_TABREF: tableNameOrAlias is null or empty for table=" + fromTableName + ", skipping");
                    break;
                }

                // 获取别名（用于后续检查）
                String tabrefAlias = null;
                if (ast.getChild(1) != null) {
                    try {
                        tabrefAlias = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
                    } catch (Exception e) {
                        logger.error("TOK_TABREF: Error getting tabrefAlias from ast.getChild(1)", e);
                    }
                } else if (ast.getChild(0).getChildCount() == 1) {
                    // 如果没有明确的别名，且只有表名（无数据库名），则使用表名作为别名
                    tabrefAlias = tableNameOrAlias;
                }

                // 首先检查是否是 CTE
                if (parseWithResults != null && !parseWithResults.isEmpty()) {
                    logger.debug("TOK_TABREF: Checking if " + tableNameOrAlias + " is a CTE, parseWithResults.size()=" + parseWithResults.size());
                    for (ParseWithResult parseWithResult : parseWithResults) {
                        // Null check for parseWithResult and its tableName
                        if (parseWithResult == null || parseWithResult.getTableName() == null) {
                            continue;
                        }

                        String cteTableName = parseWithResult.getTableName();
                        logger.debug("TOK_TABREF: Comparing " + tableNameOrAlias + " with CTE " + cteTableName);
                        if (cteTableName.equals(fromTableName) || cteTableName.equals(tableNameOrAlias)) {
                            isWithTable = Boolean.TRUE;
                            // 获取或生成别名
                            String finalAlias = tabrefAlias != null ? tabrefAlias : tableNameOrAlias;

                            if (ast.getChild(1) != null) {
                                String withTableAlias = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
                                parseWithResult.setAliasName(withTableAlias);
                                finalAlias = withTableAlias;
                            }

                            // 创建 ParseTableResult 用于 CTE 引用
                            ParseTableResult cteTableResult = new ParseTableResult();
                            cteTableResult.setAliasName(finalAlias);
                            cteTableResult.setDbName("cte");
                            cteTableResult.setTableName("cte");
                            cteTableResult.setTableFullName("cte." + finalAlias);
                            cteTableResult.setColumnNameList(new ArrayList<>());
                            // 存储原始 CTE 别名引用
                            cteTableResult.setSubQueryRef(cteTableName);
                            logger.debug("TOK_TABREF: Created ParseTableResult for CTE with alias=" + finalAlias + ", cteRef=" + cteTableName);
                            parseTableResults.add(cteTableResult);
                            break;
                        }
                    }
                }

                // 如果不是 CTE，检查 processedSubQueries（已处理的子查询）
                if (!isWithTable && !processedSubQueries.isEmpty()) {
                    logger.debug("TOK_TABREF: Not a CTE, checking processedSubQueries for " + tableNameOrAlias);
                    if (processedSubQueries.containsKey(tableNameOrAlias)) {
                        isWithTable = Boolean.TRUE;
                        isSubQueryAlias = Boolean.TRUE;
                        referencedSubQueryAlias = tableNameOrAlias;

                        // 创建 ParseTableResult 用于子查询引用
                        ParseTableResult subQueryTableResult = new ParseTableResult();
                        String finalAlias = tabrefAlias != null ? tabrefAlias : tableNameOrAlias;
                        subQueryTableResult.setAliasName(finalAlias);
                        subQueryTableResult.setDbName("subquery");
                        subQueryTableResult.setTableName("subquery");
                        subQueryTableResult.setTableFullName("subquery." + finalAlias);
                        subQueryTableResult.setColumnNameList(new ArrayList<>());
                        subQueryTableResult.setSubQueryRef(tableNameOrAlias);
                        logger.debug("TOK_TABREF: Found in processedSubQueries, created ParseTableResult for subquery with alias=" + finalAlias + ", subQueryRef=" + tableNameOrAlias);
                        parseTableResults.add(subQueryTableResult);
                    }
                }

                // 如果不是 CTE，检查是否是子查询别名
                if (!isWithTable && parseSubQueryResults != null && !parseSubQueryResults.isEmpty()) {
                    for (ParseSubQueryResult parseSubQueryResult : parseSubQueryResults) {
                        // Null check for parseSubQueryResult
                        if (parseSubQueryResult == null || parseSubQueryResult.getAliasName() == null) {
                            continue;
                        }

                        String subQueryAliasName = parseSubQueryResult.getAliasName();

                        // 检查子查询别名是否匹配 tabrefAlias 或 tableNameOrAlias
                        // 注意：如果是 TOK_TABREF "b mf"，则 tabrefAlias="b", tableNameOrAlias="mf"
                        // 如果 tabrefAlias 匹配一个已有的子查询别名，说明是引用
                        // 如果 tableNameOrAlias 匹配一个已有的子查询别名，且 tabrefAlias != tableNameOrAlias，说明是别名重映射
                        boolean matchesTabrefAlias = tabrefAlias != null && subQueryAliasName.equals(tabrefAlias);
                        boolean matchesTableNameOrAlias = tableNameOrAlias != null && subQueryAliasName.equals(tableNameOrAlias);
                        boolean isAliasRemapping = matchesTableNameOrAlias && tabrefAlias != null && !tabrefAlias.equals(tableNameOrAlias);

                        if (matchesTabrefAlias || isAliasRemapping) {
                            isSubQueryAlias = Boolean.TRUE;
                            referencedSubQueryAlias = subQueryAliasName;
                            logger.debug("TOK_TABREF: Found subquery alias reference: original=" + subQueryAliasName +
                                       ", tabrefAlias=" + tabrefAlias + ", tableNameOrAlias=" + tableNameOrAlias);

                            // 创建一个 ParseTableResult，使用 tabrefAlias 作为别名，并存储子查询引用
                            ParseTableResult parseTableResult = new ParseTableResult();
                            parseTableResult.setAliasName(tabrefAlias != null ? tabrefAlias : tableNameOrAlias);
                            parseTableResult.setDbName("subquery");
                            parseTableResult.setTableName("subquery");
                            parseTableResult.setTableFullName("subquery." + (tabrefAlias != null ? tabrefAlias : tableNameOrAlias));
                            parseTableResult.setColumnNameList(new ArrayList<>());
                            // 存储原始子查询别名引用
                            parseTableResult.setSubQueryRef(referencedSubQueryAlias);
                            logger.debug("TOK_TABREF: Created ParseTableResult with alias=" + tabrefAlias + ", subQueryRef=" + referencedSubQueryAlias);
                            parseTableResults.add(parseTableResult);
                            break;
                        }
                    }
                }

                // 只有既不是 CTE 也不是子查询别名的情况下，才当作普通表处理
                if (!isWithTable && !isSubQueryAlias) {
                    fromTables.add(fromTableName);
                    ParseTableResult parseTableResult = processTokTabref(ast);
                    logger.debug("TOK_TABREF: " + parseTableResult);
                    parseTableResults.add(parseTableResult);
                }
                break;

            case HiveParser.TOK_SELEXPR:
                int childIndex = ast.getChildIndex();

                // 添加调试日志：打印 TOK_SELEXPR 的子节点信息
                if (ast.getChild(0) != null) {
                    String childTokenName = "";
                    try {
                        childTokenName = HiveParser.tokenNames[ast.getChild(0).getType()];
                    } catch (Exception e) {
                        childTokenName = "UNKNOWN";
                    }
                    logger.debug("TOK_SELEXPR: child[0] type=" + ast.getChild(0).getType() + " (" + childTokenName + "), text='" + ast.getChild(0).getText() + "'");
                }

                if (ast.getChild(0).getType() == HiveParser.TOK_ALLCOLREF ||
                    ast.getChild(0).getType() == HiveParser.TOK_SETCOLREF) {
                    logger.debug("TOK_SELEXPR: Found " + (ast.getChild(0).getType() == HiveParser.TOK_ALLCOLREF ? "TOK_ALLCOLREF" : "TOK_SETCOLREF") +
                               ", expanding " + parseFromResult.size() + " fields");
                    for (Map.Entry<String, ParseColumnResult> entry : parseFromResult.entrySet()) {
                        ParseColumnResult parseColumnResult = entry.getValue();
                        // Create a copy to avoid modifying the original
                        ParseColumnResult copy = new ParseColumnResult();
                        copy.setAliasName(parseColumnResult.getAliasName());
                        copy.setIndex(parseColumnResult.getIndex() + childIndex);
                        copy.setFromTableColumnSet(new HashSet<>(parseColumnResult.getFromTableColumnSet()));
                        copy.setAggregate(parseColumnResult.isAggregate());
                        parseAllColref.put(copy.getAliasName(), copy);
                        logger.debug("TOK_SELEXPR: Expanded field " + copy.getAliasName() + " at index " + copy.getIndex());
                    }
                } else {
                    ProcessTokSelexpr processTokSelexpr = new ProcessTokSelexpr();
                    processTokSelexpr.setParseFromResult(parseFromResult);
                    processTokSelexpr.setGroupByFields(groupByFields);
                    ParseColumnResult parseColumnResult = processTokSelexpr.process(ast);
                    logger.debug("TOK_SELEXPR: " + parseColumnResult);
                    // 如果是聚合函数，记录索引以便后续添加 GROUP BY 字段依赖
                    if (parseColumnResult.isAggregate()) {
                        aggregateColumnIndexes.add(parseColumnResults.size());
                        logger.debug("TOK_SELEXPR: Found aggregate field at index " + parseColumnResults.size());
                    }
                    parseColumnResults.add(parseColumnResult);
                }
                break;

            case HiveParser.TOK_GROUPBY:
                // 处理 GROUP BY 子句，收集 Group By 字段
                groupByFields.clear();
                logger.debug("TOK_GROUPBY: child count = " + ast.getChildCount());
                for (int i = 0; i < ast.getChildCount(); i++) {
                    ASTNode groupByExpr = (ASTNode) ast.getChild(i);
                    logger.debug("TOK_GROUPBY: child " + i + " type = " + groupByExpr.getType() +
                               " (" + groupByExpr.getText() + ")");

                    Set<String> groupByColumnSet = new HashSet<>();

                    // 检查是否是数字常量（GROUP BY 位置引用，如 GROUP BY 1, 2, 3）
                    if (groupByExpr.getType() == HiveParser.Number) {
                        // 获取数字（1-based）
                        String positionStr = groupByExpr.getText();
                        try {
                            int position = Integer.parseInt(positionStr);
                            // 从 parseColumnResults 中获取对应位置的字段血缘
                            if (position > 0 && position <= parseColumnResults.size()) {
                                ParseColumnResult referencedColumn = parseColumnResults.get(position - 1);
                                groupByColumnSet = referencedColumn.getFromTableColumnSet();
                                logger.debug("TOK_GROUPBY: child " + i + " is position reference to " +
                                           referencedColumn.getAliasName() + ", extracted columns: " + groupByColumnSet);
                            } else {
                                logger.warn("TOK_GROUPBY: invalid position " + position +
                                          ", valid range is 1-" + parseColumnResults.size());
                            }
                        } catch (NumberFormatException e) {
                            logger.warn("TOK_GROUPBY: failed to parse position: " + positionStr);
                        }
                    } else {
                        // 常规的字段引用，使用 parseSelect 提取
                        ProcessTokSelexpr groupByProcessor = new ProcessTokSelexpr();
                        groupByProcessor.setParseFromResult(parseFromResult);
                        groupByColumnSet = groupByProcessor.parseSelect(groupByExpr);
                        logger.debug("TOK_GROUPBY: child " + i + " extracted columns: " + groupByColumnSet);
                    }

                    // 添加到 groupByFields 中（用于聚合函数血缘）
                    groupByFields.addAll(groupByColumnSet);
                }
                logger.debug("TOK_GROUPBY: collected " + groupByFields.size() + " group by fields: " + groupByFields);

                // 回过头来更新之前记录的聚合字段的血缘，添加 GROUP BY 字段作为依赖
                if (!groupByFields.isEmpty() && !aggregateColumnIndexes.isEmpty()) {
                    for (int aggIndex : aggregateColumnIndexes) {
                        if (aggIndex < parseColumnResults.size()) {
                            ParseColumnResult aggColumn = parseColumnResults.get(aggIndex);
                            logger.debug("TOK_GROUPBY: Adding GROUP BY fields to aggregate field " +
                                       aggColumn.getAliasName() + " at index " + aggIndex +
                                       ", current fromTableColumnSet=" + aggColumn.getFromTableColumnSet());

                            // 添加所有 GROUP BY 字段
                            aggColumn.getFromTableColumnSet().addAll(groupByFields);

                            logger.debug("TOK_GROUPBY: After adding GROUP BY fields, fromTableColumnSet=" +
                                       aggColumn.getFromTableColumnSet());
                        }
                    }
                }

                // 清空聚合字段索引列表，为下一个查询准备
                aggregateColumnIndexes.clear();
                break;

            default:
                break;
        }
    }

    /**
     * 构建血缘数据
     */
    private void buildLineageData() {
        for (int i = 0; i < targetTableColumns.size(); i++) {
            String columnName = targetTableColumns.get(i);
            Set<String> fromTableColumnSet;

            if (parseAllColref.size() > 0) {
                ParseColumnResult columnResult = getIndexColumnResult(parseAllColref, i);
                if (columnResult != null) {
                    fromTableColumnSet = columnResult.getFromTableColumnSet();
                } else {
                    fromTableColumnSet = new HashSet<>();
                }
            } else {
                if (i < parseColumnResults.size()) {
                    fromTableColumnSet = parseColumnResults.get(i).getFromTableColumnSet();
                } else {
                    fromTableColumnSet = new HashSet<>();
                }
            }

            String fullColumnName = targetTable + "." + columnName;
            lineageData.put(fullColumnName, fromTableColumnSet);
            logger.debug("Field: " + fullColumnName + " depends on: " + fromTableColumnSet);
        }

        parseAllColref.clear();
        parseColumnResults.clear();
        targetTableColumns.clear();
    }

    private void parseASTNode(ASTNode ast) {
        if (ast == null) {
            return;
        }

        if (ast.getToken() != null && ast.getToken().getType() == HiveParser.TOK_SUBQUERY) {
            HiveSqlLineageParser subParser = new HiveSqlLineageParser(metadataProvider);
            subParser.parseWithResults = new ArrayList<>(parseWithResults);
            subParser.parseASTNode((ASTNode) ast.getChild(0));

            Map<String, ParseColumnResult> subQueryColumnMap;
            if (subParser.getParseUnionColumnResults().size() > 0) {
                subQueryColumnMap = subParser.getParseUnionColumnResults();
            } else {
                subQueryColumnMap = subParser.getParseQueryResults().get(0);
            }

            ParseSubQueryResult parseSubQueryResult = new ParseSubQueryResult();
            String subQueryAliasName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
            parseSubQueryResult.setAliasName(subQueryAliasName);

            Map<String, ParseColumnResult> selectResults = new HashMap<>(subQueryColumnMap);
            parseSubQueryResult.setParseSubQueryResults(selectResults);
            logger.debug("TOK_SUBQUERY: " + parseSubQueryResult);
            parseSubQueryResults.add(parseSubQueryResult);

            // 同时将子查询结果存储到 processedSubQueries，以便后续引用
            processedSubQueries.put(subQueryAliasName, new HashMap<>(selectResults));
            logger.debug("TOK_SUBQUERY: Added to processedSubQueries with alias " + subQueryAliasName);
        } else {
            parseChildASTNode(ast);
            parseCurrentASTNode(ast);
        }
    }

    // 用于子解析的内部访问方法
    private List<Map<String, ParseColumnResult>> getParseQueryResults() {
        return parseQueryResults;
    }

    private Map<String, ParseColumnResult> getParseUnionColumnResults() {
        return parseUnionColumnResults;
    }

    // 公开的setter，用于子查询时传递状态
    private void setParseWithResults(List<ParseWithResult> parseWithResults) {
        this.parseWithResults = parseWithResults;
    }

    /**
     * 获取源表列表
     */
    public Set<String> getFromTables() {
        return new HashSet<>(fromTables);
    }

    /**
     * 获取目标表名
     */
    public String getTargetTable() {
        return targetTable;
    }
}
