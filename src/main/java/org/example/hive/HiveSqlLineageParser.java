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
        } else if (ast.getToken() != null && ast.getToken().getType() == HiveParser.TOK_QUERY &&
                ast.getChild(2) != null && ast.getChild(2).getType() == HiveParser.TOK_CTE) {
            for (int i = 0; i < ast.getChild(2).getChildCount(); i++) {
                parseASTNode((ASTNode) ast.getChild(2).getChild(i));
                for (int j = i; j < parseSubQueryResults.size(); j++) {
                    ParseWithResult parseWithResult = new ParseWithResult();
                    parseWithResult.setTableName(parseSubQueryResults.get(i).getAliasName());
                    Map<String, ParseColumnResult> parseSubQueryResultTmp = new HashMap<>();
                    parseSubQueryResultTmp.putAll(parseSubQueryResults.get(i).getParseSubQueryResults());
                    parseWithResult.setParseSubQueryResults(parseSubQueryResultTmp);
                    parseWithResults.add(parseWithResult);
                }
            }
            parseSubQueryResults.clear();
            parseASTNode((ASTNode) ast.getChild(0));
            parseASTNode((ASTNode) ast.getChild(1));
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
                if (ast.getChild(0).getChildCount() == 1) {
                    String tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(0));
                    for (ParseWithResult parseWithResult : parseWithResults) {
                        if (parseWithResult.getTableName().equals(tableName)) {
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
                            return cteResult;
                        }
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
            case HiveParser.TOK_MAPJOIN:
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
                Map<String, ParseColumnResult> queryColumnMapTmp = new HashMap<>(parseSelectResults);
                parseQueryResults.add(queryColumnMapTmp);
                parseSelectResults.clear();
                break;

            case HiveParser.TOK_INSERT:
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
            case HiveParser.TOK_MAPJOIN:
            case HiveParser.TOK_FULLOUTERJOIN:
            case HiveParser.TOK_UNIQUEJOIN:
                ParseJoinResult parseJoinResult = new ParseJoinResult();

                List<ParseTableResult> tableResults = new ArrayList<>(parseTableResults);
                parseTableResults.clear();
                parseJoinResult.setParseTableResults(tableResults);

                List<ParseJoinResult> joinResults = new ArrayList<>(parseJoinResults);
                parseJoinResults.clear();
                parseJoinResult.setParseJoinResults(joinResults);

                List<ParseSubQueryResult> subQueryResults = new ArrayList<>(parseSubQueryResults);
                parseSubQueryResults.clear();
                parseJoinResult.setParseSubQueryResults(subQueryResults);

                List<ParseWithResult> withResults = new ArrayList<>(parseWithResults);
                parseJoinResult.setParseWithResults(withResults);

                logger.debug("TOK_JOIN: " + parseJoinResult);
                parseJoinResults.add(parseJoinResult);
                break;

            case HiveParser.TOK_TABREF:
                String fromTableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                Boolean isWithTable = Boolean.FALSE;
                for (ParseWithResult parseWithResult : parseWithResults) {
                    if (parseWithResult.getTableName().equals(fromTableName)) {
                        isWithTable = Boolean.TRUE;
                        if (ast.getChild(1) != null) {
                            String withTableAlias = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
                            parseWithResult.setAliasName(withTableAlias);
                        }
                        break;
                    }
                }
                if (!isWithTable) {
                    fromTables.add(fromTableName);
                    ParseTableResult parseTableResult = processTokTabref(ast);
                    logger.debug("TOK_TABREF: " + parseTableResult);
                    parseTableResults.add(parseTableResult);
                }
                break;

            case HiveParser.TOK_SELEXPR:
                int childIndex = ast.getChildIndex();
                if (ast.getChild(0).getType() == HiveParser.TOK_ALLCOLREF) {
                    for (Map.Entry<String, ParseColumnResult> entry : parseFromResult.entrySet()) {
                        ParseColumnResult parseColumnResult = entry.getValue();
                        parseColumnResult.setIndex(parseColumnResult.getIndex() + childIndex);
                        parseAllColref.put(parseColumnResult.getAliasName(), parseColumnResult);
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
