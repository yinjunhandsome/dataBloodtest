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
    private List<ParseTableResult> parseTableResults = new ArrayList<>();
    private List<ParseJoinResult> parseJoinResults = new ArrayList<>();
    private List<ParseSubQueryResult> parseSubQueryResults = new ArrayList<>();
    private List<ParseWithResult> parseWithResults = new ArrayList<>();

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
        parseTableResults.clear();
        parseJoinResults.clear();
        parseSubQueryResults.clear();
        parseWithResults.clear();
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
                            return ProcessWithData.process(parseWithResults);
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
                Map<String, ParseColumnResult> newParseColumnResultMap = new HashMap<>();
                Map<String, ParseColumnResult> leftParseColumnResultMap;
                Map<String, ParseColumnResult> RightParseColumnResultMap;

                if (ast.getChild(0).getType() == HiveParser.TOK_UNIONALL) {
                    leftParseColumnResultMap = parseUnionColumnResults;
                    RightParseColumnResultMap = parseQueryResults.get(0);
                } else {
                    leftParseColumnResultMap = parseQueryResults.get(0);
                    RightParseColumnResultMap = parseQueryResults.get(1);
                }

                for (Map.Entry<String, ParseColumnResult> entry : leftParseColumnResultMap.entrySet()) {
                    String columnAliasName = entry.getKey();
                    ParseColumnResult parseColumnResult = entry.getValue();
                    int childIndex = parseColumnResult.getIndex();

                    ParseColumnResult rightColumnResult = getIndexColumnResult(RightParseColumnResultMap, childIndex);
                    if (rightColumnResult != null) {
                        Set<String> otherUnionFromColumnSet = rightColumnResult.getFromTableColumnSet();
                        parseColumnResult.getFromTableColumnSet().addAll(otherUnionFromColumnSet);
                    }
                    newParseColumnResultMap.put(columnAliasName, parseColumnResult);
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
                    ParseColumnResult parseColumnResult = processTokSelexpr.process(ast);
                    logger.debug("TOK_SELEXPR: " + parseColumnResult);
                    parseColumnResults.add(parseColumnResult);
                }
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
