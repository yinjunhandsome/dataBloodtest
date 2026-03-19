package org.example.hive.process;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.log4j.Logger;
import org.example.hive.bean.ParseColumnResult;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class ProcessTokSelexpr {
    private static Logger logger = Logger.getLogger(ProcessTokSelexpr.class);

    private Map<String, ParseColumnResult> parseFromResult;
    private Set<String> groupByFields;

    public void setParseFromResult(Map<String, ParseColumnResult> parseFromResult) {
        this.parseFromResult = parseFromResult;
    }

    public void setGroupByFields(Set<String> groupByFields) {
        this.groupByFields = groupByFields;
    }

    public static ParseColumnResult getResultByColumn(Map<String, ParseColumnResult> map, String column){
        ParseColumnResult parseColumnResult = null;
        for (Map.Entry<String, ParseColumnResult> entity : map.entrySet()) {
            String columnName = entity.getKey();
            if (columnName.contains(".")) {
                columnName = columnName.split("\\.")[1];
            }
            if (columnName.equals(column)) {
                parseColumnResult = entity.getValue();
            }
        }
        return parseColumnResult;
    }

    public Set<String> parseSelect(ASTNode ast) {
        // 依赖的字段列表
        Set<String> fromColumns = new TreeSet();

        if (ast.getType() == HiveParser.DOT && ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                && ast.getChild(0).getChildCount() == 1 && ast.getChild(1).getType() == HiveParser.Identifier) {
            // 字段 有别名
            String column = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
            String alias = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(0));

            String columnFull = alias + "." + column;
            if (parseFromResult.containsKey(columnFull)) {
                fromColumns.addAll(parseFromResult.get(columnFull).getFromTableColumnSet());
            } else {
                logger.error("columnFull: " + columnFull + "has no source..");
            }
        } else if (ast.getType() == HiveParser.TOK_TABLE_OR_COL && ast.getChildCount() == 1
                && ast.getChild(0).getType() == HiveParser.Identifier) {
            // 字段 无别名
            String column = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
            ParseColumnResult parseColumnResult = getResultByColumn(parseFromResult, column);
            if (parseColumnResult != null) {
                fromColumns.addAll(parseColumnResult.getFromTableColumnSet());
            }
        } else {
            int cnt = ast.getChildCount();
            for (int i = 0; i < cnt; i++) {
                fromColumns.addAll(parseSelect((ASTNode) ast.getChild(i)));
            }
        }
        return fromColumns;
    }

    /**
     * 提取聚合函数内部的参数字段
     */
    private Set<String> extractAggregateFunctionFields(ASTNode ast) {
        Set<String> fields = new TreeSet<>();
        if (ast == null) {
            return fields;
        }

        // 检查是否是聚合函数节点
        if (ast.getType() == HiveParser.TOK_FUNCTION ||
            ast.getType() == HiveParser.TOK_FUNCTIONDI ||
            ast.getType() == HiveParser.TOK_FUNCTIONSTAR) {

            // TOK_FUNCTIONSTAR 是 count(*) 这种形式
            if (ast.getType() == HiveParser.TOK_FUNCTIONSTAR) {
                // count(*) 应该包含所有字段
                // 这里返回一个特殊标记，表示是 count(*)
                fields.add("*");
                return fields;
            }

            // 对于其他聚合函数，递归提取参数字段
            for (int i = 1; i < ast.getChildCount(); i++) {
                ASTNode child = (ASTNode) ast.getChild(i);
                fields.addAll(extractFieldsFromExpression(child));
            }
        }

        return fields;
    }

    /**
     * 从表达式中提取字段（不递归进入聚合函数）
     */
    private Set<String> extractFieldsFromExpression(ASTNode ast) {
        Set<String> fields = new TreeSet<>();
        if (ast == null) {
            return fields;
        }

        // 处理字段引用
        if (ast.getType() == HiveParser.DOT &&
            ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL &&
            ast.getChild(0).getChildCount() == 1 &&
            ast.getChild(1).getType() == HiveParser.Identifier) {
            // 有别名的字段: t.column
            String column = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
            String alias = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(0));
            String columnFull = alias + "." + column;
            if (parseFromResult.containsKey(columnFull)) {
                fields.addAll(parseFromResult.get(columnFull).getFromTableColumnSet());
            }
        } else if (ast.getType() == HiveParser.TOK_TABLE_OR_COL &&
                   ast.getChildCount() == 1 &&
                   ast.getChild(0).getType() == HiveParser.Identifier) {
            // 无别名的字段: column
            String column = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
            ParseColumnResult parseColumnResult = getResultByColumn(parseFromResult, column);
            if (parseColumnResult != null) {
                fields.addAll(parseColumnResult.getFromTableColumnSet());
            }
        } else if (!(ast.getType() == HiveParser.TOK_FUNCTION ||
                    ast.getType() == HiveParser.TOK_FUNCTIONDI ||
                    ast.getType() == HiveParser.TOK_FUNCTIONSTAR)) {
            // 对于非聚合函数节点，递归处理子节点
            for (int i = 0; i < ast.getChildCount(); i++) {
                fields.addAll(extractFieldsFromExpression((ASTNode) ast.getChild(i)));
            }
        }

        return fields;
    }

    /**
     * 检查表达式是否包含聚合函数
     */
    private boolean containsAggregateFunction(ASTNode ast) {
        if (ast == null) {
            return false;
        }

        // 检查是否是聚合函数节点
        if (ast.getType() == HiveParser.TOK_FUNCTION ||
            ast.getType() == HiveParser.TOK_FUNCTIONDI ||
            ast.getType() == HiveParser.TOK_FUNCTIONSTAR) {

            // 获取函数名
            if (ast.getChildCount() > 0) {
                ASTNode firstChild = (ASTNode) ast.getChild(0);
                // 第一个子节点是 Identifier，存储函数名
                if (firstChild.getType() == HiveParser.Identifier) {
                    String functionName = BaseSemanticAnalyzer.getUnescapedName(firstChild);
                    logger.debug("Found function: " + functionName + ", isAggregate: " + isAggregateFunctionName(functionName));
                    if (isAggregateFunctionName(functionName)) {
                        return true;
                    }
                }
                // 也可能直接是 String 类型存储函数名
                else if (firstChild.getText() != null) {
                    String functionName = firstChild.getText();
                    logger.debug("Found function (text): " + functionName + ", isAggregate: " + isAggregateFunctionName(functionName));
                    if (isAggregateFunctionName(functionName)) {
                        return true;
                    }
                }
            }
        }

        // 递归检查子节点
        for (int i = 0; i < ast.getChildCount(); i++) {
            if (containsAggregateFunction((ASTNode) ast.getChild(i))) {
                return true;
            }
        }

        return false;
    }

    /**
     * 判断函数名是否为聚合函数
     */
    private boolean isAggregateFunctionName(String functionName) {
        if (functionName == null) {
            return false;
        }
        String lowerName = functionName.toLowerCase();
        return "count".equals(lowerName) ||
               "sum".equals(lowerName) ||
               "avg".equals(lowerName) ||
               "min".equals(lowerName) ||
               "max".equals(lowerName) ||
               "stddev".equals(lowerName) ||
               "variance".equals(lowerName) ||
               "collect_list".equals(lowerName) ||
               "collect_set".equals(lowerName) ||
               "approx_count_distinct".equals(lowerName);
    }

    public String getColumnAliasName(ASTNode ast) {
        int childIndex = ast.getChildIndex();

        ASTNode childAst = (ASTNode) ast.getChild(0);
        String columnAliasName;
        if (ast.getChild(1) != null) {
            // 有字段别名
            columnAliasName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
        } else if (childAst.getType() == HiveParser.DOT && childAst.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                && childAst.getChild(0).getChildCount() == 1 && childAst.getChild(1).getType() == HiveParser.Identifier) {
            // 没有别名，但是使用的 t1.xx 格式
            String columnName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) childAst.getChild(1));
            columnAliasName = columnName;
        } else if (childAst.getToken().getType() == HiveParser.TOK_TABLE_OR_COL) {
            // select column
            columnAliasName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) childAst.getChild(0));
        } else {
            // 使用的元数据获取到的 insert table 的字段
            columnAliasName = "col_" + childIndex;
        }
        return columnAliasName;
    }

    private boolean hasAllcolref(ASTNode ast) {
        boolean flag = false;
        for (int i = 0; i < ast.getParent().getChildCount() - ast.getChildIndex(); i++) {
            if (ast.getParent().getChild(i).getChild(0).getType() == HiveParser.TOK_ALLCOLREF) {
                flag = true;
                break;
            }
        }
        return flag;
    }

    private int getChildIndex(ASTNode ast) {
        int childIndex;
        if (hasAllcolref(ast)) {
            childIndex = ast.getChildIndex() + parseFromResult.size() - 1;
        } else {
            childIndex = ast.getChildIndex();
        }
        return childIndex;
    }

    public ParseColumnResult process(ASTNode ast) {
        Set<String> fromColumnSet = parseSelect((ASTNode) ast.getChild(0));
        String columnAliasName = getColumnAliasName(ast);

        // 检查是否包含聚合函数
        boolean hasAggregate = containsAggregateFunction((ASTNode) ast.getChild(0));

        logger.debug("process: field=" + columnAliasName + ", hasAggregate=" + hasAggregate +
                    ", groupByFields=" + groupByFields + ", initial fromColumnSet=" + fromColumnSet);

        // 检查是否引用了子查询中的聚合字段
        boolean referencesAggregateField = false;
        ParseColumnResult referencedAggregateResult = null;
        ASTNode expr = (ASTNode) ast.getChild(0);

        // 简单的字段引用 (如 page.pv 或 pv)
        if (expr.getType() == HiveParser.DOT &&
            expr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL &&
            expr.getChild(0).getChildCount() == 1 &&
            expr.getChild(1).getType() == HiveParser.Identifier) {
            // 有别名的字段: t.column
            String column = BaseSemanticAnalyzer.getUnescapedName((ASTNode) expr.getChild(1));
            String alias = BaseSemanticAnalyzer.getUnescapedName((ASTNode) expr.getChild(0).getChild(0));
            String columnFull = alias + "." + column;
            if (parseFromResult.containsKey(columnFull)) {
                ParseColumnResult referencedResult = parseFromResult.get(columnFull);
                if (referencedResult.isAggregate()) {
                    referencesAggregateField = true;
                    referencedAggregateResult = referencedResult;
                    logger.debug("process: field " + columnAliasName + " references aggregate field " + columnFull);
                }
            }
        } else if (expr.getType() == HiveParser.TOK_TABLE_OR_COL &&
                   expr.getChildCount() == 1 &&
                   expr.getChild(0).getType() == HiveParser.Identifier) {
            // 无别名的字段: column
            String column = BaseSemanticAnalyzer.getUnescapedName((ASTNode) expr.getChild(0));
            ParseColumnResult referencedResult = getResultByColumn(parseFromResult, column);
            if (referencedResult != null && referencedResult.isAggregate()) {
                referencesAggregateField = true;
                referencedAggregateResult = referencedResult;
                logger.debug("process: field " + columnAliasName + " references aggregate field " + column);
            }
        }

        // 如果当前表达式没有聚合函数，但引用了聚合字段，则继承聚合属性
        if (!hasAggregate && referencesAggregateField) {
            hasAggregate = true;
            // 将引用的聚合字段的血缘合并到当前字段的血缘中
            if (referencedAggregateResult != null) {
                fromColumnSet.addAll(referencedAggregateResult.getFromTableColumnSet());
                logger.debug("process: field " + columnAliasName + " inherited aggregate property and lineage from referenced field: " +
                           referencedAggregateResult.getFromTableColumnSet());
            } else {
                logger.debug("process: field " + columnAliasName + " inherited aggregate property from referenced field");
            }
        }

        if (hasAggregate) {
            logger.debug("process: field=" + columnAliasName + " contains aggregate function, extracting params");

            // 提取聚合函数内部的参数字段
            Set<String> aggregateParams = extractAggregateFunctionFields((ASTNode) ast.getChild(0));
            logger.debug("process: aggregate function params extracted: " + aggregateParams);

            // 检查是否是 count(*)
            if (aggregateParams.contains("*")) {
                // count(*) 只依赖 GROUP BY 字段（不添加其他字段）
                // 因为 count(*) 的语义是计数所有行，它依赖于 GROUP BY 的维度
                logger.debug("process: count(*) detected, will only add GROUP BY fields");
            } else if (!aggregateParams.isEmpty()) {
                // 对于 count(column)、sum(amount) 等，添加聚合函数的参数字段
                fromColumnSet.addAll(aggregateParams);
                logger.debug("process: added aggregate function param fields: " + aggregateParams);
            }

            // 无论如何，如果包含 GROUP BY 字段，都添加 GROUP BY 字段作为依赖
            if (groupByFields != null && !groupByFields.isEmpty()) {
                // 将所有 GROUP BY 字段添加到依赖中
                fromColumnSet.addAll(groupByFields);
                logger.debug("process: added " + groupByFields.size() + " GROUP BY fields as dependencies: " + groupByFields);
            }
        }

        ParseColumnResult parseColumnResult = new ParseColumnResult();
        parseColumnResult.setIndex(getChildIndex(ast));
        parseColumnResult.setAliasName(columnAliasName);
        parseColumnResult.setFromTableColumnSet(fromColumnSet);
        parseColumnResult.setAggregate(hasAggregate);

        logger.debug("process: final fromColumnSet=" + fromColumnSet);

        return parseColumnResult;
    }
}
