package org.example.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.*;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.expressions.aggregate.*;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.*;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.execution.command.*;
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand;
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Spark LogicalPlan 字段级血缘解析器
 *
 * 特性：
 * 1. 递归解析LogicalPlan树形结构
 * 2. 支持所有主流Spark算子
 * 3. 输出字段级血缘链路
 * 4. 自动追溯源头字段
 * 5. 支持CTE、窗口函数、聚合函数等复杂表达式
 *
 * @author Claude Code
 * @version 1.0
 */

//血缘
//1.查询，查询字段->来源表字段
//2.插入，插入字段(来源于真实表)->来源字段(常量插入为空)
public class LogicalPlanLineageParser {

    // ==================== 调试开关 ====================
    // 设为false关闭所有调试输出，设为true开启详细日志
    private static final boolean DEBUG_MODE = true;


    /**
     * 解析入口：解析LogicalPlan生成字段级血缘
     *
     * @param plan Spark逻辑计划
     * @return 字段名 -> 字段血缘 映射
     */
    public static Map<String, FieldLineage> parse(LogicalPlan plan) {
        if (plan == null) {
            return new HashMap<>();
        }
        return parseLogicalPlan(plan, new HashMap<>());
    }

    /**
     * 递归解析逻辑计划
     */
    private static Map<String, FieldLineage> parseLogicalPlan(LogicalPlan plan, Map<String, Map<String, FieldLineage>> cteCache) {
        if (plan == null) {
            return new HashMap<>();
        }

        try {
            // CTE相关
            if (plan instanceof WithCTE) {
                return parseWithCTE((WithCTE) plan, cteCache);
            }
            if (plan instanceof UnresolvedWith) {
                return parseUnresolvedWith((UnresolvedWith) plan, cteCache);
            }
            if (plan instanceof CTERelationRef) {
                return parseCTERelationRef((CTERelationRef) plan, cteCache);
            }
            if (plan instanceof SubqueryAlias) {
                return parseSubqueryAlias((SubqueryAlias) plan, cteCache);
            }

            // 数据源
            if (plan instanceof LogicalRelation) {
                return parseLogicalRelation((LogicalRelation) plan);
            }
            if (plan instanceof HiveTableRelation) {
                return parseHiveTableRelation((HiveTableRelation) plan);
            }
            if (plan instanceof UnresolvedRelation) {
                return parseUnresolvedRelation((UnresolvedRelation) plan);
            }

            // 字段变换
            if (plan instanceof Project) {
                return parseProject((Project) plan, cteCache);
            }
            if (plan instanceof Aggregate) {
                return parseAggregate((Aggregate) plan, cteCache);
            }
            if (plan instanceof Window) {
                return parseWindow((Window) plan, cteCache);
            }
            if (plan instanceof Filter) {
                return parseFilter((Filter) plan, cteCache);
            }
            if (plan instanceof Sort) {
                return parseSort((Sort) plan, cteCache);
            }
            if (plan instanceof Distinct) {
                return parseDistinct((Distinct) plan, cteCache);
            }
            if (plan instanceof GlobalLimit || plan instanceof LocalLimit) {
                return parseLimit(plan, cteCache);
            }
            if (plan instanceof Generate) {
                return parseGenerate((Generate) plan, cteCache);
            }
            if (plan instanceof Expand) {
                return parseExpand((Expand) plan, cteCache);
            }

            // Join
            if (plan instanceof Join) {
                return parseJoin((Join) plan, cteCache);
            }

            // 集合操作
            if (plan instanceof Union) {
                return parseUnion((Union) plan, cteCache);
            }
            if (plan instanceof Intersect) {
                return parseIntersect((Intersect) plan, cteCache);
            }
            if (plan instanceof Except) {
                return parseExcept((Except) plan, cteCache);
            }

            // 其他算子
            if (plan instanceof Deduplicate) {
                return parseDeduplicate((Deduplicate) plan, cteCache);
            }
            if (plan instanceof Sample) {
                return parseSample((Sample) plan, cteCache);
            }
            if (plan instanceof Repartition || plan instanceof RepartitionByExpression) {
                return parseRepartition(plan, cteCache);
            }
            if (plan instanceof View) {
                return parseView((View) plan, cteCache);
            }
            if (plan instanceof Tail) {
                return parseTail((Tail) plan, cteCache);
            }
            if (plan instanceof OneRowRelation) {
                return parseOneRowRelation();
            }

            // ==================== 建表和写入算子 ====================
            if (plan instanceof LoadDataCommand) {
                return parseLoadData((LoadDataCommand) plan);
            }
            if (plan instanceof CreateHiveTableAsSelectCommand) {
                return parseCreateHiveTableAsSelectCommand((CreateHiveTableAsSelectCommand) plan, cteCache);
            }
            if (plan instanceof InsertIntoHiveTable) {
                return parseInsertIntoHiveTable((InsertIntoHiveTable) plan, cteCache);
            }
            if (plan instanceof InsertIntoHadoopFsRelationCommand) {
                return parseInsertIntoHadoopFsRelationCommand((InsertIntoHadoopFsRelationCommand) plan, cteCache);
            }
            //todo 这个是spark自己管理的catalog表的算子，暂时不确定要不要处理，需确定spark操作的表是否都是hive表
//            if (plan instanceof SaveIntoDataSourceCommand) {
//                return parseSaveIntoDataSourceCommand((SaveIntoDataSourceCommand) plan, cteCache);
//            }
            //todo 这个是spark自己管理的catalog表的算子，暂时不确定要不要处理，需确定spark操作的表是否都是hive表
//            if (plan instanceof CreateDataSourceTableCommand) {
//                return parseCreateDataSourceTableCommand((CreateDataSourceTableCommand) plan, cteCache);
//            }
            if (plan instanceof CreateTableCommand) {
                return parseCreateTableCommand((CreateTableCommand) plan, cteCache);
            }
            if (plan instanceof DropTableCommand) {
                return parseDropTableCommand((DropTableCommand) plan);
            }
            if (plan instanceof AlterTableAddPartitionCommand) {
                return parseAlterTableAddPartitionCommand((AlterTableAddPartitionCommand) plan);
            }
            if (plan instanceof AlterTableDropPartitionCommand) {
                return parseAlterTableDropPartitionCommand((AlterTableDropPartitionCommand) plan);
            }
            // CacheTableCommand 和 UncacheTableCommand 在Spark 3.3.4中可能不存在，跳过
            if (plan instanceof RefreshTableCommand) {
                return parseRefreshTableCommand((RefreshTableCommand) plan);
            }
            if (plan instanceof ShowTablesCommand) {
                return parseShowTablesCommand();
            }
            if (plan instanceof ShowColumnsCommand) {
                return parseShowColumnsCommand((ShowColumnsCommand) plan);
            }
            if (plan instanceof ShowPartitionsCommand) {
                return parseShowPartitionsCommand((ShowPartitionsCommand) plan);
            }
            if (plan instanceof DescribeTableCommand) {
                return parseDescribeTableCommand((DescribeTableCommand) plan);
            }
            if (plan instanceof AnalyzeTableCommand) {
                return parseAnalyzeTableCommand((AnalyzeTableCommand) plan);
            }

            // 湖表
//            if (plan instanceof AppendData) {
//                return parseAppendData((AppendData) plan, cteCache);
//            }
            if (plan instanceof OverwriteByExpression) {
                return parseOverwriteByExpression((OverwriteByExpression) plan, cteCache);
            }
            if (plan instanceof OverwritePartitionsDynamic) {
                return parseOverwritePartitionsDynamic((OverwritePartitionsDynamic) plan, cteCache);
            }

            // 未知算子：穿透
            return parseUnknown(plan, cteCache);

        } catch (Exception e) {
            System.err.println("解析节点失败 [" + plan.nodeName() + "]: " + e.getMessage());
            return new HashMap<>();
        }
    }

    // ==================== CTE相关 ====================

    private static Map<String, FieldLineage> parseWithCTE(WithCTE plan, Map<String, Map<String, FieldLineage>> cteCache) {
        System.out.println("===== DEBUG: WithCTE =====");
        // 先解析所有CTE定义并缓存
        Seq<CTERelationDef> cteDefs = plan.cteDefs();
        if (cteDefs != null && !cteDefs.isEmpty()) {
            System.out.println("Found " + cteDefs.size() + " CTE definitions");
            Iterator<CTERelationDef> it = cteDefs.iterator();
            while (it.hasNext()) {
                CTERelationDef def = it.next();
                if (def != null) {
                    System.out.println("  Parsing CTE: " + def.id());
                    System.out.println("    CTE child node: " + def.child().getClass().getSimpleName());
                    try {
                        Map<String, FieldLineage> cteFields = parseLogicalPlan(def.child(), cteCache);
                        System.out.println("  CTE " + def.id() + " has " + cteFields.size() + " fields");
                        if (cteFields.isEmpty()) {
                            System.out.println("  WARNING: CTE " + def.id() + " has NO fields!");
                        }
                        cteCache.put("CTE_" + def.id(), cteFields);
                    } catch (Exception e) {
                        System.out.println("  ERROR parsing CTE " + def.id() + ": " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
        }
        // 返回主查询血缘
        System.out.println("Parsing main query plan");
        return parseLogicalPlan(plan.plan(), cteCache);
    }

    private static Map<String, FieldLineage> parseUnresolvedWith(UnresolvedWith plan, Map<String, Map<String, FieldLineage>> cteCache) {
        Seq<Tuple2<String, SubqueryAlias>> cteRelations = plan.cteRelations();
        if (cteRelations != null && !cteRelations.isEmpty()) {
            Iterator<Tuple2<String, SubqueryAlias>> it = cteRelations.iterator();
            while (it.hasNext()) {
                Tuple2<String, SubqueryAlias> tuple = it.next();
                if (tuple != null && tuple._2() != null) {
                    Map<String, FieldLineage> cteFields = parseLogicalPlan(tuple._2().child(), cteCache);
                    cteCache.put("CTE_" + tuple._1(), cteFields);
                }
            }
        }
        return parseLogicalPlan(plan.child(), cteCache);
    }

    private static Map<String, FieldLineage> parseCTERelationRef(CTERelationRef plan, Map<String, Map<String, FieldLineage>> cteCache) {
        String cacheKey = "CTE_" + plan.cteId();
        System.out.println("===== DEBUG: CTERelationRef =====");
        System.out.println("CTE ID: " + plan.cteId());
        System.out.println("Cache Key: " + cacheKey);

        Map<String, FieldLineage> cached = cteCache.get(cacheKey);
        if (cached != null) {
            System.out.println("Found cached CTE with " + cached.size() + " fields:");
            for (Map.Entry<String, FieldLineage> entry : cached.entrySet()) {
                FieldLineage lineage = entry.getValue();
                System.out.println("  - " + entry.getKey() + " -> dependencies=" + lineage.getDependencies().size());
                if (lineage.getDependencies().size() > 0) {
                    for (FieldLineage dep : lineage.getDependencies()) {
                        System.out.println("      -> " + dep.getFieldName() + " (" + dep.getTableName() + ")");
                    }
                }
            }

            // 规范化：移除所有键的前缀，只保留纯字段名
            Map<String, FieldLineage> normalizedResult = new HashMap<>();
            for (Map.Entry<String, FieldLineage> entry : cached.entrySet()) {
                String originalKey = entry.getKey();
                FieldLineage lineage = cloneFieldLineage(entry.getValue());

                // 移除前缀
                String normalizedKey = originalKey;
                if (originalKey.contains(".")) {
                    normalizedKey = originalKey.substring(originalKey.lastIndexOf('.') + 1);
                    System.out.println("  Normalizing key: '" + originalKey + "' -> '" + normalizedKey + "'");
                }

                // 如果键已存在，合并依赖而不是覆盖
                if (normalizedResult.containsKey(normalizedKey)) {
                    FieldLineage existing = normalizedResult.get(normalizedKey);
                    System.out.println("  WARNING: Key conflict! Merging dependencies for '" + normalizedKey + "'");
                    for (FieldLineage dep : lineage.getDependencies()) {
                        existing.addDependency(cloneFieldLineage(dep));
                    }
                } else {
                    normalizedResult.put(normalizedKey, lineage);
                }
            }

            System.out.println("Returning " + normalizedResult.size() + " normalized fields from cache");
            return normalizedResult;
        }
        System.out.println("WARNING: No cached data found for CTE " + cacheKey);
        return new HashMap<>();
    }

    private static Map<String, FieldLineage> parseSubqueryAlias(SubqueryAlias plan, Map<String, Map<String, FieldLineage>> cteCache) {
        Map<String, FieldLineage> childFields = parseLogicalPlan(plan.child(), cteCache);
        // 重命名表名
        String alias = plan.alias();
        Map<String, FieldLineage> result = new HashMap<>();
        for (Map.Entry<String, FieldLineage> entry : childFields.entrySet()) {
            FieldLineage original = entry.getValue();
            FieldLineage aliased = cloneFieldLineage(original);
            aliased.setTableName(alias);
            result.put(entry.getKey(), aliased);
        }
        return result;
    }

    // ==================== 数据源 ====================

    private static Map<String, FieldLineage> parseLogicalRelation(LogicalRelation plan) {
        Map<String, FieldLineage> result = new HashMap<>();

        Option<CatalogTable> catalogTableOpt = plan.catalogTable();
        if (catalogTableOpt.isEmpty()) {
            return result;
        }

        CatalogTable table = catalogTableOpt.get();
        String dbName = table.database() != null ? table.database() : "default";
        String tblName = table.identifier().table();
        String fullTableName = dbName + "." + tblName;

        Seq<AttributeReference> output = plan.output();
        if (output != null && !output.isEmpty()) {
            Iterator<AttributeReference> it = output.iterator();
            while (it.hasNext()) {
                Attribute attr = it.next();
                if (attr != null) {
                    String fieldName = attr.name();
                    FieldLineage lineage = new FieldLineage(fieldName, fullTableName);
                    lineage.setFieldType(FieldLineage.FieldType.COLUMN);
                    lineage.setSourceTableName(fullTableName);
                    result.put(fieldName, lineage);
                }
            }
        }

        return result;
    }
    private static Map<String, FieldLineage> parseHiveTableRelation(HiveTableRelation plan) {
        Map<String, FieldLineage> result = new HashMap<>();
        String dbName = StringUtils.isBlank(plan.tableMeta().database())?"default":plan.tableMeta().database();
        String tblName = plan.tableMeta().identifier().table();
        String fullTableName = dbName + "." + tblName;
        Seq<AttributeReference> output = plan.dataCols();
        if (output != null && !output.isEmpty()) {
            Iterator<AttributeReference> it = output.iterator();
            while (it.hasNext()) {
                Attribute attr = it.next();
                if (attr != null) {
                    String fieldName = attr.name();
                    FieldLineage lineage = new FieldLineage(fieldName, fullTableName);
                    lineage.setFieldType(FieldLineage.FieldType.COLUMN);
                    lineage.setSourceTableName(fullTableName);
                    result.put(fieldName, lineage);
                }
            }
        }
        return result;
    }



    private static Map<String, FieldLineage> parseUnresolvedRelation(UnresolvedRelation plan) {
        // 未解析表，返回空血缘
        return new HashMap<>();
    }

    // ==================== 字段变换 ====================

    private static Map<String, FieldLineage> parseProject(Project plan, Map<String, Map<String, FieldLineage>> cteCache) {
        Map<String, FieldLineage> childFields = parseLogicalPlan(plan.child(), cteCache);

        // 调试：打印 Project 的字段列表
        Seq<NamedExpression> projectList = plan.projectList();

        Map<String, FieldLineage> result = new HashMap<>();

        if (projectList != null && !projectList.isEmpty()) {
            Iterator<NamedExpression> it = projectList.iterator();
            while (it.hasNext()) {
                NamedExpression expr = it.next();
                if (expr == null) continue;

                // 安全获取字段名
                String fieldName;
                try {
                    //todo 这里需要再严谨判断,到底需不需要这么取全名
                    if (expr instanceof AttributeReference) {
                        fieldName=handleProjectExprName(expr.qualifiedName());
                    }
                    else {
                        fieldName = expr.name();
                    }
                    if (fieldName == null) {
                        continue;
                    }
                } catch (Exception e) {
                    // 未解析的表达式，检查是否为 Star (*)
                    String exprClassName = expr.getClass().getSimpleName();
                    System.out.println("      Expression error: " + e.getMessage() + ", class: " + exprClassName);

                    if (exprClassName.equals("Star") || exprClassName.equals("UnresolvedStar")) {
                        System.out.println("      Expanding " + exprClassName + " (*) - adding all " + childFields.size() + " child fields");
                        // Star 展开：添加所有子字段
                        for (Map.Entry<String, FieldLineage> entry : childFields.entrySet()) {
                            FieldLineage cloned = cloneFieldLineage(entry.getValue());
                            result.put(entry.getKey(), cloned);
                        }
                    }
                    continue;
                }

                FieldLineage lineage = new FieldLineage(fieldName, "PROJECT");

                // 判断字段类型
                if (expr instanceof Alias) {
                    Alias alias = (Alias) expr;
                    // 根据child的实际类型判断字段类型:
                    // - AttributeReference: 简单重命名 → ALIAS
                    // - Literal: 常量赋值 → CONSTANT (但用户可能期望CALCULATED)
                    // - 其他表达式(CAST/函数等): 计算字段 → CALCULATED
                    Expression child = alias.child();
                    if (child instanceof AttributeReference) {
                        lineage.setFieldType(FieldLineage.FieldType.ALIAS);
                    } else if (child instanceof Literal) {
                        lineage.setFieldType(FieldLineage.FieldType.LITERAL);
                    } else {
                        // CAST、函数调用等计算表达式
                        lineage.setFieldType(FieldLineage.FieldType.CALCULATED);
                    }

                    try {
                        lineage.setExpression(alias.child().sql());
                    } catch (Exception e) {
                        lineage.setExpression(alias.child().toString());
                    }

                    // 提取别名字段的依赖
                    Set<String> depNames = extractDependencies(alias.child());
                    if (DEBUG_MODE && !depNames.isEmpty()) {
                        System.out.println("      Field '" + fieldName + "' has " + depNames.size() + " dependencies: " + depNames);
                    }
                    for (String depName : depNames) {
                        FieldLineage dep = findFieldInChildFields(depName, childFields);
                        if (dep != null) {
                            lineage.addDependency(cloneFieldLineage(dep));
                            if (DEBUG_MODE) {
                                System.out.println("        Added dependency: " + depName + " (total dependencies: " + lineage.getDependencies().size() + ")");
                            }
                        } else {
                            if (DEBUG_MODE) {
                                System.out.println("        WARNING: Cannot find dependency '" + depName + "' in childFields");
                            }
                        }
                    }

                } else if (expr instanceof AttributeReference) {
                    lineage.setFieldType(FieldLineage.FieldType.COLUMN);
                    // 直接引用，从child获取
                    FieldLineage original = findFieldInChildFields(fieldName, childFields);
                    if (original != null) {
                        lineage.addDependency(cloneFieldLineage(original));
                    }
                } else if (expr instanceof UnresolvedAttribute) {
                    // 未解析的属性引用：字段不存在或SQL有错误
                    lineage.setFieldType(FieldLineage.FieldType.COLUMN);
                    lineage.setExpression(expr.toString());
                    // 尝试从childFields查找（可能只是带前缀的字段名）
                    FieldLineage original = findFieldInChildFields(fieldName, childFields);
                    if (original != null) {
                        lineage.addDependency(cloneFieldLineage(original));
                    } else {
                        // 尝试用 UnresolvedAttribute 的 name() 方法获取的字段名
                        String unresolvedName = ((UnresolvedAttribute) expr).name();
                        original = findFieldInChildFields(unresolvedName, childFields);
                        if (original != null) {
                            lineage.addDependency(cloneFieldLineage(original));
                        }
                    }
                } else {
                    // 计算字段 - 转换为Expression获取sql()
                    lineage.setFieldType(determineExpressionType((Expression) expr));
                    // 尝试获取SQL表达式
                    try {
                        lineage.setExpression(((Expression) expr).sql());
                    } catch (Exception e) {
                        lineage.setExpression(expr.toString());
                    }

                    // 提取依赖并追溯
                    Set<String> depNames = extractDependencies((Expression) expr);
                    for (String depName : depNames) {
                        FieldLineage dep = findFieldInChildFields(depName, childFields);
                        if (dep != null) {
                            lineage.addDependency(cloneFieldLineage(dep));
                        }
                    }
                }

                result.put(fieldName, lineage);
            }
        }

        return result;
    }
    private static Map<String, FieldLineage> parseAggregate(Aggregate plan, Map<String, Map<String, FieldLineage>> cteCache) {
        Map<String, FieldLineage> childFields = parseLogicalPlan(plan.child(), cteCache);

        if (DEBUG_MODE) {
            System.out.println("===== DEBUG: parseAggregate - childFields =====");
            System.out.println("Number of childFields: " + childFields.size());
            for (Map.Entry<String, FieldLineage> entry : childFields.entrySet()) {
                System.out.println("  " + entry.getKey() + " -> " + entry.getValue().getSourceTableName());
            }
        }

        Map<String, FieldLineage> result = new HashMap<>();

        // 第一步：解析 Group By 字段，并收集它们的血缘信息供后续聚合字段使用
        List<FieldLineage> groupingFieldLineages = new ArrayList<>();
        Seq<Expression> groupingExprs = plan.groupingExpressions();
        if (DEBUG_MODE) {
            System.out.println("===== DEBUG: parseAggregate - GROUP BY expressions =====");
            System.out.println("Number of GROUP BY expressions: " + (groupingExprs != null ? groupingExprs.size() : 0));
        }
        if (groupingExprs != null && !groupingExprs.isEmpty()) {
            Iterator<Expression> it = groupingExprs.iterator();
            while (it.hasNext()) {
                Expression expr = it.next();
                if (DEBUG_MODE) {
                    System.out.println("  GROUP BY expression: " + expr.getClass().getSimpleName() + " - " + expr);
                }

                // 获取字段名：支持 AttributeReference 和 NamedExpression
                String fieldName;
                if (expr instanceof AttributeReference) {
                    fieldName = ((AttributeReference) expr).name();
                    if (DEBUG_MODE) {
                        System.out.println("    -> AttributeReference, name = " + fieldName);
                    }
                } else if (expr instanceof NamedExpression) {
                    fieldName = ((NamedExpression) expr).name();
                    if (DEBUG_MODE) {
                        System.out.println("    -> NamedExpression, name = " + fieldName);
                    }
                }
                else if (expr instanceof Cast) {
                    fieldName = handleCastExpressionInGroupBy((Cast) expr, childFields, cteCache);
                    if (fieldName == null) {
                        if (DEBUG_MODE) {
                            System.out.println("    -> Skipping (Cast expression handler returned null)");
                        }
                        continue;
                    }
                    if (DEBUG_MODE) {
                        System.out.println("    -> Cast expression, name = " + fieldName);
                    }
                }

                else {
                    // 其他类型（如表达式），跳过
                    if (DEBUG_MODE) {
                        System.out.println("    -> Skipping (not AttributeReference or NamedExpression)");
                    }
                    continue;
                }

                FieldLineage lineage = new FieldLineage(fieldName, "AGGREGATE");
                Set<String> depNames = extractDependencies(expr);
                if (DEBUG_MODE) {
                    System.out.println("    -> Extracted dependencies: " + depNames);
                }
                for (String depName : depNames) {
                    FieldLineage dep = findFieldInChildFields(depName, childFields);
                    if (dep != null) {
                        lineage.addDependency(cloneFieldLineage(dep));
                    } else {
                        System.out.println("    WARNING: Cannot find dependency '" + depName + "' in childFields for grouping field " + fieldName);
                    }
                }
                // 收集 Group By 字段的血缘，供聚合字段使用
                groupingFieldLineages.add(lineage);
                if (DEBUG_MODE) {
                    System.out.println("    -> Added to groupingFieldLineages (total: " + groupingFieldLineages.size() + ")");
                }
            }
        }
        if (DEBUG_MODE) {
            System.out.println("Total groupingFieldLineages collected: " + groupingFieldLineages.size());
        }

        // 第二步：解析聚合表达式（如 COUNT(a) AS b）
        // 关键修复：区分非聚合字段和聚合字段
        // - 非聚合字段（直接SELECT的Group By字段，如user_id）：只依赖它自己
        // - 聚合字段（如COUNT(order_id)、SUM(amount)）：依赖聚合函数内部字段 + 所有Group By字段
        Seq<NamedExpression> aggExprs = plan.aggregateExpressions();
        if (aggExprs != null && !aggExprs.isEmpty()) {
            Iterator<NamedExpression> it = aggExprs.iterator();
            while (it.hasNext()) {
                NamedExpression expr = it.next();
                if (expr == null) continue;

                String fieldName = expr.name();
                FieldLineage lineage = new FieldLineage(fieldName, "AGGREGATE");

                // 安全获取SQL表达式
                try {
                    lineage.setExpression(((Expression) expr).sql());
                } catch (Exception e) {
                    lineage.setExpression(expr.toString());
                }

                // 判断是否包含聚合函数
                boolean hasAggregateFunction = containsAggregateFunction((Expression) expr);

                if (hasAggregateFunction) {
                    // === 聚合字段（如 COUNT(order_id) AS order_count） ===
                    lineage.setFieldType(FieldLineage.FieldType.AGGREGATE);

                    // 1. 先添加聚合函数内部字段的依赖
                    Set<String> depNames = extractDependencies((Expression) expr);
                    if (DEBUG_MODE && !depNames.isEmpty()) {
                        System.out.println("    Aggregate field '" + fieldName + "' has " + depNames.size() + " expression dependencies: " + depNames);
                    }
                    for (String depName : depNames) {
                        FieldLineage dep = findFieldInChildFields(depName, childFields);
                        if (dep != null) {
                            lineage.addDependency(cloneFieldLineage(dep));
                            if (DEBUG_MODE) {
                                System.out.println("      Added expression dependency: " + depName + " (total: " + lineage.getDependencies().size() + ")");
                            }
                        } else {
                            System.out.println("    WARNING: Cannot find dependency '" + depName + "' in childFields for aggregate field " + fieldName);
                        }
                    }

                    // 2. 添加所有 Group By 字段作为依赖
                    // 因为聚合字段是在每个分组内计算的，依赖于分组键
                    if (!groupingFieldLineages.isEmpty()) {
                        if (DEBUG_MODE) {
                            System.out.println("    Adding " + groupingFieldLineages.size() + " grouping field dependencies to aggregate field '" + fieldName + "'");
                        }
                        for (FieldLineage groupingLineage : groupingFieldLineages) {
                            // 克隆 Group By 字段的血缘作为聚合字段的依赖
                            lineage.addDependency(cloneFieldLineage(groupingLineage));
                            if (DEBUG_MODE) {
                                System.out.println("      Added grouping dependency: " + groupingLineage.getFieldName() + " (total: " + lineage.getDependencies().size() + ")");
                            }
                        }
                    }

                    if (DEBUG_MODE) {
                        System.out.println("    Aggregate field '" + fieldName + "' final dependencies count: " + lineage.getDependencies().size());
                    }
                } else {
                    // === 非聚合字段（直接SELECT的Group By字段，如 user_id） ===
                    // 只添加它自己的依赖，不添加所有Group By字段
                    lineage.setFieldType(FieldLineage.FieldType.COLUMN);

                    // 只提取字段自身的依赖
                    Set<String> depNames = extractDependencies((Expression) expr);
                    if (DEBUG_MODE && !depNames.isEmpty()) {
                        System.out.println("    Non-aggregate field '" + fieldName + "' has " + depNames.size() + " dependencies: " + depNames);
                    }
                    for (String depName : depNames) {
                        FieldLineage dep = findFieldInChildFields(depName, childFields);
                        if (dep != null) {
                            lineage.addDependency(cloneFieldLineage(dep));
                            if (DEBUG_MODE) {
                                System.out.println("      Added dependency: " + depName + " (total: " + lineage.getDependencies().size() + ")");
                            }
                        } else {
                            System.out.println("    WARNING: Cannot find dependency '" + depName + "' in childFields for field " + fieldName);
                        }
                    }

                    if (DEBUG_MODE) {
                        System.out.println("    Non-aggregate field '" + fieldName + "' final dependencies count: " + lineage.getDependencies().size());
                    }
                }

                result.put(fieldName, lineage);
            }
        }

        return result;
    }

    private static Map<String, FieldLineage> parseWindow(Window plan, Map<String, Map<String, FieldLineage>> cteCache) {
        Map<String, FieldLineage> childFields = parseLogicalPlan(plan.child(), cteCache);
        Map<String, FieldLineage> result = new HashMap<>(childFields);

        // 添加窗口函数字段
        Seq<NamedExpression> windowExprs = plan.windowExpressions();
        if (windowExprs != null && !windowExprs.isEmpty()) {
            Iterator<NamedExpression> it = windowExprs.iterator();
            while (it.hasNext()) {
                NamedExpression expr = it.next();
                if (expr == null) continue;

                String fieldName = expr.name();
                FieldLineage lineage = new FieldLineage(fieldName, "WINDOW");
                lineage.setFieldType(FieldLineage.FieldType.WINDOW_FUNCTION);
                // 安全获取SQL表达式
                try {
                    lineage.setExpression(((Expression) expr).sql());
                } catch (Exception e) {
                    lineage.setExpression(expr.toString());
                }

                Set<String> depNames = extractDependencies((Expression) expr);
                for (String depName : depNames) {
                    FieldLineage dep = findFieldInChildFields(depName, childFields);
                    if (dep != null) {
                        lineage.addDependency(cloneFieldLineage(dep));
                    } else {
                        System.out.println("    WARNING: Cannot find dependency '" + depName + "' in childFields for window field " + fieldName);
                    }
                }

                result.put(fieldName, lineage);
            }
        }

        return result;
    }

    private static Map<String, FieldLineage> parseGenerate(Generate plan, Map<String, Map<String, FieldLineage>> cteCache) {
        Map<String, FieldLineage> childFields = parseLogicalPlan(plan.child(), cteCache);
        Map<String, FieldLineage> result = new HashMap<>(childFields);

        // 添加生成字段 - 使用 output() 方法获取所有输出字段
        Seq<Attribute> output = plan.output();
        if (output != null && !output.isEmpty()) {
            Iterator<Attribute> it = output.iterator();
            while (it.hasNext()) {
                Attribute attr = it.next();
                if (attr != null) {
                    String fieldName = attr.name();
                    // 只处理新增的生成字段（不在childFields中的）
                    if (!childFields.containsKey(fieldName)) {
                        FieldLineage lineage = new FieldLineage(fieldName, "GENERATE");
                        lineage.setFieldType(FieldLineage.FieldType.CALCULATED);
                        // Generator没有sql()方法，使用toString()代替
                        lineage.setExpression(plan.generator().toString());

                        // Generator提取依赖：通过Generator的输入字段
                        Set<String> depNames = new HashSet<>();
                        // 获取Generate节点的所有输入字段
                        Seq<Attribute> inputAttrs = plan.child().output();
                        if (inputAttrs != null && !inputAttrs.isEmpty()) {
                            Iterator<Attribute> inputIt = inputAttrs.iterator();
                            while (inputIt.hasNext()) {
                                Attribute inputAttr = inputIt.next();
                                if (inputAttr != null) {
                                    depNames.add(inputAttr.name());
                                }
                            }
                        }

                        for (String depName : depNames) {
                            FieldLineage dep = findFieldInChildFields(depName, childFields);
                            if (dep != null) {
                                lineage.addDependency(cloneFieldLineage(dep));
                            } else {
                                System.out.println("    WARNING: Cannot find dependency '" + depName + "' in childFields for generate field " + fieldName);
                            }
                        }

                        result.put(fieldName, lineage);
                    }
                }
            }
        }

        return result;
    }

    private static Map<String, FieldLineage> parseExpand(Expand plan, Map<String, Map<String, FieldLineage>> cteCache) {
        // Expand直接穿透
        return parseLogicalPlan(plan.child(), cteCache);
    }

    // ==================== 穿透算子 ====================

    private static Map<String, FieldLineage> parseFilter(Filter plan, Map<String, Map<String, FieldLineage>> cteCache) {
        return parseLogicalPlan(plan.child(), cteCache);
    }

    private static Map<String, FieldLineage> parseSort(Sort plan, Map<String, Map<String, FieldLineage>> cteCache) {
        return parseLogicalPlan(plan.child(), cteCache);
    }

    private static Map<String, FieldLineage> parseDistinct(Distinct plan, Map<String, Map<String, FieldLineage>> cteCache) {
        return parseLogicalPlan(plan.child(), cteCache);
    }

    private static Map<String, FieldLineage> parseLimit(LogicalPlan plan, Map<String, Map<String, FieldLineage>> cteCache) {
        return parseLogicalPlan(plan.children().head(), cteCache);
    }

    private static Map<String, FieldLineage> parseSample(Sample plan, Map<String, Map<String, FieldLineage>> cteCache) {
        return parseLogicalPlan(plan.child(), cteCache);
    }

    private static Map<String, FieldLineage> parseRepartition(LogicalPlan plan, Map<String, Map<String, FieldLineage>> cteCache) {
        return parseLogicalPlan(plan.children().head(), cteCache);
    }

    private static Map<String, FieldLineage> parseDeduplicate(Deduplicate plan, Map<String, Map<String, FieldLineage>> cteCache) {
        return parseLogicalPlan(plan.child(), cteCache);
    }

    private static Map<String, FieldLineage> parseTail(Tail plan, Map<String, Map<String, FieldLineage>> cteCache) {
        return parseLogicalPlan(plan.child(), cteCache);
    }

    private static Map<String, FieldLineage> parseOneRowRelation() {
        return new HashMap<>();
    }

    // ==================== Join ====================

    private static Map<String, FieldLineage> parseJoin(Join plan, Map<String, Map<String, FieldLineage>> cteCache) {
        Map<String, FieldLineage> leftFields = parseLogicalPlan(plan.left(), cteCache);
        Map<String, FieldLineage> rightFields = parseLogicalPlan(plan.right(), cteCache);

        Map<String, FieldLineage> result = new HashMap<>();

        // 处理左表字段
        for (Map.Entry<String, FieldLineage> entry : leftFields.entrySet()) {
            String fieldName = entry.getKey();
            FieldLineage lineage = entry.getValue();

            // 先直接 put 左表字段
            result.put(fieldName, lineage);
        }

        // 处理右表字段
        for (Map.Entry<String, FieldLineage> entry : rightFields.entrySet()) {
            String fieldName = entry.getKey();
            FieldLineage lineage = entry.getValue();

            // 检查是否有同名字段
            if (result.containsKey(fieldName)) {
                // 存在同名，需要处理冲突
                FieldLineage existing = result.remove(fieldName);

                // 获取两个字段的 tableName
                String leftTableName = existing.getTableName();
                String rightTableName = lineage.getTableName();

                // 如果 tableName 不为空，给左表字段加前缀
                if (leftTableName != null && !leftTableName.isEmpty()) {
                    String leftPrefixedKey = leftTableName + "." + fieldName;
                    result.put(leftPrefixedKey, existing);
                }

                // 如果 tableName 不为空，给右表字段加前缀
                if (rightTableName != null && !rightTableName.isEmpty()) {
                    String rightPrefixedKey = rightTableName + "." + fieldName;
                    result.put(rightPrefixedKey, lineage);
                }
            } else {
                // 没有同名，直接 put
                result.put(fieldName, lineage);
            }
        }

        return result;
    }


    // ==================== 集合操作 ====================

    private static Map<String, FieldLineage> parseUnion(Union plan, Map<String, Map<String, FieldLineage>> cteCache) {
        Map<String, FieldLineage> childFieldLineage = new HashMap<>();
        Seq<LogicalPlan> children = plan.children();
        if (children != null && !children.isEmpty()) {
            Iterator<LogicalPlan> it = children.iterator();
            while (it.hasNext()) {
                Map<String, FieldLineage> child = parseLogicalPlan(it.next(), cteCache);
                for (Map.Entry<String, FieldLineage> entry : child.entrySet()) {
                    if (childFieldLineage.containsKey(entry.getKey())) {
                        FieldLineage fieldLineage = childFieldLineage.get(entry.getKey());
                        fieldLineage.getDependencies().add(entry.getValue());
                    }
                    else  {
                        childFieldLineage.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        }
//        Map<String, FieldLineage> result = new HashMap<>();
//        Seq<Attribute> output = plan.output();
//        Iterator<Attribute> iterator = output.iterator();
//        while (iterator.hasNext()) {
//            Attribute attribute = iterator.next();
//            FieldLineage fieldLineage = new FieldLineage();
//            fieldLineage.setFieldName(attribute.name());
//            fieldLineage.setFieldType(FieldLineage.FieldType.COLUMN);
//            for (Map.Entry<String, FieldLineage> entry : childFieldLineage.entrySet()) {
//                String[] split = entry.getKey().split("\\.");
//                if (split[split.length - 1].equals(attribute.name())) {
//                    fieldLineage.getDependencies().add(entry.getValue());
//                }
//            }
//            result.put(attribute.name(), fieldLineage);
//        }
//        return result;
          return childFieldLineage;
    }

    private static Map<String, FieldLineage> parseIntersect(Intersect plan, Map<String, Map<String, FieldLineage>> cteCache) {
        return parseLogicalPlan(plan.left(), cteCache);
    }

    private static Map<String, FieldLineage> parseExcept(Except plan, Map<String, Map<String, FieldLineage>> cteCache) {
        return parseLogicalPlan(plan.left(), cteCache);
    }

    // ==================== 其他 ====================

    private static Map<String, FieldLineage> parseView(View plan, Map<String, Map<String, FieldLineage>> cteCache) {
        return parseLogicalPlan(plan.child(), cteCache);
    }

    private static Map<String, FieldLineage> parseUnknown(LogicalPlan plan, Map<String, Map<String, FieldLineage>> cteCache) {
        Seq<LogicalPlan> children = plan.children();
        if (children != null && !children.isEmpty()) {
            return parseLogicalPlan(children.head(), cteCache);
        }
        return new HashMap<>();
    }

    /**
     * 解析 LoadData
     * LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename
     */
    private static Map<String, FieldLineage> parseLoadData(LoadDataCommand plan) {
        // LOAD DATA 操作只是将文件数据加载到表中
        // 由于无法从外部文件推断字段级血缘关系，返回空Map
        // 如需追踪数据加载来源，可在此记录表名与文件路径的映射关系
        return new HashMap<>();
    }

    private static Map<String, FieldLineage> parseInsertIntoHadoopFsRelationCommand(InsertIntoHadoopFsRelationCommand plan, Map<String, Map<String, FieldLineage>> cteCache) {
        // 获取写入逻辑计划，解析其血缘
        LogicalPlan query = plan.query();
        Map<String, FieldLineage> result = parseLogicalPlan(query, cteCache);

        // 使用输出路径作为表名标识
        Option<CatalogTable> catalogTableOption = plan.catalogTable();
        String fullName="";
        if (catalogTableOption.isDefined()) {
            fullName=catalogTableOption.get().qualifiedName();
        }
        // 更新所有字段的表名为目标表
        Map<String, FieldLineage> finalResult = new HashMap<>();
        for (Map.Entry<String, FieldLineage> entry : result.entrySet()) {
            FieldLineage lineage = cloneFieldLineage(entry.getValue());
            lineage.setTableName(fullName);
            finalResult.put(fullName+"."+entry.getKey(), lineage);
        }
        return finalResult;
    }


    /**
     * 解析 CreateHiveTableAsSelectCommand
     * CREATE TABLE hive_table AS SELECT ...
     */
    private static Map<String, FieldLineage> parseCreateHiveTableAsSelectCommand(CreateHiveTableAsSelectCommand plan, Map<String, Map<String, FieldLineage>> cteCache) {
        System.out.println("===== DEBUG: CreateHiveTableAsSelectCommand =====");

        // 获取目标表信息
        String tableName = plan.tableDesc().identifier().table();
        String dbName = plan.tableDesc().identifier().database().getOrElse(()->"default");
        String fullName = dbName + "." + tableName;
        // 解析查询部分的血缘
        Map<String, FieldLineage> childFields = parseLogicalPlan(plan.query(), cteCache);

        // 更新表名为目标表
        Map<String, FieldLineage> result = new HashMap<>();
        for (Map.Entry<String, FieldLineage> entry : childFields.entrySet()) {
            String[] split = entry.getKey().split("\\.");
            String fieldName = split[split.length-1];
            FieldLineage dependency = cloneFieldLineage(entry.getValue());
            FieldLineage lineage =new FieldLineage();
            lineage.setTableName(fullName);
            lineage.setFieldName(fullName+"."+fieldName);
            lineage.setDependencies(Collections.singletonList(dependency));
            lineage.setTableName(tableName);
            result.put(fullName+"."+fieldName, lineage);
        }

        return result;
    }

    /**
     * 解析 InsertIntoHiveTable
     * INSERT INTO TABLE hive_table SELECT ...
     * INSERT OVERWRITE TABLE hive_table SELECT ...
     */
    private static Map<String, FieldLineage> parseInsertIntoHiveTable(InsertIntoHiveTable plan, Map<String, Map<String, FieldLineage>> cteCache) {
        System.out.println("===== DEBUG: InsertIntoHiveTable =====");

        // 获取目标表信息
        String tableName = plan.table().identifier().table();
        String dbName = plan.table().identifier().database().getOrElse(()->"default");
        String fullName = dbName + "." + tableName;
        boolean overwrite = plan.overwrite();

        // 解析查询部分的血缘
        Map<String, FieldLineage> childFields = parseLogicalPlan(plan.query(), cteCache);

        // 更新表名为目标表
        Map<String, FieldLineage> result = new HashMap<>();
        for (Map.Entry<String, FieldLineage> entry : childFields.entrySet()) {
            FieldLineage lineage = cloneFieldLineage(entry.getValue());
            result.put(fullName+"."+entry.getKey(), lineage);
        }

        return result;
    }

    /**
     * 解析 SaveIntoDataSourceCommand
     * df.save() / df.write.save() 产生的命令
     */
    private static Map<String, FieldLineage> parseSaveIntoDataSourceCommand(SaveIntoDataSourceCommand plan, Map<String, Map<String, FieldLineage>> cteCache) {
        System.out.println("===== DEBUG: SaveIntoDataSourceCommand =====");
        String tableName = plan.toString();
        System.out.println("DataSource operation: " + tableName);

        // 解析查询部分的血缘
        Map<String, FieldLineage> childFields = parseLogicalPlan(plan.query(), cteCache);

        // 更新表名
        Map<String, FieldLineage> result = new HashMap<>();
        for (Map.Entry<String, FieldLineage> entry : childFields.entrySet()) {
            FieldLineage lineage = cloneFieldLineage(entry.getValue());
            lineage.setTableName(tableName);
            result.put(entry.getKey(), lineage);
        }

        return result;
    }

    /**
     * 解析 CreateDataSourceTableCommand
     * CREATE TABLE table_name (schema) USING format
     */
    private static Map<String, FieldLineage> parseCreateDataSourceTableCommand(CreateDataSourceTableCommand plan, Map<String, Map<String, FieldLineage>> cteCache) {
        System.out.println("===== DEBUG: CreateDataSourceTableCommand =====");
        String tableName = plan.table().toString();
        System.out.println("Created table: " + tableName);
        // 创建空表，无血缘数据
        return new HashMap<>();
    }

    /**
     * 解析 CreateTableCommand
     * CREATE TABLE table_name (schema)
     */
    private static Map<String, FieldLineage> parseCreateTableCommand(CreateTableCommand plan, Map<String, Map<String, FieldLineage>> cteCache) {
        String tableName = plan.toString();
        System.out.println("Created table: " + tableName);
        // 创建空表，无血缘数据
        return new HashMap<>();
    }

    /**
     * 解析 DropTableCommand
     * DROP TABLE table_name
     */
    private static Map<String, FieldLineage> parseDropTableCommand(DropTableCommand plan) {
        String tableName = plan.tableName().toString();
        System.out.println("Dropped table: " + tableName);
        // 删除表操作，无血缘数据
        return new HashMap<>();
    }

    /**
     * 解析 AlterTableAddPartitionCommand
     * ALTER TABLE table_name ADD PARTITION (partition_spec)
     */
    private static Map<String, FieldLineage> parseAlterTableAddPartitionCommand(AlterTableAddPartitionCommand plan) {
        String tableName = plan.toString();
        System.out.println("Added partition to table: " + tableName);
        // 添加分区操作，无血缘数据
        return new HashMap<>();
    }

    /**
     * 解析 AlterTableDropPartitionCommand
     * ALTER TABLE table_name DROP PARTITION (partition_spec)
     */
    private static Map<String, FieldLineage> parseAlterTableDropPartitionCommand(AlterTableDropPartitionCommand plan) {
        String tableName = plan.toString();
        System.out.println("Dropped partition from table: " + tableName);
        // 删除分区操作，无血缘数据
        return new HashMap<>();
    }

    /**
     * 解析 CacheTableCommand
     * CACHE TABLE table_name SELECT ...
     * 注意：此命令在Spark 3.3.4中可能不存在
     */
    // private static Map<String, FieldLineage> parseCacheTableCommand(CacheTableCommand plan) {
    //     System.out.println("===== DEBUG: CacheTableCommand =====");
    //     return new HashMap<>();
    // }

    /**
     * 解析 UncacheTableCommand
     * UNCACHE TABLE table_name
     * 注意：此命令在Spark 3.3.4中可能不存在
     */
    // private static Map<String, FieldLineage> parseUncacheTableCommand(UncacheTableCommand plan) {
    //     System.out.println("===== DEBUG: UncacheTableCommand =====");
    //     return new HashMap<>();
    // }

    /**
     * 解析 RefreshTableCommand
     * REFRESH TABLE table_name
     */
    private static Map<String, FieldLineage> parseRefreshTableCommand(RefreshTableCommand plan) {
        String tableName = plan.toString();
        System.out.println("Refreshed table: " + tableName);
        // 刷新表操作，无血缘数据
        return new HashMap<>();
    }

    /**
     * 解析 ShowTablesCommand
     * SHOW TABLES
     */
    private static Map<String, FieldLineage> parseShowTablesCommand() {
        System.out.println("Showed all tables");
        // 查询操作，无血缘数据
        return new HashMap<>();
    }

    /**
     * 解析 ShowColumnsCommand
     * SHOW COLUMNS FROM table_name
     */
    private static Map<String, FieldLineage> parseShowColumnsCommand(ShowColumnsCommand plan) {
        String tableName = plan.toString();
        System.out.println("Showed columns from table: " + tableName);
        // 查询操作，无血缘数据
        return new HashMap<>();
    }


    /**
     * 处理 GROUP BY 中的 CAST 表达式
     * 例如：GROUP BY CAST(user_id AS STRING)
     *
     * @param castExpr Cast 表达式
     * @param childFields 子节点的字段血缘
     * @param cteCache CTE 缓存
     * @return Cast 后的字段名，如果无法处理则返回 null
     */
    private static String handleCastExpressionInGroupBy(Cast castExpr, Map<String, FieldLineage> childFields, Map<String, Map<String, FieldLineage>> cteCache) {
        if (castExpr == null) {
            return null;
        }

        // 获取 Cast 内部的子表达式
        Expression child = castExpr.child();
        if (child == null) {
            return null;
        }

        // 获取数据类型（用于生成字段名）
        String dataType;
        try {
            dataType = castExpr.dataType().sql();
        } catch (Exception e) {
            dataType = castExpr.dataType().toString();
        }

        if (DEBUG_MODE) {
            System.out.println("      >>> Cast Expression Debug <<<");
            System.out.println("      Child type: " + child.getClass().getSimpleName());
            System.out.println("      Child expression: " + child);
            System.out.println("      Target data type: " + dataType);
        }

        // 根据子表达式类型生成字段名
        String baseFieldName;

        // 1. 如果子表达式是 AttributeReference（直接字段引用）
        if (child instanceof AttributeReference) {
            AttributeReference attrRef = (AttributeReference) child;
            baseFieldName = attrRef.name();

            if (DEBUG_MODE) {
                System.out.println("      AttributeReference name: " + baseFieldName);
            }

            // 生成 Cast 后的字段名：格式为 "cast_{字段名}_as_{类型}"
            return "cast_" + baseFieldName + "_as_" + dataType.toLowerCase().replaceAll("[^a-z0-9]", "_");
        }

        // 2. 如果子表达式是 UnresolvedAttribute（未解析的字段引用）
        else if (child instanceof UnresolvedAttribute) {
            UnresolvedAttribute unresolvedAttr = (UnresolvedAttribute) child;
            baseFieldName = unresolvedAttr.name();

            if (DEBUG_MODE) {
                System.out.println("      UnresolvedAttribute name: " + baseFieldName);
            }

            // 生成 Cast 后的字段名
            return "cast_" + baseFieldName.replaceAll("[^a-zA-Z0-9_]", "_") + "_as_" + dataType.toLowerCase().replaceAll("[^a-z0-9]", "_");
        }

        // 3. 如果子表达式是 Alias（带别名的表达式）
        else if (child instanceof Alias) {
            Alias alias = (Alias) child;
            baseFieldName = alias.name();

            if (DEBUG_MODE) {
                System.out.println("      Alias name: " + baseFieldName);
                System.out.println("      Alias child: " + alias.child());
            }

            // 生成 Cast 后的字段名
            return "cast_" + baseFieldName + "_as_" + dataType.toLowerCase().replaceAll("[^a-z0-9]", "_");
        }

        // 4. 如果子表达式是另一个 Cast（嵌套 Cast）
        else if (child instanceof Cast) {
            String innerCastName = handleCastExpressionInGroupBy((Cast) child, childFields, cteCache);
            if (innerCastName != null) {
                return innerCastName + "_as_" + dataType.toLowerCase().replaceAll("[^a-z0-9]", "_");
            }
        }

        // 5. 对于其他类型的表达式（函数调用、算术表达式等）
        else {
            // 尝试提取依赖字段并生成一个描述性的字段名
            Set<String> depNames = extractDependencies(child);

            if (DEBUG_MODE) {
                System.out.println("      Complex expression, dependencies: " + depNames);
            }

            if (!depNames.isEmpty()) {
                // 使用依赖字段生成字段名
                String depsStr = String.join("_", depNames).replaceAll("[^a-zA-Z0-9_]", "_");
                return "cast_expr_" + depsStr + "_as_" + dataType.toLowerCase().replaceAll("[^a-z0-9]", "_");
            } else {
                // 使用表达式 SQL 生成字段名
                String exprSql;
                try {
                    exprSql = child.sql();
                } catch (Exception e) {
                    exprSql = child.toString();
                }
                // 清理 SQL 字符串，移除不安全字符
                String cleanSql = exprSql.replaceAll("[^a-zA-Z0-9_]", "_").replaceAll("_+", "_");
                // 限制长度避免字段名过长
                if (cleanSql.length() > 30) {
                    cleanSql = cleanSql.substring(0, 30);
                }
                return "cast_" + cleanSql + "_as_" + dataType.toLowerCase().replaceAll("[^a-z0-9]", "_");
            }
        }

        // 无法处理的情况
        if (DEBUG_MODE) {
            System.out.println("      WARNING: Unable to generate field name for Cast expression");
        }
        return null;
    }



    /**
     * 解析 ShowPartitionsCommand
     * SHOW PARTITIONS table_name
     */
    private static Map<String, FieldLineage> parseShowPartitionsCommand(ShowPartitionsCommand plan) {
        String tableName = plan.toString();
        System.out.println("Showed partitions from table: " + tableName);
        // 查询操作，无血缘数据
        return new HashMap<>();
    }

    /**
     * 解析 DescribeTableCommand
     * DESCRIBE TABLE table_name
     */
    private static Map<String, FieldLineage> parseDescribeTableCommand(DescribeTableCommand plan) {
        String tableName = plan.toString();
        System.out.println("Described table: " + tableName);
        // 查询操作，无血缘数据
        return new HashMap<>();
    }

    /**
     * 解析 AnalyzeTableCommand
     * ANALYZE TABLE table_name COMPUTE STATISTICS
     */
    private static Map<String, FieldLineage> parseAnalyzeTableCommand(AnalyzeTableCommand plan) {
        String tableName = plan.toString();
        System.out.println("Analyzed table: " + tableName);
        // 分析操作，无血缘数据
        return new HashMap<>();
    }

    // ==================== V2 Write操作 ====================

    /**
     * 解析 AppendData
     * DataFrameWriterV2 append 操作
     */
    private static Map<String, FieldLineage> parseAppendData(AppendData plan, Map<String, Map<String, FieldLineage>> cteCache) {
        System.out.println("===== DEBUG: AppendData =====");
        String tableName = plan.table().name();
        System.out.println("Append to table: " + tableName);

        // 解析查询部分的血缘
        Map<String, FieldLineage> childFields = parseLogicalPlan(plan.query(), cteCache);

        // 更新表名为目标表
        Map<String, FieldLineage> result = new HashMap<>();
        for (Map.Entry<String, FieldLineage> entry : childFields.entrySet()) {
            FieldLineage lineage = cloneFieldLineage(entry.getValue());
            lineage.setTableName(tableName);
            result.put(entry.getKey(), lineage);
        }

        return result;
    }

    /**
     * 解析 OverwriteByExpression
     * DataFrameWriterV2 overwrite(where) 操作
     */
    private static Map<String, FieldLineage> parseOverwriteByExpression(OverwriteByExpression plan, Map<String, Map<String, FieldLineage>> cteCache) {
        System.out.println("===== DEBUG: OverwriteByExpression =====");
        String tableName = plan.table().name();
        System.out.println("Overwrite table: " + tableName);

        // 解析查询部分的血缘
        Map<String, FieldLineage> childFields = parseLogicalPlan(plan.query(), cteCache);

        // 更新表名为目标表
        Map<String, FieldLineage> result = new HashMap<>();
        for (Map.Entry<String, FieldLineage> entry : childFields.entrySet()) {
            FieldLineage lineage = cloneFieldLineage(entry.getValue());
            lineage.setTableName(tableName);
            result.put(entry.getKey(), lineage);
        }

        return result;
    }

    /**
     * 解析 OverwritePartitionsDynamic
     * 动态分区覆盖操作
     */
    private static Map<String, FieldLineage> parseOverwritePartitionsDynamic(OverwritePartitionsDynamic plan, Map<String, Map<String, FieldLineage>> cteCache) {
        System.out.println("===== DEBUG: OverwritePartitionsDynamic =====");
        String tableName = plan.table().name();
        System.out.println("Dynamic overwrite table: " + tableName);

        // 解析查询部分的血缘
        Map<String, FieldLineage> childFields = parseLogicalPlan(plan.query(), cteCache);

        // 更新表名为目标表
        Map<String, FieldLineage> result = new HashMap<>();
        for (Map.Entry<String, FieldLineage> entry : childFields.entrySet()) {
            FieldLineage lineage = cloneFieldLineage(entry.getValue());
            lineage.setTableName(tableName);
            result.put(entry.getKey(), lineage);
        }

        return result;
    }

    // ==================== 辅助方法 ====================

    /**
     * 判断表达式是否包含聚合函数
     */
    private static boolean containsAggregateFunction(Expression expr) {
        return containsAggregateFunction(expr, 0);
    }

    // 常见的SQL聚合函数名集合
    private static final Set<String> AGGREGATE_FUNCTION_NAMES = new HashSet<>(Arrays.asList(
            "count", "sum", "avg", "min", "max", "stddev", "variance",
            "count_distinct", "approx_count_distinct",
            "first", "last", "collect_list", "collect_set",
            "array_agg", "string_agg", "bit_and", "bit_or", "bit_xor"
    ));

    private static boolean containsAggregateFunction(Expression expr, int depth) {
        if (expr == null) {
            return false;
        }

        if (DEBUG_MODE && depth == 0) {
            System.out.println("  Checking if expression contains aggregate: " + expr.getClass().getSimpleName());
        }

        // 1. 检查是否是已解析的聚合函数
        if (expr instanceof AggregateFunction) {
            if (DEBUG_MODE && depth == 0) {
                System.out.println("    -> IS an AggregateFunction: " + expr.getClass().getSimpleName());
            }
            return true;
        }

        // 2. 检查是否是未解析的函数，通过函数名判断是否为聚合函数
        if (expr instanceof UnresolvedFunction) {
            // UnresolvedFunction 使用 toString() 来获取函数名
            // 输出格式类似: 'count('o.order_id) 或 'sum('oi.goods_price * 'oi.goods_num)
            String exprString = expr.toString();
            // 提取函数名：去掉前面的单引号，然后取第一个单词
            String functionName = exprString.replaceAll("^'+", "").toLowerCase();
            int parenIndex = functionName.indexOf('(');
            if (parenIndex > 0) {
                functionName = functionName.substring(0, parenIndex);
            }

            // 处理 distinct 修饰符，如 count_distinct
            if (functionName.contains("distinct")) {
                functionName = "count_distinct";
            }

            boolean isAggregate = AGGREGATE_FUNCTION_NAMES.contains(functionName);
            if (DEBUG_MODE && depth == 0) {
                System.out.println("    -> UnresolvedFunction: " + exprString + ", extracted name: " + functionName + ", isAggregate: " + isAggregate);
            }
            if (isAggregate) {
                return true;
            }
        }

        // 3. 递归检查子表达式
        Seq<Expression> children = expr.children();
        if (children != null && !children.isEmpty()) {
            Iterator<Expression> it = children.iterator();
            while (it.hasNext()) {
                Expression child = it.next();
                if (DEBUG_MODE && depth == 0) {
                    System.out.println("    -> Checking child: " + child.getClass().getSimpleName());
                }
                if (containsAggregateFunction(child, depth + 1)) {
                    return true;
                }
            }
        }

        if (DEBUG_MODE && depth == 0) {
            System.out.println("    -> NOT an aggregate expression");
        }
        return false;
    }

    /**
     * 从表达式中提取依赖的字段名
     */
    private static Set<String> extractDependencies(Expression expr) {
        Set<String> deps = new HashSet<>();
        if (expr == null) {
            return deps;
        }

        // 处理已解析的属性引用
        if (expr instanceof AttributeReference) {
            Seq<String> qualifier = ((AttributeReference) expr).qualifier();
            if (qualifier != null&& !qualifier.isEmpty()) {
                deps.add(qualifier.head()+"."+((AttributeReference) expr).name());
            }
            else {
                deps.add(((AttributeReference) expr).name());
            }
        }
        // 处理未解析的属性引用（带前缀的字段，如 rnk.uid）
        else if (expr instanceof UnresolvedAttribute) {
            String name = ((UnresolvedAttribute) expr).name();
            if (name != null) {
                deps.add(name);
            }
        }
        // 处理 CaseWhen 表达式（需要特殊处理分支结构）
        else if (isCaseWhenExpression(expr)) {
            deps.addAll(extractCaseWhenDependencies((CaseWhen) expr));
        }
        // 递归处理子表达式
        else {
            Seq<Expression> children = expr.children();
            if (children != null && !children.isEmpty()) {
                Iterator<Expression> it = children.iterator();
                while (it.hasNext()) {
                    deps.addAll(extractDependencies(it.next()));
                }
            }
        }

        return deps;
    }

    /**
     * 专门处理 CaseWhen 表达式的依赖提取
     * 例如：CASE WHEN SUM(oi.goods_price * oi.goods_num) >= 2000 THEN 'VIP' ... END AS user_level
     *
     * @param caseExpr CaseWhen 表达式
     * @return CaseWhen 表达式中依赖的所有字段名（带表别名）
     */
    private static Set<String> extractCaseWhenDependencies(CaseWhen caseExpr) {
        Set<String> deps = new HashSet<>();
        if (caseExpr == null) {
            return deps;
        }

        if (DEBUG_MODE) {
            System.out.println("  >>> Extracting CaseWhen dependencies <<<");
            System.out.println("  CaseWhen branches count: " + caseExpr.branches().size());
        }

        // 获取所有分支：每个分支是 Tuple2<predicate, value>
        scala.collection.Seq<scala.Tuple2<org.apache.spark.sql.catalyst.expressions.Expression, org.apache.spark.sql.catalyst.expressions.Expression>> branches = caseExpr.branches();
        scala.collection.Iterator<scala.Tuple2<org.apache.spark.sql.catalyst.expressions.Expression, org.apache.spark.sql.catalyst.expressions.Expression>> branchesIter = branches.iterator();

        int branchIndex = 0;
        while (branchesIter.hasNext()) {
            scala.Tuple2<org.apache.spark.sql.catalyst.expressions.Expression, org.apache.spark.sql.catalyst.expressions.Expression> branch = branchesIter.next();
            branchIndex++;

            // 第一个元素是 predicate（WHEN 条件）
            org.apache.spark.sql.catalyst.expressions.Expression predicate = branch._1();
            if (predicate != null) {
                if (DEBUG_MODE) {
                    System.out.println("    Branch " + branchIndex + " predicate: " + predicate.getClass().getSimpleName() + " - " + predicate);
                }
                // 递归提取 predicate 中的依赖
                deps.addAll(extractDependencies(predicate));
            }

            // 第二个元素是 value（THEN 结果）
            org.apache.spark.sql.catalyst.expressions.Expression value = branch._2();
            if (value != null) {
                if (DEBUG_MODE) {
                    System.out.println("    Branch " + branchIndex + " value: " + value.getClass().getSimpleName() + " - " + value);
                }
                // 如果 value 不是 Literal（常量），提取其依赖
                if (!(value instanceof Literal)) {
                    deps.addAll(extractDependencies(value));
                }
            }
        }

        // 处理 ELSE 分支
        scala.Option<org.apache.spark.sql.catalyst.expressions.Expression> elseValueOpt = caseExpr.elseValue();
        if (elseValueOpt.isDefined()) {
            org.apache.spark.sql.catalyst.expressions.Expression elseValue = elseValueOpt.get();
            if (DEBUG_MODE) {
                System.out.println("    ELSE value: " + elseValue.getClass().getSimpleName() + " - " + elseValue);
            }
            // 如果 ELSE 值不是 Literal（常量），提取其依赖
            if (!(elseValue instanceof Literal)) {
                deps.addAll(extractDependencies(elseValue));
            }
        } else {
            if (DEBUG_MODE) {
                System.out.println("    ELSE value: None (no ELSE clause)");
            }
        }

        if (DEBUG_MODE) {
            System.out.println("  <<< CaseWhen dependencies extracted: " + deps + " >>>");
        }

        return deps;
    }

    /**
     * 判断字段类型是否为 CaseWhen 表达式
     *
     * @param expr 表达式
     * @return 如果是 CaseWhen 表达式返回 true，否则返回 false
     */
    private static boolean isCaseWhenExpression(Expression expr) {
        return expr instanceof CaseWhen;
    }

    //todo 重大问题
    private static FieldLineage findFieldInChildFields(String fieldName, Map<String, FieldLineage> childFields) {
        if (fieldName == null || childFields == null) {
            return null;
        }
        String[] split = fieldName.split("\\.");
        if (split.length==1) {
            String shortName=split[0];
            FieldLineage fieldLineage = childFields.get(shortName);
            if (fieldLineage != null) {
                return fieldLineage;
            }
            else {
                for (Map.Entry<String, FieldLineage> child : childFields.entrySet()) {
                    String[] children = child.getKey().split("\\.");
                    if (children.length>1) {
                        if (StringUtils.equals(children[children.length-1], shortName)) {
                            return child.getValue();
                        }
                    }
                }
            }
        }
        else  {
            String shortName=split[split.length-2]+"."+split[split.length-1];
            FieldLineage fieldLineage = childFields.get(shortName);
            if (fieldLineage != null) {
                return fieldLineage;
            }
            else {
                for (Map.Entry<String, FieldLineage> child : childFields.entrySet()) {
                    String[] children = child.getKey().split("\\.");
                    if (children.length==1) {
                        if (StringUtils.equals(children[0], split[split.length-1])) {
                            return child.getValue();
                        }
                    }
                    else {
                        String childShortName = children[children.length-2]+"."+children[children.length-1];
                        FieldLineage value = child.getValue();
                        String tableName = StringUtils.isBlank(value.getTableName())?"":value.getTableName();
                        String aliasName = tableName+"."+children[split.length-1];
                        if (StringUtils.equals(childShortName, shortName)||StringUtils.equals(shortName, aliasName)) {
                            return child.getValue();
                        }
                    }

                }
            }
        }
        return null;
    }

    private static String handleProjectExprName(String name){
        if (StringUtils.isBlank(name)) {
            return name;
        }
        String[] split = name.split("\\.");
        if (split.length==1){
            return split[0];
        }
        else {
            return split[split.length-2]+"."+split[split.length-1];
        }

    }

    /**
     * 判断表达式类型
     */
    private static FieldLineage.FieldType determineExpressionType(Expression expr) {
        if (expr instanceof AggregateFunction) {
            return FieldLineage.FieldType.AGGREGATE;
        } else if (expr instanceof WindowExpression) {
            return FieldLineage.FieldType.WINDOW_FUNCTION;
        } else if (expr instanceof Alias) {
            return FieldLineage.FieldType.ALIAS;
        } else if (expr instanceof Literal) {
            return FieldLineage.FieldType.CONSTANT;
        } else {
            return FieldLineage.FieldType.CALCULATED;
        }
    }

    /**
     * 深度克隆FieldLineage
     */
    private static FieldLineage cloneFieldLineage(FieldLineage original) {
        if (original == null) {
            return null;
        }

        FieldLineage cloned = new FieldLineage();
        cloned.setFieldName(original.getFieldName());
        cloned.setTableName(original.getTableName());
        cloned.setFieldType(original.getFieldType());
        cloned.setExpression(original.getExpression());
        cloned.setSourceTableName(original.getSourceTableName());


        for (FieldLineage dep : original.getDependencies()) {
            cloned.addDependency(cloneFieldLineage(dep));
        }

        return cloned;
    }

    /**
     * 深度克隆Map
     */
    private static Map<String, FieldLineage> deepCopyFieldLineageMap(Map<String, FieldLineage> original) {
        Map<String, FieldLineage> copy = new HashMap<>();
        for (Map.Entry<String, FieldLineage> entry : original.entrySet()) {
            copy.put(entry.getKey(), cloneFieldLineage(entry.getValue()));
        }
        return copy;
    }

    /**
     * 获取表名
     */
    private static String getTableName(Map<String, FieldLineage> fields) {
        if (fields.isEmpty()) {
            return "unknown";
        }
        return fields.values().iterator().next().getTableName();
    }

    // ==================== 格式化输出 ====================

    /**
     * 打印血缘结果
     */
    public static void printLineage(Map<String, FieldLineage> lineage) {
        if (lineage == null || lineage.isEmpty()) {
            System.out.println("无血缘信息");
            return;
        }

        System.out.println("========== 字段级血缘解析结果 ==========");
        for (Map.Entry<String, FieldLineage> entry : lineage.entrySet()) {
            System.out.println("\n字段: " + entry.getKey());
            System.out.println(entry.getValue().getLineageDescription());
        }
        System.out.println("========================================");
    }

    /**
     * 获取血缘文本描述
     */
    public static String getLineageDescription(Map<String, FieldLineage> lineage) {
        if (lineage == null || lineage.isEmpty()) {
            return "无血缘信息";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("========== 字段级血缘解析结果 ==========\n");

        for (Map.Entry<String, FieldLineage> entry : lineage.entrySet()) {
            sb.append("\n字段: ").append(entry.getKey()).append("\n");
            sb.append(entry.getValue().getLineageDescription());
        }

        sb.append("========================================\n");
        return sb.toString();
    }

    /**
     * 获取所有字段及其源头字段
     */
    public static Map<String, Set<String>> getAllSourceFields(Map<String, FieldLineage> lineage) {
        Map<String, Set<String>> result = new HashMap<>();

        for (Map.Entry<String, FieldLineage> entry : lineage.entrySet()) {
            String fieldName = entry.getKey();
            Set<String> sources = entry.getValue().getAllSourceFields();
            if (!sources.isEmpty()) {
                result.put(fieldName, sources);
            }
        }

        return result;
    }

    public static void main(String[] args) {
//        String sql = "WITH grade_s_products AS (SELECT qc_code, brand_id, brand_name, model_id, model_name, dt AS product_dt, weekofyear(to_date(dt, 'yyyy-MM-dd')) AS product_week, concat_ws('-', brand_name, model_name) AS full_product_name, CASE WHEN buying_price > 5000 THEN 'high_end' ELSE 'mid_low' END AS price_grade FROM hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_bmskyway_t_skyway_product_full_1d WHERE dt >= date_format(date_sub(current_date(), 180), 'yyyy-MM-dd') AND dt <= date_format(current_date(), 'yyyy-MM-dd') AND grade = 'S' AND brand_name NOT IN ('测试品牌', '未知品牌') AND isnotnull(qc_code)), sales_orders AS (SELECT info_id, qc_code, total_amt / 100 AS real_pay_price, to_date(pay_time, 'yyyy-MM-dd HH:mm:ss') AS pay_date, weekofyear(to_date(pay_time, 'yyyy-MM-dd HH:mm:ss')) AS pay_week, row_number() OVER (PARTITION BY qc_code ORDER BY pay_time) AS sales_seq, IF(total_amt >= 10000, 'big_order', 'normal_order') AS order_level FROM hdp_ubu_zhuanzhuan_dw_b2c.dw_trade_order_ord_all_subject_dtl_full_1d WHERE dt = date_format(current_date(), 'yyyy-MM-dd') AND cate_first_id = 101 AND company_flag = 1 AND isnotnull(pay_time)), product_details AS (SELECT info_id, qc_code, spec_ram, spec_version, spec_appearance_quality, spec_function_quality, hand_price, dt AS detail_dt FROM hdp_zhuanzhuan_dw_global.dw_info_prod_detail_full_1d WHERE dt BETWEEN date_format(date_sub(current_date(), 180), 'yyyy-MM-dd') AND date_format(current_date(), 'yyyy-MM-dd') AND status = 1 AND sale_where = 1 AND spec_machine_source != 'BS机' AND is_searchable = 1), product_sales_join AS (SELECT gp.brand_id, gp.brand_name, gp.model_id, gp.model_name, gp.full_product_name, gp.price_grade, so.real_pay_price, so.pay_week, so.order_level, pd.spec_ram, pd.spec_version, pd.spec_appearance_quality, pd.hand_price AS product_list_price, ROUND((so.real_pay_price / pd.hand_price) * 100, 2) AS discount_rate FROM grade_s_products gp INNER JOIN sales_orders so ON gp.qc_code = so.qc_code LEFT JOIN product_details pd ON so.info_id = pd.info_id AND gp.qc_code = pd.qc_code WHERE so.real_pay_price > 0 AND pd.hand_price > 0) SELECT brand_name, model_name, pay_week, price_grade, spec_ram, spec_version, COUNT(DISTINCT so.info_id) AS order_count, SUM(real_pay_price) AS total_sales_amount, AVG(real_pay_price) AS avg_sales_price, MAX(real_pay_price) AS max_sales_price, MIN(real_pay_price) AS min_sales_price, AVG(discount_rate) AS avg_discount_rate, COUNT(CASE WHEN order_level = 'big_order' THEN 1 END) AS big_order_count, concat_ws('/', spec_ram, spec_version) AS product_config FROM product_sales_join GROUP BY brand_name, model_name, pay_week, price_grade, spec_ram, spec_version HAVING order_count >= 5 ORDER BY pay_week DESC, total_sales_amount DESC LIMIT 100;";
//        String sql="WITH valid_orders AS (SELECT od.order_id,od.order_detail_id,od.user_id,u.user_name,u.member_grade,u.user_source,od.product_id,od.sku_id,od.order_num,od.unit_price,od.pay_amt,od.discount_amt,date_format (to_date (od.pay_time, 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') AS pay_date,hour (od.pay_time) AS pay_hour FROM dw_fact.fact_order_detail od INNER JOIN dw_dim.dim_user u ON od.user_id = u.user_id AND od.dt = u.dt WHERE od.dt = '2026-01-30' AND od.order_status IN (2,3,4) AND od.pay_amt > 0),order_product_relation AS (SELECT vo.*,p.product_name,p.brand_id,p.brand_name,p.cate1_id,p.cate1_name,p.cate2_name,ROUND ((1 - vo.discount_amt/vo.unit_price) * 100, 2) AS single_discount_rate FROM valid_orders vo LEFT JOIN dw_dim.dim_product p ON vo.product_id = p.product_id AND vo.dt = p.dt WHERE p.shelf_status = 1),brand_date_agg AS (SELECT brand_id,brand_name,cate1_name,pay_date,pay_hour,COUNT (DISTINCT order_id) AS order_count,COUNT (DISTINCT user_id) AS user_count,SUM (order_num) AS total_sales_num,SUM (pay_amt) AS total_pay_amt,AVG (single_discount_rate) AS avg_discount_rate,ROW_NUMBER () OVER (PARTITION BY brand_id, pay_date ORDER BY SUM (pay_amt) DESC) AS hour_sales_rank,ROUND (SUM (pay_amt) / SUM (SUM (pay_amt)) OVER (PARTITION BY cate1_name, pay_date) * 100, 2) AS cate_sales_ratio FROM order_product_relation GROUP BY brand_id, brand_name, cate1_name, pay_date, pay_hour HAVING order_count >= 3) SELECT brand_id,brand_name,cate1_name,pay_date,pay_hour,order_count,user_count,total_sales_num,CONCAT (ROUND (total_pay_amt / 10000, 2), ' 万 ') AS total_pay_amt_wan,avg_discount_rate,cate_sales_ratio,hour_sales_rank FROM brand_date_agg WHERE total_pay_amt >= 10000 OR (user_count / (SELECT COUNT (DISTINCT user_id) FROM valid_orders) >= 0.6) ORDER BY total_pay_amt DESC, order_count DESC LIMIT 50;";
        // 本地开发：强制指定Hadoop用户（与远程Hive集群的操作用户一致，如hadoop）
//        System.setProperty("HADOOP_USER_NAME", "hadoop");
        // 禁用Hive元数据本地缓存，强制走远程（本地开发必配）
//        System.setProperty("hive.metastore.cache.pinobjtypes", "NONE");
//        System.setProperty("hive.metastore.cache.expireAfter", "0s");

        // 构建SparkSession：本地解析血缘专属配置
        String sql = "WITH user_order_detail AS (\n" +
                "    -- 关联用户表、订单表、订单明细表，整合基础信息\n" +
                "    SELECT\n" +
                "        u.user_id,\n" +
                "        u.user_name,\n" +
                "        o.order_id,\n" +
                "        o.order_amount,\n" +
                "        o.create_time AS order_create_time,\n" +
                "        oi.item_id,\n" +
                "        oi.goods_id,\n" +
                "        oi.goods_num,\n" +
                "        oi.goods_price,\n" +
                "        -- 计算单商品的小计金额\n" +
                "        oi.goods_num * oi.goods_price AS goods_subtotal,\n" +
                "        -- 【不存在字段】测试血缘：t_order表中无此字段\n" +
                "        o.nonexistent_order_field\n" +
                "    FROM t_user u\n" +
                "    INNER JOIN t_order o \n" +
                "        ON u.user_id = o.user_id\n" +
                "    INNER JOIN t_order_item oi \n" +
                "        ON o.order_id = oi.order_id\n" +
                "    WHERE o.order_date >= '2026-01-01' -- 筛选2026年的订单\n" +
                ")\n" +
                "-- 主查询：计算用户维度的统计指标\n" +
                "SELECT\n" +
                "    user_id,\n" +
                "    user_name,\n" +
                "    order_id,\n" +
                "    order_create_time,\n" +
                "    goods_id,\n" +
                "    goods_num,\n" +
                "    goods_price,\n" +
                "    goods_subtotal,\n" +
                "    -- 计算单订单的商品总金额（同订单下所有商品小计之和）\n" +
                "    SUM(goods_subtotal) OVER (PARTITION BY order_id) AS order_goods_total,\n" +
                "    -- 计算用户的累计消费金额（用户所有订单的商品总金额之和）\n" +
                "    SUM(goods_subtotal) OVER (PARTITION BY user_id) AS user_total_consume,\n" +
                "    -- 计算用户的订单数排名（按下单时间倒序）\n" +
                "    ROW_NUMBER() OVER (\n" +
                "        PARTITION BY user_id \n" +
                "        ORDER BY order_create_time DESC\n" +
                "    ) AS user_order_rn,\n" +
                "    nonexistent_order_field\n" +
                "FROM user_order_detail\n" +
                "ORDER BY user_id, user_order_rn; -- 关键修正：添加英文分号，结束SQL语句";
        sql = "-- SQL19：多层CASE WHEN+IFNULL，测试条件字段血缘\n" +
                "SELECT\n" +
                "  u.user_id,\n" +
                "  CASE\n" +
                "    WHEN SUM(oi.goods_price * oi.goods_num) >= 2000 THEN 'VIP'\n" +
                "    WHEN SUM(oi.goods_price * oi.goods_num) >= 1000 THEN 'HIGH'\n" +
                "    ELSE 'LOW'\n" +
                "  END AS user_level,\n" +
                "  IFNULL(o.pay_time, o.create_time) AS effective_pay_time,  -- NULL兜底\n" +
                "  CASE WHEN o.order_status = 'REFUNDED' THEN -o.order_amount ELSE o.order_amount END AS actual_amount\n" +
                "FROM default.t_user u\n" +
                "LEFT JOIN default.t_order o ON u.user_id = o.user_id\n" +
                "LEFT JOIN default.t_order_item oi ON o.order_id = oi.order_id\n" +
                "GROUP BY u.user_id, o.pay_time, o.create_time, o.order_status, o.order_amount;";

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
                .config("spark.hadoop.fs.defaultFS", "hdfs://115.191.22.177:8020")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .config("hive.metastore.client.socket.timeout", "300000")
                .config("log4j.logger.org.apache.hive", "ERROR")
                .config("log4j.logger.org.apache.hadoop", "ERROR")
                .getOrCreate();

        // 屏蔽Spark冗余日志，只看关键输出
        spark.sparkContext().setLogLevel("ERROR");

        try {
            // 1. 语法解析：生成未解析的逻辑计划
            LogicalPlan unresolvedPlan = spark.sessionState().sqlParser().parsePlan(sql);
            System.out.println("===== 1. 未解析逻辑计划（UnresolvedPlan） =====");
            System.out.println(unresolvedPlan.simpleString(10000) + "\n");

            // 2. 元数据解析：关联远程Hive Metastore，解析库表/字段元数据（核心步骤）
            Analyzer analyzer = spark.sessionState().analyzer();
            LogicalPlan resolvedPlan = analyzer.execute(unresolvedPlan);

            // 3. 验证解析结果：检查是否还有未解析的表/字段
            boolean hasUnresolved = resolvedPlan.exists(p -> p instanceof UnresolvedRelation);
//            System.out.println("===== 2. 解析结果验证 =====");
//            System.out.println("是否存在未解析的表/字段：" + (hasUnresolved ? "是（需检查元数据/配置）" : "否（解析成功）") + "\n");
//
//            // 4. 输出已解析的逻辑计划（用于血缘解析）
//            System.out.println("===== 3. 已解析逻辑计划（ResolvedPlan） =====");
//            System.out.println(resolvedPlan.simpleString(1000000) + "\n");

            // 5. 调用你的血缘解析方法：传入已解析的逻辑计划
            Map<String, FieldLineage> parse = parse(resolvedPlan);
            System.out.println("===== 4. 血缘解析结果 =====");
            Map<String, Set<String>> outputToSourceMapping = new HashMap<>();

            for (Map.Entry<String, FieldLineage> entry : parse.entrySet()) {
                String outputField = entry.getKey();
                Set<String> sourceFields = entry.getValue().getAllSourceFields();
                outputToSourceMapping.put(outputField, sourceFields);
            }

            // 3. 打印映射
            System.out.println("========== 字段映射关系 ==========");
            for (Map.Entry<String, Set<String>> entry : outputToSourceMapping.entrySet()) {
                System.out.println(entry.getKey() + " → " + entry.getValue());
            }
            // 此处可添加血缘结果的打印/解析逻辑（如递归输出字段依赖）

        } catch (ParseException e) {
            System.err.println("===== SQL语法解析失败 =====");
            System.err.println("错误信息：" + e.getMessage());
        } catch (Exception e) {
            System.err.println("===== 远程Hive元数据连接/解析失败 =====");
            System.err.println("错误信息：" + e.getMessage());
            // 打印关键堆栈，定位核心问题（无需打印全量堆栈）
            e.printStackTrace();
        } finally {
            // 关闭SparkSession，释放资源
            if (spark != null) {
                spark.stop();
            }
        }
    }
}
