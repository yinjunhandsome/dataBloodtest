package org.example.utils;

import com.bj58.dpd.dp.util.ExpUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.*;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.expressions.aggregate.*;
import org.apache.spark.sql.catalyst.plans.logical.*;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.execution.command.*;
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand;
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable;
import org.springframework.stereotype.Component;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;

import javax.annotation.Resource;
import java.util.*;

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
@Component
public class LogicalPlanLineageParser {

    @Resource
    private SparkSession spark;

    public Map<String, Set<String>> parse(String sql){
        try {
            String new_sql= ExpUtils.replace(sql);
            LogicalPlan unresolvedPlan = spark.sessionState().sqlParser().parsePlan(new_sql);
            Analyzer analyzer = spark.sessionState().analyzer();
            LogicalPlan resolvedPlan = analyzer.execute(unresolvedPlan);
            Map<String, FieldLineage> parse = parse(resolvedPlan);
            Map<String, Set<String>> allSourceFields = getAllSourceFields(parse);
            for (Map.Entry<String, Set<String>> entry : allSourceFields.entrySet()) {
                System.out.println(entry.getKey() + " → " + entry.getValue());
            }
            return allSourceFields;
        }
        catch (Exception e) {
            System.err.println("===== 远程Hive元数据连接/sql解析失败 =====");
            System.err.println("错误信息：" + e.getMessage());
            throw new RuntimeException(e);
        }
    }

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
        // 先解析所有CTE定义并缓存
        Seq<CTERelationDef> cteDefs = plan.cteDefs();
        if (cteDefs != null && !cteDefs.isEmpty()) {
            System.out.println("Found " + cteDefs.size() + " CTE definitions");
            Iterator<CTERelationDef> it = cteDefs.iterator();
            while (it.hasNext()) {
                CTERelationDef def = it.next();
                if (def != null) {
                    try {
                        Map<String, FieldLineage> cteFields = parseLogicalPlan(def.child(), cteCache);
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

        Map<String, FieldLineage> cached = cteCache.get(cacheKey);
        if (cached != null) {
            // 规范化：移除所有键的前缀，只保留纯字段名
            Map<String, FieldLineage> normalizedResult = new HashMap<>();
            for (Map.Entry<String, FieldLineage> entry : cached.entrySet()) {
                String originalKey = entry.getKey();
                FieldLineage lineage = cloneFieldLineage(entry.getValue());

                // 移除前缀
                String normalizedKey = originalKey;
                if (originalKey.contains(".")) {
                    normalizedKey = originalKey.substring(originalKey.lastIndexOf('.') + 1);
                }

                // 如果键已存在，合并依赖而不是覆盖
                if (normalizedResult.containsKey(normalizedKey)) {
                    FieldLineage existing = normalizedResult.get(normalizedKey);
                    for (FieldLineage dep : lineage.getDependencies()) {
                        existing.addDependency(cloneFieldLineage(dep));
                    }
                } else {
                    normalizedResult.put(normalizedKey, lineage);
                }
            }
            return normalizedResult;
        }
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
                    if (exprClassName.equals("Star") || exprClassName.equals("UnresolvedStar")) {
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
                    for (String depName : depNames) {
                        FieldLineage dep = findFieldInChildFields(depName, childFields);
                        if (dep != null) {
                            lineage.addDependency(cloneFieldLineage(dep));
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

        Map<String, FieldLineage> result = new HashMap<>();

        // 第一步：解析 Group By 字段，并收集它们的血缘信息供后续聚合字段使用
        List<FieldLineage> groupingFieldLineages = new ArrayList<>();
        Seq<Expression> groupingExprs = plan.groupingExpressions();
        if (groupingExprs != null && !groupingExprs.isEmpty()) {
            Iterator<Expression> it = groupingExprs.iterator();
            while (it.hasNext()) {
                Expression expr = it.next();
                // 获取字段名：支持 AttributeReference 和 NamedExpression
                String fieldName;
                if (expr instanceof AttributeReference) {
                    fieldName = ((AttributeReference) expr).name();
                } else if (expr instanceof NamedExpression) {
                    fieldName = ((NamedExpression) expr).name();
                }
                else if (expr instanceof Cast) {
                    fieldName = handleCastExpressionInGroupBy((Cast) expr, childFields, cteCache);
                    if (fieldName == null) {
                        continue;
                    }
                }

                else {
                    // 其他类型（如表达式），跳过
                    continue;
                }

                FieldLineage lineage = new FieldLineage(fieldName, "AGGREGATE");
                Set<String> depNames = extractDependencies(expr);
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
            }
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
                    for (String depName : depNames) {
                        FieldLineage dep = findFieldInChildFields(depName, childFields);
                        if (dep != null) {
                            lineage.addDependency(cloneFieldLineage(dep));
                        } else {
                            System.out.println("    WARNING: Cannot find dependency '" + depName + "' in childFields for aggregate field " + fieldName);
                        }
                    }

                    // 2. 添加所有 Group By 字段作为依赖
                    // 因为聚合字段是在每个分组内计算的，依赖于分组键
                    if (!groupingFieldLineages.isEmpty()) {
                        for (FieldLineage groupingLineage : groupingFieldLineages) {
                            // 克隆 Group By 字段的血缘作为聚合字段的依赖
                            lineage.addDependency(cloneFieldLineage(groupingLineage));
                        }
                    }
                } else {
                    // === 非聚合字段（直接SELECT的Group By字段，如 user_id） ===
                    // 只添加它自己的依赖，不添加所有Group By字段
                    lineage.setFieldType(FieldLineage.FieldType.COLUMN);

                    // 只提取字段自身的依赖
                    Set<String> depNames = extractDependencies((Expression) expr);
                    for (String depName : depNames) {
                        FieldLineage dep = findFieldInChildFields(depName, childFields);
                        if (dep != null) {
                            lineage.addDependency(cloneFieldLineage(dep));
                        } else {
                            System.out.println("    WARNING: Cannot find dependency '" + depName + "' in childFields for field " + fieldName);
                        }
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
        //todo 这里最好日志或者数据库记录一下，有哪些算子未被捕获，后续不断扩充
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

        // 根据子表达式类型生成字段名
        String baseFieldName;

        // 1. 如果子表达式是 AttributeReference（直接字段引用）
        if (child instanceof AttributeReference) {
            AttributeReference attrRef = (AttributeReference) child;
            baseFieldName = attrRef.name();
            // 生成 Cast 后的字段名：格式为 "cast_{字段名}_as_{类型}"
            return "cast_" + baseFieldName + "_as_" + dataType.toLowerCase().replaceAll("[^a-z0-9]", "_");
        }

        // 2. 如果子表达式是 UnresolvedAttribute（未解析的字段引用）
        else if (child instanceof UnresolvedAttribute) {
            UnresolvedAttribute unresolvedAttr = (UnresolvedAttribute) child;
            baseFieldName = unresolvedAttr.name();

            // 生成 Cast 后的字段名
            return "cast_" + baseFieldName.replaceAll("[^a-zA-Z0-9_]", "_") + "_as_" + dataType.toLowerCase().replaceAll("[^a-z0-9]", "_");
        }

        // 3. 如果子表达式是 Alias（带别名的表达式）
        else if (child instanceof Alias) {
            Alias alias = (Alias) child;
            baseFieldName = alias.name();

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

        // 1. 检查是否是已解析的聚合函数
        if (expr instanceof AggregateFunction) {
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
                if (containsAggregateFunction(child, depth + 1)) {
                    return true;
                }
            }
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
                // 递归提取 predicate 中的依赖
                deps.addAll(extractDependencies(predicate));
            }

            // 第二个元素是 value（THEN 结果）
            org.apache.spark.sql.catalyst.expressions.Expression value = branch._2();
            if (value != null) {
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
            // 如果 ELSE 值不是 Literal（常量），提取其依赖
            if (!(elseValue instanceof Literal)) {
                deps.addAll(extractDependencies(elseValue));
            }
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

    private static FieldLineage findFieldInChildFields(String fieldName, Map<String, FieldLineage> childFields) {
        if (fieldName == null || childFields == null) {
            return null;
        }
        String[] split = fieldName.split("\\.");
        if (split.length==1) {
            String shortName=split[0];
            // 先尝试精确匹配
            FieldLineage fieldLineage = childFields.get(shortName);
            if (fieldLineage != null) {
                return fieldLineage;
            }
            // 精确匹配失败，进行忽略大小写的遍历查找
            else {
                for (Map.Entry<String, FieldLineage> child : childFields.entrySet()) {
                    String[] children = child.getKey().split("\\.");
                    if (children.length>1) {
                        if (StringUtils.equalsIgnoreCase(children[children.length-1], shortName)) {
                            return child.getValue();
                        }
                    } else if (children.length==1) {
                        // 处理单层级字段名的忽略大小写匹配
                        if (StringUtils.equalsIgnoreCase(children[0], shortName)) {
                            return child.getValue();
                        }
                    }
                }
            }
        }
        else  {
            String shortName=split[split.length-2]+"."+split[split.length-1];
            // 先尝试精确匹配
            FieldLineage fieldLineage = childFields.get(shortName);
            if (fieldLineage != null) {
                return fieldLineage;
            }
            // 精确匹配失败，进行忽略大小写的遍历查找
            else {
                for (Map.Entry<String, FieldLineage> child : childFields.entrySet()) {
                    String[] children = child.getKey().split("\\.");
                    if (children.length==1) {
                        if (StringUtils.equalsIgnoreCase(children[0], split[split.length-1])) {
                            return child.getValue();
                        }
                    }
                    else {
                        String childShortName = children[children.length-2]+"."+children[children.length-1];
                        FieldLineage value = child.getValue();
                        String tableName = StringUtils.isBlank(value.getTableName())?"":value.getTableName();
                        String aliasName = tableName+"."+children[split.length-1];
                        if (StringUtils.equalsIgnoreCase(childShortName, shortName)||StringUtils.equalsIgnoreCase(shortName, aliasName)) {
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
}
