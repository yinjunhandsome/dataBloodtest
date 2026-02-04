package org.example.utils;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.Analyzer;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.expressions.aggregate.*;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.*;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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
public class LogicalPlanLineageParser {

    // ==================== 调试开关 ====================
    // 设为false关闭所有调试输出，设为true开启详细日志
    private static final boolean DEBUG_MODE = false;

    // CTE缓存，避免循环递归
    private static final Map<String, Map<String, FieldLineage>> CTE_CACHE = new ConcurrentHashMap<>();

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
        CTE_CACHE.clear();
        return parseLogicalPlan(plan, new HashMap<>());
    }

    /**
     * 递归解析逻辑计划
     */
    private static Map<String, FieldLineage> parseLogicalPlan(LogicalPlan plan, Map<String, Map<String, FieldLineage>> cteCache) {
        if (plan == null) {
            return new HashMap<>();
        }

        // 调试：打印节点类型（不调用 output() 避免未解析节点报错）
        System.out.println(">>> Parsing node: " + plan.getClass().getSimpleName());
        if (plan instanceof Window) {
            System.out.println("    This is a Window node!");
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
        System.out.println("    DEBUG Project: childFields has " + childFields.size() + " fields");
        Seq<NamedExpression> projectList = plan.projectList();
        if (projectList != null && !projectList.isEmpty()) {
            System.out.println("    DEBUG Project: projectList has " + projectList.size() + " expressions");
            // 打印每个表达式
            Iterator<NamedExpression> it = projectList.iterator();
            int idx = 0;
            while (it.hasNext()) {
                NamedExpression expr = it.next();
                if (expr != null) {
                    String exprName = "UNKNOWN";
                    try {
                        exprName = expr.name();
                    } catch (Exception e) {
                        exprName = expr.toString();
                    }
                    System.out.println("      [" + idx + "] " + exprName);
                    idx++;
                }
            }
        }

        Map<String, FieldLineage> result = new HashMap<>();

        if (projectList != null && !projectList.isEmpty()) {
            Iterator<NamedExpression> it = projectList.iterator();
            while (it.hasNext()) {
                NamedExpression expr = it.next();
                if (expr == null) continue;

                // 安全获取字段名
                String fieldName;
                try {
                    fieldName = expr.name();
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

        // 解析Group By字段
        Seq<Expression> groupingExprs = plan.groupingExpressions();
        if (groupingExprs != null && !groupingExprs.isEmpty()) {
            Iterator<Expression> it = groupingExprs.iterator();
            while (it.hasNext()) {
                Expression expr = it.next();
                if (expr instanceof NamedExpression) {
                    String fieldName = ((NamedExpression) expr).name();
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

                    result.put(fieldName, lineage);
                }
            }
        }

        // 解析聚合表达式
        Seq<NamedExpression> aggExprs = plan.aggregateExpressions();
        if (aggExprs != null && !aggExprs.isEmpty()) {
            Iterator<NamedExpression> it = aggExprs.iterator();
            while (it.hasNext()) {
                NamedExpression expr = it.next();
                if (expr == null) continue;

                String fieldName = expr.name();
                FieldLineage lineage = new FieldLineage(fieldName, "AGGREGATE");
                lineage.setFieldType(FieldLineage.FieldType.AGGREGATE);
                // 安全获取SQL表达式
                try {
                    lineage.setExpression(((Expression) expr).sql());
                } catch (Exception e) {
                    lineage.setExpression(expr.toString());
                }

                Set<String> depNames = extractDependencies((Expression) expr);
                if (DEBUG_MODE && !depNames.isEmpty()) {
                    System.out.println("    Aggregate field '" + fieldName + "' has " + depNames.size() + " dependencies: " + depNames);
                }
                for (String depName : depNames) {
                    FieldLineage dep = findFieldInChildFields(depName, childFields);
                    if (dep != null) {
                        lineage.addDependency(cloneFieldLineage(dep));
                        if (DEBUG_MODE) {
                            System.out.println("      Added dependency: " + depName + " (total: " + lineage.getDependencies().size() + ")");
                        }
                    } else {
                        System.out.println("    WARNING: Cannot find dependency '" + depName + "' in childFields for aggregate field " + fieldName);
                    }
                }

                if (DEBUG_MODE) {
                    System.out.println("    Aggregate field '" + fieldName + "' final dependencies count: " + lineage.getDependencies().size());
                }
                result.put(fieldName, lineage);
            }
        }

        return result;
    }

    private static Map<String, FieldLineage> parseWindow(Window plan, Map<String, Map<String, FieldLineage>> cteCache) {
        Map<String, FieldLineage> childFields = parseLogicalPlan(plan.child(), cteCache);
        Map<String, FieldLineage> result = new HashMap<>(childFields);

        // 调试：打印 Window 节点的所有输出字段
        System.out.println("===== DEBUG: Window Output =====");
        Seq<Attribute> windowOutput = plan.output();
        if (windowOutput != null && !windowOutput.isEmpty()) {
            Iterator<Attribute> it = windowOutput.iterator();
            while (it.hasNext()) {
                Attribute attr = it.next();
                if (attr != null) {
                    System.out.println("  Window output: " + attr.name());
                }
            }
        }
        System.out.println("Window childFields: " + childFields.keySet());
        System.out.println("==================================");

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

        // 合并左右表字段（添加表名前缀避免冲突）
        String leftPrefix = getTableName(leftFields);
        String rightPrefix = getTableName(rightFields);

        for (Map.Entry<String, FieldLineage> entry : leftFields.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, FieldLineage> entry : rightFields.entrySet()) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }


    // ==================== 集合操作 ====================

    private static Map<String, FieldLineage> parseUnion(Union plan, Map<String, Map<String, FieldLineage>> cteCache) {
        Map<String, FieldLineage> result = new HashMap<>();

        Seq<LogicalPlan> children = plan.children();
        if (children != null && !children.isEmpty()) {
            Iterator<LogicalPlan> it = children.iterator();
            if (it.hasNext()) {
                // Union取第一个子计划的字段结构
                result = parseLogicalPlan(it.next(), cteCache);
            }
        }

        return result;
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

    // ==================== 辅助方法 ====================

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
            deps.add(((AttributeReference) expr).name());
        }
        // 处理未解析的属性引用（带前缀的字段，如 rnk.uid）
        else if (expr instanceof UnresolvedAttribute) {
            String name = ((UnresolvedAttribute) expr).name();
            if (name != null) {
                deps.add(name);
            }
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
     * 在childFields中查找字段，支持带前缀的字段名匹配
     * 例如：rnk.uid 可以匹配 uid 或 rnk.uid
     */
    private static FieldLineage findFieldInChildFields(String fieldName, Map<String, FieldLineage> childFields) {
        if (fieldName == null || childFields == null) {
            return null;
        }

        // 1. 精确匹配
        FieldLineage field = childFields.get(fieldName);
        if (field != null) {
            return field;
        }

        // 提取短字段名（去掉前缀）
        String shortName = null;
        if (fieldName.contains(".")) {
            shortName = fieldName.substring(fieldName.lastIndexOf('.') + 1);
            // 2. 尝试用短字段名匹配
            field = childFields.get(shortName);
            if (field != null) {
                return field;
            }
        }

        // 3. 遍历所有字段，尝试匹配字段名部分
        for (Map.Entry<String, FieldLineage> entry : childFields.entrySet()) {
            String childFieldName = entry.getKey();

            // 如果输入字段名等于child字段名（不分大小写）
            if (fieldName.equalsIgnoreCase(childFieldName)) {
                return entry.getValue();
            }

            // 如果都有短名称，比较短名称
            if (shortName != null) {
                String childShortName = childFieldName;
                if (childFieldName.contains(".")) {
                    childShortName = childFieldName.substring(childFieldName.lastIndexOf('.') + 1);
                }

                if (shortName.equalsIgnoreCase(childShortName)) {
                    return entry.getValue();
                }
            }
        }

        return null;
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
        String sql = "WITH\n" +
                "-- 【CTE1：基础用户表筛选】单表过滤，提取有效用户（注册日期+等级过滤），基础字段重命名\n" +
                "cte_base_user AS (\n" +
                "    SELECT\n" +
                "        user_id,\n" +
                "        t.user_name AS uname,\n" +
                "        user_level AS ulevel,\n" +
                "        register_time AS reg_time,\n" +
                "        register_date AS reg_date\n" +
                "    FROM t_user t\n" +
                "    WHERE register_date = '2026-02-04'\n" +
                "      AND user_level >= 2  -- 过滤低等级用户\n" +
                "),\n" +
                "\n" +
                "-- 【CTE2：内部含JOIN】订单主表+明细表INNER JOIN，行级聚合+无效数据过滤，为后续统计做准备\n" +
                "cte_order_join AS (\n" +
                "    SELECT\n" +
                "        oi.order_id AS oid,\n" +
                "        o.user_id AS uid,\n" +
                "        o.order_amount AS o_total_amt,  -- 主表订单总金额\n" +
                "        o.pay_status AS pay_sts,\n" +
                "        o.create_time AS o_cre_time,\n" +
                "        o.pay_time AS o_pay_time,\n" +
                "        -- 行级聚合：明细表计算实际支付金额（单价*数量），过滤无效订单项\n" +
                "        SUM(oi.goods_price * oi.goods_num) AS o_item_amt,\n" +
                "        COUNT(oi.item_id) AS o_item_count,  -- 订单包含商品数\n" +
                "        AVG(oi.goods_price) AS o_avg_price  -- 订单商品平均单价\n" +
                "    FROM t_order o\n" +
                "    INNER JOIN t_order_item oi\n" +
                "        ON o.order_id = oi.order_id\n" +
                "        AND o.order_date = oi.order_date  -- 分区字段关联，提升JOIN效率\n" +
                "    WHERE o.order_date = '2026-02-04'\n" +
                "      AND oi.goods_num > 0  -- 过滤0数量订单项\n" +
                "      AND o.order_amount > 0\n" +
                "    GROUP BY o.order_id, o.user_id, o.order_amount, o.pay_status, o.create_time, o.pay_time\n" +
                "),\n" +
                "\n" +
                "-- 【CTE3：查询前置CTE】关联CTE1(用户)和CTE2(订单JOIN结果)，新增条件函数+字段加工\n" +
                "cte_user_order AS (\n" +
                "    SELECT\n" +
                "        bu.uid,\n" +
                "        bu.uname,\n" +
                "        bu.ulevel,\n" +
                "        bu.reg_time,\n" +
                "        oj.oid,\n" +
                "        oj.o_total_amt,\n" +
                "        oj.o_item_amt,\n" +
                "        oj.o_item_count,\n" +
                "        oj.o_avg_price,\n" +
                "        oj.pay_sts,\n" +
                "        oj.o_cre_time,\n" +
                "        oj.o_pay_time,\n" +
                "        -- 条件函数：将支付状态转为文字，血缘覆盖CASE WHEN\n" +
                "        CASE oj.pay_sts\n" +
                "            WHEN 1 THEN 'PAID'\n" +
                "            WHEN 0 THEN 'UNPAID'\n" +
                "            WHEN 2 THEN 'REFUND'\n" +
                "            ELSE 'UNKNOWN'\n" +
                "        END AS pay_status_desc,\n" +
                "        -- 日期函数：计算下单到支付的时长（秒），血缘覆盖TIMESTAMPDIFF\n" +
                "        TIMESTAMPDIFF(SECOND, oj.o_cre_time, oj.o_pay_time) AS pay_duration_sec\n" +
                "    FROM cte_base_user bu\n" +
                "    LEFT JOIN cte_order_join oj\n" +
                "        ON bu.uid = oj.uid\n" +
                "),\n" +
                "\n" +
                "-- 【CTE4：自引用CTE+聚合函数】基于CTE3做用户维度聚合统计，覆盖SUM/COUNT/DISTINCT/IF\n" +
                "cte_user_agg AS (\n" +
                "    SELECT\n" +
                "        uid,\n" +
                "        uname,\n" +
                "        ulevel,\n" +
                "        reg_time,\n" +
                "        -- 聚合函数：用户总订单数（含所有状态）\n" +
                "        COUNT(DISTINCT oid) AS total_order_count,\n" +
                "        -- 聚合函数：用户已支付订单数（条件计数，血缘覆盖IF+COUNT）\n" +
                "        COUNT(IF(pay_sts = 1, oid, NULL)) AS paid_order_count,\n" +
                "        -- 聚合函数：用户已支付订单总金额（过滤退款/未支付，血缘覆盖SUM+WHERE）\n" +
                "        SUM(IF(pay_sts = 1, o_total_amt, 0)) AS paid_total_amt,\n" +
                "        -- 聚合函数：用户已支付订单平均商品数\n" +
                "        AVG(IF(pay_sts = 1, o_item_count, NULL)) AS paid_avg_item_count,\n" +
                "        -- 聚合函数：用户平均支付时长（仅已支付订单）\n" +
                "        AVG(IF(pay_sts = 1, pay_duration_sec, NULL)) AS avg_pay_duration_sec,\n" +
                "        -- 比例计算：已支付订单占比（保留2位小数）\n" +
                "        ROUND(COUNT(IF(pay_sts = 1, oid, NULL)) / COUNT(DISTINCT oid), 2) AS paid_order_rate\n" +
                "    FROM cte_user_order\n" +
                "    GROUP BY uid, uname, ulevel, reg_time\n" +
                "    HAVING total_order_count > 0  -- 过滤无订单用户\n" +
                "),\n" +
                "\n" +
                "-- 【CTE5：窗口函数+分组】基于CTE4做用户等级维度排名，覆盖ROW_NUMBER/PARTITION BY\n" +
                "cte_user_rnk AS (\n" +
                "    SELECT\n" +
                "        *,\n" +
                "        -- 窗口函数：按用户等级分组，已支付金额降序排名，血缘覆盖窗口函数\n" +
                "        ROW_NUMBER() OVER (PARTITION BY ulevel ORDER BY paid_total_amt DESC) AS level_rnk,\n" +
                "        -- 窗口函数：按用户等级分组，已支付金额累计求和，血缘覆盖SUM窗口\n" +
                "        SUM(paid_total_amt) OVER (PARTITION BY ulevel ORDER BY paid_total_amt DESC) AS level_paid_amt_cum\n" +
                "    FROM cte_user_agg\n" +
                ")\n" +
                "\n" +
                "-- 【主查询：多CTE关联+最终过滤】关联CTE3(明细)和CTE5(聚合+排名)，输出全量血缘字段\n" +
                "SELECT\n" +
                "    -- 聚合层字段（来自CTE5）\n" +
                "    rnk.uid,\n" +
                "    rnk.uname,\n" +
                "    rnk.ulevel,\n" +
                "    rnk.reg_time,\n" +
                "    rnk.total_order_count,\n" +
                "    rnk.paid_order_count,\n" +
                "    rnk.paid_total_amt,\n" +
                "    rnk.paid_avg_item_count,\n" +
                "    rnk.avg_pay_duration_sec,\n" +
                "    rnk.paid_order_rate,\n" +
                "    rnk.level_rnk,\n" +
                "    rnk.level_paid_amt_cum,\n" +
                "    -- 明细层字段（来自CTE3）\n" +
                "    ord.oid,\n" +
                "    ord.o_total_amt,\n" +
                "    ord.o_item_amt,\n" +
                "    ord.o_item_count,\n" +
                "    ord.pay_sts,\n" +
                "    ord.pay_status_desc,\n" +
                "    ord.o_cre_time,\n" +
                "    ord.o_pay_time,\n" +
                "    ord.pay_duration_sec,\n" +
                "    -- 常量字段：血缘覆盖常量值\n" +
                "    '2026-02-04' AS data_date,\n" +
                "    -- 函数嵌套：血缘覆盖多层函数（ISNULL+CAST）\n" +
                "    CAST(ISNULL(ord.pay_duration_sec, 0) AS BIGINT) AS pay_duration_sec_nn  -- 空值填充为0\n" +
                "FROM cte_user_rnk rnk\n" +
                "LEFT JOIN cte_user_order ord\n" +
                "    ON rnk.uid = ord.uid\n" +
                "WHERE rnk.paid_order_count > 0  -- 最终过滤：仅保留有已支付订单的用户\n" +
                "ORDER BY\n" +
                "    rnk.ulevel ASC,\n" +
                "    rnk.level_rnk ASC,\n" +
                "    ord.o_cre_time ASC;";
        SparkSession spark = SparkSession.builder()
                .appName("LocalHiveLineageParser")
                .master("local[*]")
                .enableHiveSupport()
                // ========== 新增：显式绑定远程Hive Metastore地址（核心） ==========
                .config("hive.metastore.uris", "thrift://115.191.22.177:9083")
                // ========== 原有辅助配置保留 ==========
//                .config("spark.sql.optimizer.enabled", "false")
//                .config("spark.sql.autoBroadcastJoinThreshold", "-1")
//                .config("spark.sql.adaptive.enabled", "false")
//                .config("spark.hadoop.fs.defaultFS", "hdfs://115.191.22.177:8020")
//                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
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
            System.out.println("===== 2. 解析结果验证 =====");
            System.out.println("是否存在未解析的表/字段：" + (hasUnresolved ? "是（需检查元数据/配置）" : "否（解析成功）") + "\n");

            // 4. 输出已解析的逻辑计划（用于血缘解析）
            System.out.println("===== 3. 已解析逻辑计划（ResolvedPlan） =====");
            System.out.println(resolvedPlan.simpleString(1000000) + "\n");

            // 5. 调用你的血缘解析方法：传入已解析的逻辑计划
            Map<String, FieldLineage> parse = parse(resolvedPlan);
            System.out.println("===== 4. 血缘解析结果 =====");
            System.out.println(parse);

            // 打印详细血缘链（选择几个示例字段）
            System.out.println("\n===== 5. 详细血缘链示例 =====");
            String[] sampleFields = {"rnk.uid", "rnk.paid_total_amt", "rnk.level_rnk"};
            for (String fieldName : sampleFields) {
                FieldLineage lineage = parse.get(fieldName);
                if (lineage != null) {
                    System.out.println("\n字段: " + fieldName);
                    System.out.println(lineage.getLineageDescription());
                    System.out.println("所有源头字段: " + lineage.getAllSourceFields());
                }
            }
            System.out.println("\n===== 血缘链示例结束 =====");
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
