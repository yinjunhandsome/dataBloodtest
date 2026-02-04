package org.example.utils;

import com.alibaba.fastjson.JSON;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.*;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Spark SQL 字段血缘核心解析器（优雅重构版+空指针全修复）
 * 特性：极简数据结构、节点解耦、递归穿透、自动追溯源头、全空值保护
 * 支持解析：LogicalRelation/Project/Aggregate/Join/Filter/CTE/SubqueryAlias/WithCTE
 */
public class SparkSqlLineageParser {

    // 生成唯一节点后缀（用于CTE/Join/Project等加工节点）
    private static final Random RANDOM = new Random();

    /**
     * 对外暴露的核心解析方法
     * @param plan Spark已解析的逻辑计划（resolvedPlan）
     * @return 最终血缘结果（永不返回null）
     */
    public static DataBlood parse(LogicalPlan plan) {
        if (plan == null) {
            return new DataBlood(); // 修复：空plan返回空DataBlood，禁止null
        }

        // 策略模式：节点类型→解析方法，替代冗长if-else
        if (plan instanceof WithCTE) {
            return parseWithCTE((WithCTE) plan);
        } else if (plan instanceof UnresolvedWith) {
            return parseUnresolvedWith((UnresolvedWith) plan);
        } else if (plan instanceof SubqueryAlias) {
            return parseSubqueryAlias((SubqueryAlias) plan);
        } else if (plan instanceof Project) {
            return parseProject((Project) plan);
        } else if (plan instanceof Filter) {
            return parseFilter((Filter) plan); // 穿透解析
        } else if (plan instanceof LogicalRelation) {
            return parseLogicalRelation((LogicalRelation) plan); // 物理表，最终源头
        } else if (plan instanceof Aggregate) {
            return parseAggregate((Aggregate) plan);
        } else if (plan instanceof Join) {
            return parseJoin((Join) plan);
        } else if (plan instanceof CTERelationRef) {
            return parseCTERelationRef((CTERelationRef) plan); // CTE引用节点
        } else {
            // 未支持的节点类型：尝试解析子节点，子节点为空则返回空DataBlood
            Seq<LogicalPlan> children = plan.children();
            return children.nonEmpty() ? parse(children.head()) : new DataBlood();
        }
    }

    // 解析WithCTE（已解析的CTE）- 修复空值遍历+空返回
    private static DataBlood parseWithCTE(WithCTE plan) {
        // 先解析所有CTE定义，再解析主查询
        Seq<CTERelationDef> cteDefs = plan.cteDefs();
        if (cteDefs != null && !cteDefs.isEmpty()) { // 非空保护
            Iterator<CTERelationDef> iterator = cteDefs.iterator();
            while (iterator.hasNext()) {
                CTERelationDef def = iterator.next();
                if (def != null) { // 空值保护
                    DataBlood parse = parse(def.child());// CTE定义解析（递归）
                    System.out.println(JSON.toJSONString(parse));
                }
            }
        }
        // 主查询为最终结果，parse保证永不返回null
        return parse(plan.plan());
    }

    // 解析UnresolvedWith（未解析的CTE）- 修复空返回
    private static DataBlood parseUnresolvedWith(UnresolvedWith plan) {
        Seq<Tuple2<String, SubqueryAlias>> cteRelations = plan.cteRelations();
        if (cteRelations != null && !cteRelations.isEmpty()) { // 非空保护
            Iterator<Tuple2<String, SubqueryAlias>> iterator = cteRelations.iterator();
            while (iterator.hasNext()) {
                Tuple2<String, SubqueryAlias> tuple = iterator.next();
                if (tuple != null && tuple._2() != null) { // 空值保护
                    parse(tuple._2().child()); // 解析CTE子计划
                }
            }
        }
        // 修复：返回parse结果（永不null），禁止直接返回null
        return parse(plan.child());
    }

    // 解析子查询别名（SubqueryAlias）- 修复空值处理
    private static DataBlood parseSubqueryAlias(SubqueryAlias plan) {
        DataBlood childBlood = parse(plan.child()); // 子血缘永不null
        // 重命名当前节点，血缘关系不变
        childBlood.setCurrentNode(buildNodeName("ALIAS", plan.alias()));
        return childBlood; // 直接返回，无需判空
    }

    // 解析投影节点（Project）：字段加工/重命名核心节点
    private static DataBlood parseProject(Project plan) {
        DataBlood childBlood = parse(plan.child()); // 子血缘永不null
        // 初始化当前Project节点血缘
        DataBlood projectBlood = DataBlood.builder()
                .nodeType(DataBlood.NodeType.PROCESS_NODE)
                .currentNode(buildRandomNodeName("PROJECT"))
                .build();

        // 子血缘无字段信息，直接返回空projectBlood
        if (childBlood.getFieldLineage().isEmpty()) {
            return projectBlood;
        }

        // 遍历所有投影字段，解析依赖并追溯源头
        Seq<NamedExpression> projectList = plan.projectList();
        if (projectList != null && !projectList.isEmpty()) { // 非空保护
            Iterator<NamedExpression> iterator = projectList.iterator();
            while (iterator.hasNext()) {
                NamedExpression expr = iterator.next();
                if (expr == null) continue; // 空值跳过
                String targetField = expr.name(); // 当前投影后的字段名
                Set<String> dependFields = AliasUtils.parseNamedExprDependencies(expr); // 依赖的上游字段

                // 追溯每个依赖字段的源头表和字段
                for (String dependField : dependFields) {
                    traceAndAddLineage(projectBlood, targetField, dependField, childBlood);
                }
            }
        }

        return projectBlood;
    }

    // 解析过滤节点（Filter）：直接穿透，不修改血缘
    private static DataBlood parseFilter(Filter plan) {
        DataBlood childBlood = parse(plan.child()); // 子血缘永不null
        childBlood.setCurrentNode(buildRandomNodeName("FILTER"));
        return childBlood;
    }

    // 解析物理表（LogicalRelation）：最终源头，初始化血缘
    private static DataBlood parseLogicalRelation(LogicalRelation plan) {
        Option<CatalogTable> catalogTableOpt = plan.catalogTable();
        if (catalogTableOpt.isEmpty()) {
            return new DataBlood(); // 无表信息返回空血缘
        }

        CatalogTable table = catalogTableOpt.get();
        String dbName = table.database() == null ? "default" : table.database(); // 兜底默认库
        String tblName = table.identifier().table();
        String fullTableName = String.join(".", dbName, tblName);

        // 初始化物理表血缘：字段→自身表→自身字段
        DataBlood tableBlood = DataBlood.builder()
                .nodeType(DataBlood.NodeType.PHYSICAL_TABLE)
                .currentNode(fullTableName)
                .build();

        // 提取表所有字段，非空保护
        Seq<AttributeReference> output = plan.output();
        if (output != null && !output.isEmpty()) {
            Iterator<AttributeReference> iterator = output.iterator();
            while (iterator.hasNext()) {
                AttributeReference attr = iterator.next();
                if (attr != null) {
                    String fieldName = attr.name();
                    tableBlood.addFieldLineage(fieldName, fullTableName, fieldName);
                }
            }
        }

        return tableBlood;
    }

    // 解析聚合节点（Aggregate）：GroupBy+聚合函数解析
    private static DataBlood parseAggregate(Aggregate plan) {
        DataBlood childBlood = parse(plan.child()); // 子血缘永不null
        // 初始化聚合节点血缘
        DataBlood aggBlood = DataBlood.builder()
                .nodeType(DataBlood.NodeType.PROCESS_NODE)
                .currentNode(buildRandomNodeName("AGGREGATE"))
                .build();

        // 子血缘无字段信息，直接返回
        if (childBlood.getFieldLineage().isEmpty()) {
            return aggBlood;
        }

        // 遍历聚合表达式（包含GroupBy字段+聚合函数字段）
        Seq<NamedExpression> aggExprs = plan.aggregateExpressions();
        if (aggExprs != null && !aggExprs.isEmpty()) { // 非空保护
            Iterator<NamedExpression> iterator = aggExprs.iterator();
            while (iterator.hasNext()) {
                NamedExpression expr = iterator.next();
                if (expr == null) continue;
                String targetField = expr.name();
                Set<String> dependFields = AliasUtils.parseNamedExprDependencies(expr);

                // 追溯聚合字段的源头
                for (String dependField : dependFields) {
                    traceAndAddLineage(aggBlood, targetField, dependField, childBlood);
                }
            }
        }

        return aggBlood;
    }

    // 解析Join节点：融合左右表血缘，保留所有源头关系
    private static DataBlood parseJoin(Join plan) {
        // 左右表解析，均返回非null
        DataBlood leftBlood = parse(plan.left());
        DataBlood rightBlood = parse(plan.right());

        // 初始化Join节点血缘
        DataBlood joinBlood = DataBlood.builder()
                .nodeType(DataBlood.NodeType.PROCESS_NODE)
                .currentNode(buildNodeName("JOIN",
                        leftBlood.getCurrentNode() != null ? leftBlood.getCurrentNode() : "LEFT",
                        rightBlood.getCurrentNode() != null ? rightBlood.getCurrentNode() : "RIGHT"))
                .build();

        // 融合左右表血缘（mergeBlood做了空值保护）
        joinBlood.mergeBlood(leftBlood);
        joinBlood.mergeBlood(rightBlood);

        return joinBlood;
    }

    // 解析CTE引用（CTERelationRef）：CTE引用节点，返回标识化血缘
    private static DataBlood parseCTERelationRef(CTERelationRef plan) {
        // 修复：叶子节点无child，返回带CTE_REF标识的血缘，永不null
        return DataBlood.builder()
                .nodeType(DataBlood.NodeType.CTE_REF)
                .currentNode(buildNodeName("CTE_REF", String.valueOf(plan.cteId())))
                .build();
    }

    /**
     * 核心方法：追溯依赖字段的源头，并添加到目标血缘中（全空值保护+类型严格匹配）
     * 适配DataBlood的三层嵌套fieldLineage：Map<目标字段, Map<源头表, Set<源头字段>>>
     * @param targetBlood 目标血缘对象（存储最终追溯结果）
     * @param targetField 目标字段（当前节点的字段）
     * @param dependField 依赖字段（上游节点的字段，需要追溯其源头）
     * @param upstreamBlood 上游血缘对象（依赖字段所在的血缘）
     */
    private static void traceAndAddLineage(DataBlood targetBlood, String targetField,
                                           String dependField, DataBlood upstreamBlood) {
        // 全链路空值保护：任意入参为null直接返回，避免NPE
        if (targetBlood == null || targetField == null || dependField == null || upstreamBlood == null) {
            return;
        }

        // 1. 修正类型：严格匹配DataBlood的三层嵌套Map（核心修复点）
        Map<String, Map<String, Set<String>>> fieldLineage = upstreamBlood.getFieldLineage();
        // 上游血缘无数据，直接返回
        if (fieldLineage == null || fieldLineage.isEmpty()) {
            return;
        }

        // 2. 上游血缘中无该依赖字段，直接返回
        if (!fieldLineage.containsKey(dependField)) {
            return;
        }

        // 3. 提取依赖字段对应的「源头表→源头字段集合」映射（第二层Map）
        Map<String, Set<String>> upstreamTable2Fields = fieldLineage.get(dependField);
        // 依赖字段无源头信息，直接返回
        if (upstreamTable2Fields == null || upstreamTable2Fields.isEmpty()) {
            return;
        }

        // 4. 安全遍历：逐层添加到目标血缘，全空值保护
        upstreamTable2Fields.forEach((sourceTable, sourceFields) -> {
            if (sourceTable == null || sourceFields == null || sourceFields.isEmpty()) {
                return;
            }
            sourceFields.forEach(sourceField -> {
                if (sourceField != null) {
                    // 调用DataBlood的addFieldLineage，逐层初始化嵌套集合
                    targetBlood.addFieldLineage(targetField, sourceTable, sourceField);
                }
            });
        });
    }
    // 构建带标识的节点名（如 ALIAS_user, CTE_REF_0）
    private static String buildNodeName(String prefix, String... suffixes) {
        if (prefix == null) prefix = "UNKNOWN";
        String suffix = Arrays.stream(suffixes)
                .filter(Objects::nonNull)
                .map(s -> s.replace(".", "_"))
                .collect(Collectors.joining("_"));
        return String.join("_", prefix, suffix);
    }

    // 构建随机唯一节点名（如 PROJECT_8F2A9D7C），避免重复
    private static String buildRandomNodeName(String prefix) {
        if (prefix == null) prefix = "PROCESS";
        String randomStr = UUID.randomUUID().toString().replace("-", "").substring(0, 8).toUpperCase();
        return String.join("_", prefix, randomStr);
    }

    // 测试主方法（可直接运行，依赖Hive元数据需修改配置）
    public static void main(String[] args) {
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
        // 构建SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("SparkSqlLineageParser")
                .master("local[*]")
                .enableHiveSupport()
                .config("hive.metastore.uris", "thrift://115.191.22.177:9083")
                .config("hive.metastore.client.socket.timeout", "300000")
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        try {
            // 解析SQL为逻辑计划（未解析→已解析）
            LogicalPlan unresolvedPlan = spark.sessionState().sqlParser().parsePlan(sql);
            LogicalPlan resolvedPlan = spark.sessionState().analyzer().execute(unresolvedPlan);

            // 执行血缘解析（返回非null）
            DataBlood finalBlood = SparkSqlLineageParser.parse(resolvedPlan);

            // 打印血缘结果（格式化输出，便于查看）
            printLineageResult(finalBlood);

        } catch (Exception e) {
            System.err.println("血缘解析失败：" + e.getMessage());
            e.printStackTrace();
        } finally {
            if (spark != null) {
                spark.stop();
            }
        }
    }

    // 格式化打印血缘结果（核心字段→源头表→源头字段），空值保护
    public static void printLineageResult(DataBlood blood) {
        if (blood == null || blood.getFieldLineage().isEmpty()) {
            System.out.println("无血缘信息");
            return;
        }

        System.out.println("===== Spark SQL 字段血缘解析结果 =====");
        System.out.println("当前节点：" + (blood.getCurrentNode() == null ? "UNKNOWN" : blood.getCurrentNode()));
        System.out.println("节点类型：" + (blood.getNodeType() == null ? "UNKNOWN" : blood.getNodeType()));
        System.out.println("=====================================");
        blood.getFieldLineage().forEach((targetField, table2Fields) -> {
            if (targetField == null || table2Fields.isEmpty()) return;
            System.out.println("目标字段：" + targetField);
            table2Fields.forEach((sourceTable, sourceFields) -> {
                if (sourceTable == null || sourceFields.isEmpty()) return;
                System.out.println("  → 源头表：" + sourceTable);
                System.out.println("    → 源头字段：" + String.join(", ", sourceFields));
            });
            System.out.println("-------------------------------------");
        });
    }
}