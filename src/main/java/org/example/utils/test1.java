package org.example.utils;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.Analyzer;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class test1 {

    // ====================== 新增：分支原始类型枚举（标记别名归属）======================
    public enum OriginalNodeType {
        REAL_TABLE,  // 真实物理表（hive/db.tbl）
        CTE,         // CTE临时表（WITH定义）
        SUBQUERY     // 子查询（SELECT (...) AS alias）
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    @Builder
    public static class DataBlood {
        //物理来源 db.name->set<column>
        private Map<String,BranchBlood> branches = new HashMap<>();
        //字段映射
        private Map<String,Set<String>> columnFlat = new HashMap<>();
        // ====================== 新增：别名溯源映射（新别名 → 原始/上一级标识）======================
        private Map<String, String> aliasTraceMap = new HashMap<>(); // 解决别名嵌套溯源
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    @Builder
    public static class BranchBlood {
        private String dbName;
        private String tblName;       // 原始名称（真实表名/CTE名/子查询标识，永久不变）
        private String tblAlias;      // 当前别名（随SubqueryAlias更新，可多层嵌套）
        private Set<String> fields;
        // ====================== 新增：溯源标记（标记当前分支原始类型）======================
        private OriginalNodeType originalType; // 核心：知道别名是给什么类型节点起的
    }


    //cte名字：（cte字段：真实表来源字段）
    public Map<String, Map<String, Set<String>>> map = new HashMap<>();

    // ====================== 对外入口：保持原有方法签名不变 ======================
    public static DataBlood parser(LogicalPlan plan) {
        // 调用重载方法，初始分支标识为null，递归传递
        return parser(plan, null);
    }

    // ====================== 新增重载方法：带当前分支标识（核心改造，不影响外部调用）======================
    // currentBranchKey：当前分支唯一标识（db.tbl/CTE名/子查询标识，递归传递）
    private static DataBlood parser(LogicalPlan plan, String currentBranchKey) {
        if (plan instanceof UnresolvedWith) {
            UnresolvedWith withPlan = (UnresolvedWith) plan;
            // 解析CTE定义
            Seq<Tuple2<String, SubqueryAlias>> ctes = withPlan.cteRelations();
            Iterator<Tuple2<String, SubqueryAlias>> cteIter = ctes.iterator();
            while (cteIter.hasNext()) {
                Tuple2<String, SubqueryAlias> cte = cteIter.next();
                LogicalPlan ctePlan = cte._2();
                // 解析单个CTE的字段映射：传递CTE名称作为初始分支标识，标记为CTE类型
                DataBlood cteBlood = parser(ctePlan, cte._1());
                if (cteBlood != null) {
                    cteBlood.getBranches().values().forEach(branch -> {
                        if (branch.getOriginalType() == null) {
                            branch.setOriginalType(OriginalNodeType.CTE);
                        }
                    });
                }
            }
        }

        if (plan instanceof WithCTE) {
            WithCTE withPlan = (WithCTE) plan;
            // 解析CTE定义
            Seq<CTERelationDef> ctes = withPlan.cteDefs();
            LogicalPlan query = withPlan.plan();
            Iterator<CTERelationDef> iterator = ctes.iterator();
            List<DataBlood> DataBloods = new ArrayList<>();
            while (iterator.hasNext()) {
                CTERelationDef next = iterator.next();
                LogicalPlan child = next.child();
                // 传递CTE名称作为初始分支标识，标记为CTE类型
                DataBlood cteBlood = parser(child, next.nodeName());
                if (cteBlood != null) {
                    cteBlood.getBranches().values().forEach(branch -> {
                        if (branch.getOriginalType() == null) {
                            branch.setOriginalType(OriginalNodeType.CTE);
                        }
                    });
                    DataBloods.add(cteBlood);
                }
            }
        }

        // ====================== 核心改造：SubqueryAlias节点处理（不修改其他逻辑）======================
        if (plan instanceof SubqueryAlias) {
            SubqueryAlias aliasPlan = (SubqueryAlias) plan;
            String newAlias = aliasPlan.alias(); // 当前节点定义的新别名
            LogicalPlan childPlan = aliasPlan.child(); // 被别名的子节点（真实表/CTE/子查询）

            // 1. 递归解析子节点，传递原有分支标识（溯源被别名的节点）
            DataBlood childBlood = parser(childPlan, currentBranchKey);
            if (childBlood == null || childBlood.getBranches().isEmpty()) {
                return childBlood;
            }

            // 2. 子节点的唯一分支标识（递归保证单分支一个key）
            String originalBranchKey = childBlood.getBranches().keySet().iterator().next();
            BranchBlood originalBranch = childBlood.getBranches().get(originalBranchKey);

            // 3. 记录别名溯源：新别名 → 原始分支标识（解决多层别名：a→b→t_user）
            childBlood.getAliasTraceMap().put(newAlias, originalBranchKey);

            // 4. 更新分支的当前别名（覆盖原有别名，支持多层嵌套）
            originalBranch.setTblAlias(newAlias);

            // 5. 构建新的分支标识（统一格式：真实表=db.alias，其他=类型_alias）
            String newBranchKey = buildBranchKey(originalBranch);

            // 6. 维护branches：删除原始标识，保留最新别名标识（递归始终取最新）
            childBlood.getBranches().remove(originalBranchKey);
            childBlood.getBranches().put(newBranchKey, originalBranch);

            return childBlood;
        }

        // 分支3：解析Project（字段投影/计算层，核心）→ 完全保留原有逻辑
        if (plan instanceof Project) {
            Project projectPlan = (Project) plan;
            // 解析Project中的所有字段
            Map<String, Set<String>> flatMap = new HashMap<>();
            Seq<NamedExpression> namedExpressionSeq = projectPlan.projectList();
            Iterator<NamedExpression> iterator = namedExpressionSeq.iterator();
            while (iterator.hasNext()) {
                NamedExpression namedExpression = iterator.next();
                Set<String> strings = AliasUtils.parseNamedExprDependencies(namedExpression);
                flatMap.put(namedExpression.name(), strings);
            }
            // 递归解析Project的子节点：传递当前分支标识
            DataBlood DataBlood = parser(projectPlan.child(), currentBranchKey);
            if (DataBlood == null) {
                DataBlood = new DataBlood();
            }
            DataBlood.setColumnFlat(flatMap);

            // 子查询初始化：无原始分支时，标记为SUBQUERY类型
            if (currentBranchKey != null && DataBlood.getBranches().isEmpty()) {
                BranchBlood subqBranch = BranchBlood.builder()
                        .tblName(currentBranchKey)
                        .tblAlias(currentBranchKey)
                        .fields(new HashSet<>(flatMap.keySet()))
                        .originalType(OriginalNodeType.SUBQUERY)
                        .build();
                DataBlood.getBranches().put(buildBranchKey(subqBranch), subqBranch);
            }
            return DataBlood;
        }

        // 分支4：解析Filter（过滤层，直接穿透）→ 完全保留原有逻辑，传递分支标识
        if (plan instanceof Filter) {
            Filter filterPlan = (Filter) plan;
            // 递归解析Filter的子节点，传递当前分支标识
            return parser(filterPlan.child(), currentBranchKey);
        }

        // 分支5：解析UnresolvedRelation（原始表，最终源头）→ 完全保留原有逻辑
        if (plan instanceof UnresolvedRelation) {
            UnresolvedRelation relationPlan = (UnresolvedRelation) plan;
            // 提取库名和表名（适配Spark SQL的UnresolvedRelation）
            String fullTableName = parseUnresolvedRelationName(relationPlan);
            // 原始表是LOGICAL类型，无源头（source为空）
        }

        // 分支6：解析LogicalRelation（真实物理表）→ 仅新增初始化溯源标记和分支标识
        if (plan instanceof LogicalRelation) {
            LogicalRelation relationPlan = (LogicalRelation) plan;
            Seq<AttributeReference> output = relationPlan.output();
            List<String> attrNameList = new ArrayList<>();
            Option<CatalogTable> catalogTableOption = relationPlan.catalogTable();
            if (catalogTableOption.isEmpty()) {
                return null;
            }
            String dbName = catalogTableOption.get().database();
            String TableName = catalogTableOption.get().identifier().table();
            Iterator<AttributeReference> iterator = output.iterator();
            while (iterator.hasNext()) {
                AttributeReference attr = iterator.next();
                attrNameList.add(attr.name());
            }

            // 初始化真实表分支信息：标记为REAL_TABLE，初始别名=表名
            Set<String> fields = new HashSet<>(attrNameList);
            BranchBlood realTableBranch = BranchBlood.builder()
                    .dbName(dbName)
                    .tblName(TableName)
                    .tblAlias(TableName) // 初始别名=原始表名
                    .fields(fields)
                    .originalType(OriginalNodeType.REAL_TABLE) // 标记为真实表
                    .build();
            // 构建初始分支标识（db.tbl）
            String initBranchKey = buildBranchKey(realTableBranch);
            DataBlood dataBlood = new DataBlood();
            dataBlood.getBranches().put(initBranchKey, realTableBranch);
            return dataBlood;
        }

        // 分支7：解析Aggregate（聚合层）→ 完全保留原有逻辑，传递分支标识
        if (plan instanceof Aggregate) {
            Aggregate aggregatePlan = (Aggregate) plan;
            Seq<NamedExpression> namedExpressionSeq = aggregatePlan.aggregateExpressions();
            Iterator<NamedExpression> iterator = namedExpressionSeq.iterator();
            Map<String, Set<String>> flatMap = new HashMap<>();
            while (iterator.hasNext()) {
                NamedExpression next = iterator.next();
                Set<String> strings = AliasUtils.parseNamedExprDependencies(next);
                flatMap.put(next.name(), strings);
            }
            DataBlood DataBlood = parser(aggregatePlan.child(), currentBranchKey);
            if (DataBlood == null) {
                DataBlood = new DataBlood();
            }
            DataBlood.setColumnFlat(flatMap);
            return DataBlood;
        }

        // 分支8：解析Join（多表关联）→ 完全保留原有逻辑，分支标识传null（各自初始化）
        if (plan instanceof Join) {
            Join joinPlan = (Join) plan;
            DataBlood leftDataBlood = parser(joinPlan.left(), null);
            DataBlood rightDataBlood = parser(joinPlan.right(), null);
            // 合并左右分支（保留原有逻辑，可根据需要补充合并规则）
            if (leftDataBlood != null && rightDataBlood != null) {
                DataBlood joinBlood = new DataBlood();
                joinBlood.getBranches().putAll(leftDataBlood.getBranches());
                joinBlood.getBranches().putAll(rightDataBlood.getBranches());
                joinBlood.getColumnFlat().putAll(leftDataBlood.getColumnFlat());
                joinBlood.getColumnFlat().putAll(rightDataBlood.getColumnFlat());
                joinBlood.getAliasTraceMap().putAll(leftDataBlood.getAliasTraceMap());
                joinBlood.getAliasTraceMap().putAll(rightDataBlood.getAliasTraceMap());
                return joinBlood;
            }
            return leftDataBlood != null ? leftDataBlood : rightDataBlood;
        }

        return null;
    }

    // ====================== 新增：构建分支唯一标识（统一格式）======================
    private static String buildBranchKey(BranchBlood branch) {
        // 真实表：dbName.tblAlias（如hdp_db.t_user_alias）
        // CTE/子查询：ORIGINAL_TYPE_alias（如CTE_cte_user_alias、SUBQUERY_subq_alias）
        if (OriginalNodeType.REAL_TABLE.equals(branch.getOriginalType()) && branch.getDbName() != null) {
            return branch.getDbName() + "." + branch.getTblAlias();
        } else {
            return branch.getOriginalType().name() + "_" + branch.getTblAlias();
        }
    }

    // ====================== 新增：别名溯源工具方法（根据别名找原始节点）======================
    // 支持多层别名溯源：如a→b→t_user，传入a返回原始t_user的分支信息
    public static BranchBlood traceOriginalBranch(String alias, DataBlood dataBlood) {
        if (dataBlood == null || alias == null || dataBlood.getBranches().isEmpty()) {
            return null;
        }
        // 1. 递归追溯原始标识
        String currentAlias = alias;
        while (dataBlood.getAliasTraceMap().containsKey(currentAlias)) {
            currentAlias = dataBlood.getAliasTraceMap().get(currentAlias);
        }
        // 2. 根据原始标识找分支信息
        for (Map.Entry<String, BranchBlood> entry : dataBlood.getBranches().entrySet()) {
            BranchBlood branch = entry.getValue();
            // 匹配原始标识（分支key/原始表名/当前别名）
            if (entry.getKey().equals(currentAlias)
                    || branch.getTblName().equals(currentAlias)
                    || branch.getTblAlias().equals(currentAlias)) {
                return branch;
            }
        }
        return null;
    }

    // 原有方法：parseProjectFields → 完全保留
    private static Map<String, String> parseProjectFields(Project projectPlan) {
        Map<String, String> map = new HashMap<>();
        // 遍历Project中的所有NamedExpression（字段）
        for (NamedExpression expr : scala.collection.JavaConverters.seqAsJavaList(projectPlan.projectList())) {
            // 1. 提取字段别名（最终展示的字段名）
            if (expr instanceof Alias) {
                Alias alias = (Alias) expr;
                String fieldAlias = alias.sql();
                String[] split = fieldAlias.split("AS");
                map.put(split[0].trim(), split[1].trim());
                System.out.println(fieldAlias);
            }
            if (expr instanceof UnresolvedAttribute) {
                UnresolvedAttribute alias = (UnresolvedAttribute) expr;
                String fieldAlias = alias.name();
                map.put(fieldAlias, fieldAlias);
                System.out.println(fieldAlias);
            }
            // 4. 为每个依赖字段构建源节点（后续关联到原始表）
        }
        return map;
    }

    // 原有方法：parseUnresolvedRelationName → 完全保留
    private static String parseUnresolvedRelationName(UnresolvedRelation relationPlan) {
        // 适配Spark SQL的UnresolvedRelation获取库表名逻辑
        String parts = relationPlan.tableName();
        return parts;
    }

    // 原有方法：extractDependFields → 完全保留
    private static List<String> extractDependFields(String expr) {
        List<String> dependFields = new ArrayList<>();
        if (expr == null || expr.isEmpty()) {
            return dependFields;
        }
        // 正则1：匹配单引号/反引号包裹的字段名（'dt' / `dt`）
        Pattern pattern1 = Pattern.compile("['`]([a-zA-Z0-9_]+)['`]");
        Matcher matcher1 = pattern1.matcher(expr);
        while (matcher1.find()) {
            String field = matcher1.group(1);
            if (!isSqlKeyword(field)) {
                dependFields.add(field);
            }
        }
        // 正则2：匹配无包裹的直接字段名（适用于简单字段如dt、brand_name）
        if (dependFields.isEmpty() && !expr.contains("(") && !expr.contains("CASE")) {
            dependFields.add(expr.trim());
        }
        // 去重
        return new ArrayList<>(new LinkedHashSet<>(dependFields));
    }

    // 原有方法：isSqlKeyword → 完全保留
    private static boolean isSqlKeyword(String str) {
        Set<String> keywords = new HashSet<>(Arrays.asList(
                // 日期函数
                "yyyy-MM-dd", "current_date", "date_sub", "date_format", "to_date", "weekofyear",
                // 字符串函数
                "concat_ws", "concat", "substr", "trim",
                // 条件关键字
                "CASE", "WHEN", "THEN", "ELSE", "END", "high_end", "mid_low",
                // 聚合函数
                "sum", "count", "avg", "max", "min"
        ));
        return keywords.contains(str.toUpperCase()) || keywords.contains(str);
    }

    // 原有注释方法：全部保留
//    private static Map<String,Set<String>> handleColumn(Map<String,Set<String>> map, DataBlood DataBlood) {
//        List<String> sources = DataBlood.getSources();
//        Map<String,String> flat = new HashMap();
//        map.entrySet().forEach(e->{
//            if(sources.contains(e.getKey())||sources.contains(e.getKey().replace(DataBlood.currentTableName+".",""))) {
//                flat.put(e.getValue(),e.getKey().replace(DataBlood.currentTableName+".",""));
//            }
//            else {
//                flat.put(e.getValue(),null);
//            }
//        });
//        return flat;
//    }

    // 原有main方法：完全保留（仅在最后添加血缘结果打印示例）
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
        // 本地开发：强制指定Hadoop用户（与远程Hive集群的操作用户一致，如hadoop）
//        System.setProperty("HADOOP_USER_NAME", "hadoop");
        // 禁用Hive元数据本地缓存，强制走远程（本地开发必配）
//        System.setProperty("hive.metastore.cache.pinobjtypes", "NONE");
//        System.setProperty("hive.metastore.cache.expireAfter", "0s");

        // 构建SparkSession：本地解析血缘专属配置
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
            DataBlood bloodResult = parser(resolvedPlan);
            System.out.println("===== 4. 血缘解析结果 =====");
            // 新增：打印分支信息和别名溯源
            if (bloodResult != null) {
                // 打印所有分支信息
                System.out.println("★ 所有分支信息：");
                bloodResult.getBranches().forEach((key, branch) -> {
                    System.out.printf("分支标识：%s | 原始类型：%s | 原始表名：%s | 当前别名：%s | 字段：%s%n",
                            key, branch.getOriginalType(), branch.getTblName(), branch.getTblAlias(), branch.getFields());
                });
                // 打印别名溯源映射
                if (!bloodResult.getAliasTraceMap().isEmpty()) {
                    System.out.println("\n★ 别名溯源映射（新别名→原始/上一级标识）：");
                    bloodResult.getAliasTraceMap().forEach((newAlias, original) -> {
                        System.out.printf("%s → %s%n", newAlias, original);
                        // 溯源原始分支
                        BranchBlood originalBranch = traceOriginalBranch(newAlias, bloodResult);
                        if (originalBranch != null) {
                            System.out.printf("  → 原始节点：%s（%s）%n", originalBranch.getTblName(), originalBranch.getOriginalType());
                        }
                    });
                }
                // 打印字段映射
                if (!bloodResult.getColumnFlat().isEmpty()) {
                    System.out.println("\n★ 字段映射（当前字段→源字段）：");
                    bloodResult.getColumnFlat().forEach((col, sourceCols) -> {
                        System.out.printf("%s → %s%n", col, sourceCols);
                    });
                }
            }

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