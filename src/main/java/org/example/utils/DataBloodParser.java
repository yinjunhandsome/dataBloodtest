package org.example.utils;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.Analyzer;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.expressions.aggregate.*;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.*;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DataBloodParser {

    private static  class fieldBlood{
        //CTE字段来源，key是cte的库名.表名.字段名，value是真实存在的库名.表名.字段名
        // 后续解析到select cte的时候，到这个map里面取相应的真实来源.
        private Map<String,String> CTEMap;
        //用于存储全局的默认数据库，比如脚步使用了use default;
        private String defaultDb;
        //用于脚本里面新建的表，因为脚步里面的create还没有正式执行，会导致hive没有这部分元数据
        //最终血缘解析结果，key为输入，value为输出，insert是key和value都不为空，select为value为空
        private Map<String,String> fieldMap;
    }

    // 抽象后的节点类型：仅区分「逻辑单元」和「字段血缘单元」
    public enum NodeType {
        LOGICAL,  // 纯逻辑单元（CTE、物理表(dbname.tablename)等，无具体字段）
        FIELD     // 字段血缘单元（关联dbname.tablename.columnname完整标识）
    }

    public static class FieldLineage {
        private NodeType nodeType;                // 节点类型（LOGICAL/FIELD）
        private String logicalName;               // 逻辑单元名称：CTE名 / 表全名(dbname.tablename)
        private String fullFieldName;             // 完整字段标识：dbname.tablename.columnname（仅FIELD节点有效）
        private List<FieldLineage> source;        // 源节点列表（递归溯源）

        // ========== 构造器：按节点类型区分 ==========
        // 1. 逻辑单元构造器（CTE/表）
        public FieldLineage(String logicalName, List<FieldLineage> source) {
            this.nodeType = NodeType.LOGICAL;
            this.logicalName = logicalName;
            this.source = source == null ? new ArrayList<>() : source;
        }

        // 2. 字段单元构造器（完整字段标识）
        public FieldLineage(NodeType nodeType, String fullFieldName, List<FieldLineage> source) {
            if (nodeType != NodeType.FIELD) {
                throw new IllegalArgumentException("仅FIELD节点可使用此构造器");
            }
            this.nodeType = NodeType.FIELD;
            this.fullFieldName = fullFieldName;
            this.source = source == null ? new ArrayList<>() : source;
        }

        // ========== Getter ==========
        public NodeType getNodeType() { return nodeType; }
        public String getLogicalName() { return logicalName; }
        public String getFullFieldName() { return fullFieldName; }
        public List<FieldLineage> getSource() { return source; }
    }

    public static FieldLineage parser(LogicalPlan plan) {
        List<FieldLineage> source = new ArrayList<>();
        if (plan instanceof UnresolvedWith) {
            UnresolvedWith withPlan = (UnresolvedWith) plan;
            // 解析CTE定义
            Seq<Tuple2<String, SubqueryAlias>> ctes = withPlan.cteRelations();
            Iterator<Tuple2<String, SubqueryAlias>> cteIter = ctes.iterator();
            while (cteIter.hasNext()) {
                Tuple2<String, SubqueryAlias> cte = cteIter.next();
                LogicalPlan ctePlan = cte._2();
                // 解析单个CTE的字段映射
                source.add(parser(ctePlan));
            }
            return new FieldLineage(withPlan.nodeName(),source);
        }

        if (plan instanceof WithCTE) {
            WithCTE withPlan = (WithCTE) plan;
            // 解析CTE定义
            Seq<CTERelationDef> ctes = withPlan.cteDefs();
            LogicalPlan query = withPlan.plan();
            Iterator<CTERelationDef> iterator = ctes.iterator();
            while (iterator.hasNext()) {
                CTERelationDef next = iterator.next();
                LogicalPlan child = next.child();
                // 解析单个CTE的字段映射
                source.add(parser(child));
            }
            return new FieldLineage(withPlan.nodeName(),source);
        }

        // 分支2：解析SubqueryAlias（子查询别名）
        if (plan instanceof SubqueryAlias) {
            SubqueryAlias aliasPlan = (SubqueryAlias) plan;
            // 递归解析别名对应的子计划
            FieldLineage childLineage = parser(aliasPlan.child());
            if (childLineage != null) {
                source.add(childLineage);
            }
            // SubqueryAlias是LOGICAL类型，名称为别名
            return new FieldLineage("SubqueryAlias_" + aliasPlan.alias(), source);
        }

        // 分支3：解析Project（字段投影/计算层，核心）
        if (plan instanceof Project) {
            Project projectPlan = (Project) plan;
            // 解析Project中的所有字段
            List<FieldLineage> fieldLineages = parseProjectFields(projectPlan);
            // 递归解析Project的子节点（Filter/UnresolvedRelation等）
            FieldLineage childLineage = parser(projectPlan.child());
            // 给字段节点关联源表/上游字段
            fieldLineages.forEach(field -> {
                if (childLineage != null) {
                    field.getSource().addAll(childLineage.getSource());
                }
            });
            // Project节点是LOGICAL类型，子节点为所有字段
            return new FieldLineage("Project", fieldLineages);
        }

        // 分支4：解析Filter（过滤层，直接穿透）
        if (plan instanceof Filter) {
            Filter filterPlan = (Filter) plan;
            // 递归解析Filter的子节点，Filter本身不生成新节点，直接返回子节点结果
            return parser(filterPlan.child());
        }

        // 分支5：解析UnresolvedRelation（原始表，最终源头）
        if (plan instanceof UnresolvedRelation) {
            UnresolvedRelation relationPlan = (UnresolvedRelation) plan;
            // 提取库名和表名（适配Spark SQL的UnresolvedRelation）
            String fullTableName = parseUnresolvedRelationName(relationPlan);
            // 原始表是LOGICAL类型，无源头（source为空）
            return new FieldLineage("Table_" + fullTableName, new ArrayList<>());
        }

        return null;
    }

    private static List<FieldLineage> parseProjectFields(Project projectPlan) {
        List<FieldLineage> fieldLineages = new ArrayList<>();
        // 遍历Project中的所有NamedExpression（字段）
        for (NamedExpression expr : scala.collection.JavaConverters.seqAsJavaList(projectPlan.projectList())) {
            // 1. 提取字段别名（最终展示的字段名）
            String fieldAlias = expr.name();
            // 2. 提取字段原始表达式（SQL字符串）
//            String fieldExpr = expr.sql();
            // 3. 提取表达式依赖的源字段名
            List<String> dependFieldNames = extractDependFields(fieldAlias);
            // 4. 为每个依赖字段构建源节点（后续关联到原始表）
            List<FieldLineage> dependFields = dependFieldNames.stream()
                    .map(fieldName -> new FieldLineage(NodeType.FIELD, fieldName, new ArrayList<>()))
                    .collect(Collectors.toList());
            // 5. 构建当前字段的FIELD节点
            FieldLineage fieldLineage = new FieldLineage(NodeType.FIELD, fieldAlias, dependFields);
            fieldLineages.add(fieldLineage);
        }
        return fieldLineages;
    }
    /**
     * 私有方法：解析UnresolvedRelation的库表名，拼接成db.table格式
     */
    private static String parseUnresolvedRelationName(UnresolvedRelation relationPlan) {
        // 适配Spark SQL的UnresolvedRelation获取库表名逻辑
        String parts = relationPlan.tableName();

//        List<String> nameParts = scala.collection.JavaConverters.seqAsJavaList(parts);
//        // 场景1：db.table → 拼接
//        if (nameParts.size() >= 2) {
//            return nameParts.get(nameParts.size() - 2) + "." + nameParts.get(nameParts.size() - 1);
//        }
        // 场景2：仅表名 → 直接返回
        return parts;
    }

    /**
     * 私有方法：从字段表达式中提取依赖的源字段名（正则匹配）
     */
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

    /**
     * 私有方法：过滤SQL关键字/函数名，只保留字段名
     */
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

    // ====================== 辅助方法：提取指定字段的血缘路径 ======================
    /**


     /**
     * 解析单个CTE的字段依赖映射
     * @param ctePlan CTE的逻辑计划
     * @param cteName CTE名称
     * @return 字段名 -> 依赖字段列表
     */
//    private static Map<String, List<String>> parseCTEColumnDeps(LogicalPlan ctePlan, String cteName) {
//        Map<String, List<String>> columnDeps = new HashMap<>();
//
//        // 递归找到Project节点（SELECT后的字段）
//        LogicalPlan current = ctePlan;
//        while (current != null && !(current instanceof Project)) {
//            if (current.children().nonEmpty()) {
//                current = JavaConverters.seqAsJavaList(current.children()).get(0);
//            } else {
//                break;
//            }
//        }
//
//        if (current instanceof Project) {
//            Project projectPlan = (Project) current;
//            List<NamedExpression> projectExprs = JavaConverters.seqAsJavaList(projectPlan.projectList());
//
//            for (NamedExpression expr : projectExprs) {
//                String colName = expr.name();
//                List<String> deps = new ArrayList<>();
//
//                if (expr instanceof Alias ||expr instanceof UnresolvedAlias) {
//                    // 带别名的表达式字段（如a/b AS rate）
//                    Alias alias = (Alias) expr;
//                    deps = extractColumnNames(alias.child());
//                } else if (expr instanceof AttributeReference ||expr instanceof UnresolvedAttribute) {
//                    // 直接引用的字段（如qc_code）
//                    deps.add(expr.name());
//                }
//
//                columnDeps.put(colName, deps);
//            }
//        }
//
//        return columnDeps;
//    }
//
//    /**
//     * 提取表达式中的字段名
//     * @param expr 表达式（如SUM(real_pay_price)、a/b）
//     * @return 字段名列表
//     */
//    private static List<String> extractColumnNames(Expression expr) {
//        List<String> columnNames = new ArrayList<>();
//
//        if (expr instanceof AttributeReference) {
//            // 基础字段引用
//            columnNames.add(((AttributeReference) expr).name());
//        }
//        else if (expr instanceof UnresolvedAttribute) {
//            // 基础字段引用
//            columnNames.add(((UnresolvedAttribute) expr).name());
//        }
//
//        else if (expr instanceof AggregateFunction) {
//            // 聚合函数（如COUNT/SUM）
//            AggregateFunction aggFunc = (AggregateFunction) expr;
//            for (Expression child : JavaConverters.seqAsJavaList(aggFunc.children())) {
//                columnNames.addAll(extractColumnNames(child));
//            }
//        } else if (expr instanceof BinaryExpression) {
//            // 二元表达式（如a + b、a / b）
//            BinaryExpression binaryExpr = (BinaryExpression) expr;
//            columnNames.addAll(extractColumnNames(binaryExpr.left()));
//            columnNames.addAll(extractColumnNames(binaryExpr.right()));
//        } else if (expr instanceof UnaryExpression) {
//            // 一元表达式（如NOT a）
//            UnaryExpression unaryExpr = (UnaryExpression) expr;
//            columnNames.addAll(extractColumnNames(unaryExpr.child()));
//        } else if (expr instanceof CaseWhen) {
//            // CASE WHEN表达式
//            CaseWhen caseWhen = (CaseWhen) expr;
//            for (Expression child : JavaConverters.seqAsJavaList(caseWhen.children())) {
//                columnNames.addAll(extractColumnNames(child));
//            }
//        } else if (expr instanceof ScalarSubquery) {
//            // 子查询（简化处理）
//            columnNames.add("subquery_fields");
//        } else {
//            // 遍历所有子节点
//            for (Expression child : JavaConverters.seqAsJavaList(expr.children())) {
//                columnNames.addAll(extractColumnNames(child));
//            }
//        }
//
//        // 去重
//        return columnNames.stream().distinct().collect(Collectors.toList());
//    }
//
//    /**
//     * 追溯字段的完整血缘链路
//     * @param fields 待追溯的字段列表
//     * @param source 当前数据源（CTE/表名）
//     * @return 完整血缘链路
//     */
//    private static List<String> traceFullLineage(List<String> fields, String source) {
//        List<String> fullLineage = new ArrayList<>();
//
//        for (String field : fields) {
//            if (CTE_COLUMN_MAP.containsKey(source)) {
//                Map<String, List<String>> sourceDeps = CTE_COLUMN_MAP.get(source);
//                if (sourceDeps.containsKey(field)) {
//                    List<String> parentFields = sourceDeps.get(field);
//                    // 递归追溯父级依赖（适配多层CTE）
//                    if ("product_sales_join".equals(source)) {
//                        // 解析product_sales_join的依赖源（gp/so/pd）
//                        for (String parentField : parentFields) {
//                            if (parentField.startsWith("gp.")) {
//                                String gpField = parentField.substring(3);
//                                fullLineage.add("grade_s_products." + gpField + " -> product_sales_join." + field);
//                            } else if (parentField.startsWith("so.")) {
//                                String soField = parentField.substring(3);
//                                fullLineage.add("sales_orders." + soField + " -> product_sales_join." + field);
//                            } else if (parentField.startsWith("pd.")) {
//                                String pdField = parentField.substring(3);
//                                fullLineage.add("product_details." + pdField + " -> product_sales_join." + field);
//                            } else {
//                                fullLineage.add(source + "." + field);
//                            }
//                        }
//                    } else {
//                        // 追溯到原始表
//                        fullLineage.add(source + "." + field + " -> 原始表字段");
//                    }
//                } else {
//                    fullLineage.add(source + "." + field);
//                }
//            } else {
//                fullLineage.add("原始表." + field);
//            }
//        }
//
//        return fullLineage;
//    }

    /**
     * 获取表达式类型（聚合/普通/条件等）
     * @param expr 表达式
     * @return 类型描述
     */
    private static String getExpressionType(Expression expr) {
        if (expr instanceof Count) {
            return "COUNT聚合字段";
        } else if (expr instanceof Sum) {
            return "SUM聚合字段";
        } else if (expr instanceof Max) {
            return "MAX聚合字段";
        } else if (expr instanceof Min) {
            return "MIN聚合字段";
        } else if (expr instanceof CaseWhen) {
            return "条件表达式字段";
        } else if (expr instanceof BinaryExpression) {
            return "算术表达式字段";
        } else if (expr instanceof WindowExpression) {
            return "窗口函数字段";
        } else {
            return "普通字段";
        }
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
        String sql="WITH\n" +
                "-- 【CTE1：基础用户表筛选】单表过滤，提取有效用户（注册日期+等级过滤），基础字段重命名\n" +
                "cte_base_user AS (\n" +
                "    SELECT\n" +
                "        user_id AS uid,\n" +
                "        user_name AS uname,\n" +
                "        user_level AS ulevel,\n" +
                "        register_time AS reg_time,\n" +
                "        register_date AS reg_date\n" +
                "    FROM t_user\n" +
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
            FieldLineage rootLineage = parser(resolvedPlan);
            System.out.println("===== 4. 血缘解析结果 =====");
            System.out.println(rootLineage);
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
