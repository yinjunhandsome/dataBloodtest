package org.example;


import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias;
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.analysis.UnresolvedHaving;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.expressions.aggregate.*;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.*;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Hello world!
 *
 */
//字段级别血缘，回溯datax血缘，推动数据接入字段级别的数据治理
public class App 
{
    // 存储CTE的字段映射: CTE名称 -> (字段名 -> 依赖字段列表)
    private static final Map<String, Map<String, List<String>>> CTE_COLUMN_MAP = new HashMap<>();
    // 最终的字段血缘结果
    private static final List<ColumnLineage> FINAL_LINEAGE = new ArrayList<>();

    /**
     * 字段血缘实体类
     */
    public static class ColumnLineage {
        private String columnName;        // 字段名称（别名）
        private String columnType;        // 字段类型（普通/聚合/表达式/关联）
        private List<String> directDeps;  // 直接依赖字段 (格式：表/CTE名.字段名)
        private List<String> fullLineage; // 完整血缘链路
        private String source;            // 所属表/CTE

        public ColumnLineage(String columnName, String columnType, List<String> directDeps, List<String> fullLineage, String source) {
            this.columnName = columnName;
            this.columnType = columnType;
            this.directDeps = directDeps;
            this.fullLineage = fullLineage;
            this.source = source;
        }

        // Getter & toString
        @Override
        public String toString() {
            return String.format(
                    "字段名: %s\n类型: %s\n直接依赖: %s\n完整血缘: %s\n所属源: %s\n%s",
                    columnName,
                    columnType,
                    String.join(", ", directDeps),
                    String.join(" -> ", fullLineage),
                    source,
                    "----------------------------------------"
            );
        }

        public String getColumnName() { return columnName; }
        public String getColumnType() { return columnType; }
        public List<String> getDirectDeps() { return directDeps; }
        public List<String> getFullLineage() { return fullLineage; }
        public String getSource() { return source; }
    }
    public static void main( String[] args ) throws ParseException {
        String sql="WITH grade_s_products AS (SELECT qc_code, brand_id, brand_name, model_id, model_name, dt AS product_dt, weekofyear(to_date(dt, 'yyyy-MM-dd')) AS product_week, concat_ws('-', brand_name, model_name) AS full_product_name, CASE WHEN buying_price > 5000 THEN 'high_end' ELSE 'mid_low' END AS price_grade FROM hdp_zhuanzhuan_rawdb_global.raw_mysql_dbzz_bmskyway_t_skyway_product_full_1d WHERE dt >= date_format(date_sub(current_date(), 180), 'yyyy-MM-dd') AND dt <= date_format(current_date(), 'yyyy-MM-dd') AND grade = 'S' AND brand_name NOT IN ('测试品牌', '未知品牌') AND isnotnull(qc_code)), sales_orders AS (SELECT info_id, qc_code, total_amt / 100 AS real_pay_price, to_date(pay_time, 'yyyy-MM-dd HH:mm:ss') AS pay_date, weekofyear(to_date(pay_time, 'yyyy-MM-dd HH:mm:ss')) AS pay_week, row_number() OVER (PARTITION BY qc_code ORDER BY pay_time) AS sales_seq, IF(total_amt >= 10000, 'big_order', 'normal_order') AS order_level FROM hdp_ubu_zhuanzhuan_dw_b2c.dw_trade_order_ord_all_subject_dtl_full_1d WHERE dt = date_format(current_date(), 'yyyy-MM-dd') AND cate_first_id = 101 AND company_flag = 1 AND isnotnull(pay_time)), product_details AS (SELECT info_id, qc_code, spec_ram, spec_version, spec_appearance_quality, spec_function_quality, hand_price, dt AS detail_dt FROM hdp_zhuanzhuan_dw_global.dw_info_prod_detail_full_1d WHERE dt BETWEEN date_format(date_sub(current_date(), 180), 'yyyy-MM-dd') AND date_format(current_date(), 'yyyy-MM-dd') AND status = 1 AND sale_where = 1 AND spec_machine_source != 'BS机' AND is_searchable = 1), product_sales_join AS (SELECT gp.brand_id, gp.brand_name, gp.model_id, gp.model_name, gp.full_product_name, gp.price_grade, so.real_pay_price, so.pay_week, so.order_level, pd.spec_ram, pd.spec_version, pd.spec_appearance_quality, pd.hand_price AS product_list_price, ROUND((so.real_pay_price / pd.hand_price) * 100, 2) AS discount_rate FROM grade_s_products gp INNER JOIN sales_orders so ON gp.qc_code = so.qc_code LEFT JOIN product_details pd ON so.info_id = pd.info_id AND gp.qc_code = pd.qc_code WHERE so.real_pay_price > 0 AND pd.hand_price > 0) SELECT brand_name, model_name, pay_week, price_grade, spec_ram, spec_version, COUNT(DISTINCT so.info_id) AS order_count, SUM(real_pay_price) AS total_sales_amount, AVG(real_pay_price) AS avg_sales_price, MAX(real_pay_price) AS max_sales_price, MIN(real_pay_price) AS min_sales_price, AVG(discount_rate) AS avg_discount_rate, COUNT(CASE WHEN order_level = 'big_order' THEN 1 END) AS big_order_count, concat_ws('/', spec_ram, spec_version) AS product_config FROM product_sales_join GROUP BY brand_name, model_name, pay_week, price_grade, spec_ram, spec_version HAVING order_count >= 5 ORDER BY pay_week DESC, total_sales_amount DESC LIMIT 100;\n";
//        String sql="SELECT t.id FROM (SELECT t.id FROM dws.sales_order t WHERE t.dt = '2026-01-06') t";
        SparkSession spark = SparkSession.builder()
                .appName("LogicalPlanParser")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();;
        LogicalPlan parsedPlan = spark.sessionState().sqlParser().parsePlan(sql);
        analyzeColumnLineage(parsedPlan);
        System.out.println(FINAL_LINEAGE);
    }

    /**
     * 递归解析逻辑计划的字段血缘
     * @param plan 逻辑计划节点
     */
    public static void analyzeColumnLineage(LogicalPlan plan) {
        // 1. 处理CTE节点（UnresolvedWith）
        if (plan instanceof UnresolvedWith) {
            UnresolvedWith withPlan = (UnresolvedWith) plan;
            // 解析CTE定义
            Seq<Tuple2<String, SubqueryAlias>> ctes = withPlan.cteRelations();
            Iterator<Tuple2<String, SubqueryAlias>> cteIter = ctes.iterator();

            while (cteIter.hasNext()) {
                Tuple2<String, SubqueryAlias> cte = cteIter.next();
                String cteName = cte._1();
                LogicalPlan ctePlan = cte._2();
                // 解析单个CTE的字段映射
                Map<String, List<String>> cteColumnDeps = parseCTEColumnDeps(ctePlan, cteName);
                CTE_COLUMN_MAP.put(cteName, cteColumnDeps);
            }
            // 解析CTE之后的主查询逻辑
            analyzeColumnLineage(withPlan.child());
        }

        else if (plan instanceof Join) {
            analyzeColumnLineage(((Join) plan).left());
            analyzeColumnLineage(((Join) plan).right());
        }

        // 2. 处理Limit节点（GlobalLimit/LocalLimit）
        else if (plan instanceof GlobalLimit || plan instanceof LocalLimit) {
            analyzeColumnLineage(((UnaryNode) plan).child());
        }

        // 3. 处理Sort节点
        else if (plan instanceof Sort) {
            analyzeColumnLineage(((Sort) plan).child());
        }

        // 4. 处理Having节点（UnresolvedHaving）
        else if (plan instanceof UnresolvedHaving) {
            UnresolvedHaving havingPlan = (UnresolvedHaving) plan;
            analyzeColumnLineage(havingPlan.child());
        }

        // 5. 处理聚合节点（Aggregate）- 核心：解析聚合字段依赖
        else if (plan instanceof Aggregate) {
            Aggregate aggPlan = (Aggregate) plan;
            // 解析分组字段
            List<Expression> groupByExprs = JavaConverters.seqAsJavaList(aggPlan.groupingExpressions());
            // 解析聚合表达式（SELECT后的字段）
            List<NamedExpression> aggExprs = JavaConverters.seqAsJavaList(aggPlan.aggregateExpressions());

            for (NamedExpression expr : aggExprs) {
                String colName = expr.name();
                List<String> directDeps = new ArrayList<>();
                String colType = "普通字段";

                // 解析聚合函数依赖（如COUNT/SUM/AVG）
                if (expr instanceof Alias) {
                    Alias alias = (Alias) expr;
                    Expression childExpr = alias.child();
                    colType = getExpressionType(childExpr);
                    // 获取聚合函数依赖的字段
                    directDeps = extractColumnNames(childExpr);
                } else if (expr instanceof AttributeReference || expr instanceof UnresolvedAttribute) {
                    // 分组字段
                    directDeps.add(expr.name());
                }

                // 追溯完整血缘（从CTE到原始表）
                List<String> fullLineage = traceFullLineage(directDeps, "product_sales_join");
                // 添加到最终结果
                FINAL_LINEAGE.add(new ColumnLineage(
                        colName,
                        colType,
                        directDeps,
                        fullLineage,
                        "product_sales_join"
                ));
            }

            // 递归解析聚合的子节点
            analyzeColumnLineage(aggPlan.child());
        }

        // 6. 处理表引用（UnresolvedRelation）
        else if (plan instanceof UnresolvedRelation) {
            UnresolvedRelation relation = (UnresolvedRelation) plan;
            String tableName=relation.tableName();
            // 原始表的字段血缘（如果需要解析原始表元数据，可扩展此处）
        }

        // 7. 处理其他节点（Project/Join/Filter等）
        else if (plan.children().nonEmpty()) {
            for (LogicalPlan child : JavaConverters.seqAsJavaList(plan.children())) {
                analyzeColumnLineage(child);
            }
        }
    }

    /**
     * 解析单个CTE的字段依赖映射
     * @param ctePlan CTE的逻辑计划
     * @param cteName CTE名称
     * @return 字段名 -> 依赖字段列表
     */
    private static Map<String, List<String>> parseCTEColumnDeps(LogicalPlan ctePlan, String cteName) {
        Map<String, List<String>> columnDeps = new HashMap<>();

        // 递归找到Project节点（SELECT后的字段）
        LogicalPlan current = ctePlan;
        while (current != null && !(current instanceof Project)) {
            if (current.children().nonEmpty()) {
                current = JavaConverters.seqAsJavaList(current.children()).get(0);
            } else {
                break;
            }
        }

        if (current instanceof Project) {
            Project projectPlan = (Project) current;
            List<NamedExpression> projectExprs = JavaConverters.seqAsJavaList(projectPlan.projectList());

            for (NamedExpression expr : projectExprs) {
                String colName = expr.name();
                List<String> deps = new ArrayList<>();

                if (expr instanceof Alias||expr instanceof UnresolvedAlias) {
                    // 带别名的表达式字段（如a/b AS rate）
                    Alias alias = (Alias) expr;
                    deps = extractColumnNames(alias.child());
                } else if (expr instanceof AttributeReference||expr instanceof UnresolvedAttribute) {
                    // 直接引用的字段（如qc_code）
                    deps.add(expr.name());
                }

                columnDeps.put(colName, deps);
            }
        }

        return columnDeps;
    }

    /**
     * 提取表达式中的字段名
     * @param expr 表达式（如SUM(real_pay_price)、a/b）
     * @return 字段名列表
     */
    private static List<String> extractColumnNames(Expression expr) {
        List<String> columnNames = new ArrayList<>();

        if (expr instanceof AttributeReference) {
            // 基础字段引用
            columnNames.add(((AttributeReference) expr).name());
        }
        else if (expr instanceof UnresolvedAttribute) {
            // 基础字段引用
            columnNames.add(((UnresolvedAttribute) expr).name());
        }

        else if (expr instanceof AggregateFunction) {
            // 聚合函数（如COUNT/SUM）
            AggregateFunction aggFunc = (AggregateFunction) expr;
            for (Expression child : JavaConverters.seqAsJavaList(aggFunc.children())) {
                columnNames.addAll(extractColumnNames(child));
            }
        } else if (expr instanceof BinaryExpression) {
            // 二元表达式（如a + b、a / b）
            BinaryExpression binaryExpr = (BinaryExpression) expr;
            columnNames.addAll(extractColumnNames(binaryExpr.left()));
            columnNames.addAll(extractColumnNames(binaryExpr.right()));
        } else if (expr instanceof UnaryExpression) {
            // 一元表达式（如NOT a）
            UnaryExpression unaryExpr = (UnaryExpression) expr;
            columnNames.addAll(extractColumnNames(unaryExpr.child()));
        } else if (expr instanceof CaseWhen) {
            // CASE WHEN表达式
            CaseWhen caseWhen = (CaseWhen) expr;
            for (Expression child : JavaConverters.seqAsJavaList(caseWhen.children())) {
                columnNames.addAll(extractColumnNames(child));
            }
        } else if (expr instanceof ScalarSubquery) {
            // 子查询（简化处理）
            columnNames.add("subquery_fields");
        } else {
            // 遍历所有子节点
            for (Expression child : JavaConverters.seqAsJavaList(expr.children())) {
                columnNames.addAll(extractColumnNames(child));
            }
        }

        // 去重
        return columnNames.stream().distinct().collect(Collectors.toList());
    }

    /**
     * 追溯字段的完整血缘链路
     * @param fields 待追溯的字段列表
     * @param source 当前数据源（CTE/表名）
     * @return 完整血缘链路
     */
    private static List<String> traceFullLineage(List<String> fields, String source) {
        List<String> fullLineage = new ArrayList<>();

        for (String field : fields) {
            if (CTE_COLUMN_MAP.containsKey(source)) {
                Map<String, List<String>> sourceDeps = CTE_COLUMN_MAP.get(source);
                if (sourceDeps.containsKey(field)) {
                    List<String> parentFields = sourceDeps.get(field);
                    // 递归追溯父级依赖（适配多层CTE）
                    if ("product_sales_join".equals(source)) {
                        // 解析product_sales_join的依赖源（gp/so/pd）
                        for (String parentField : parentFields) {
                            if (parentField.startsWith("gp.")) {
                                String gpField = parentField.substring(3);
                                fullLineage.add("grade_s_products." + gpField + " -> product_sales_join." + field);
                            } else if (parentField.startsWith("so.")) {
                                String soField = parentField.substring(3);
                                fullLineage.add("sales_orders." + soField + " -> product_sales_join." + field);
                            } else if (parentField.startsWith("pd.")) {
                                String pdField = parentField.substring(3);
                                fullLineage.add("product_details." + pdField + " -> product_sales_join." + field);
                            } else {
                                fullLineage.add(source + "." + field);
                            }
                        }
                    } else {
                        // 追溯到原始表
                        fullLineage.add(source + "." + field + " -> 原始表字段");
                    }
                } else {
                    fullLineage.add(source + "." + field);
                }
            } else {
                fullLineage.add("原始表." + field);
            }
        }

        return fullLineage;
    }

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
}
