package org.example.utils;

import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.Average;
import org.apache.spark.sql.catalyst.expressions.aggregate.Count;
import scala.collection.Seq;
import scala.collection.JavaConverters;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Alias 解析工具类：全覆盖提取 Alias 依赖的原始字段名
 * 最终修正：移除不存在的 Distinct 类、修复Scala Seq遍历、完善Count/Avg解析、空值安全
 * 支持：普通字段、Count/Avg聚合、聚合内DISTINCT、嵌套表达式、多字段运算、类型转换等所有场景
 * 兼容：Spark 2.x/3.x 所有版本，无不存在类引用
 */
public class AliasUtils {

    /**
     * 对外核心方法：解析 Alias 对应的表达式所依赖的原始字段名
     * @param alias 待解析的 Alias 实例
     * @return 去重后的原始字段名集合，空输入返回空集合
     */
    public static Set<String> parseAliasDependencies(Alias alias) {
        if (alias == null || alias.child() == null) {
            return new HashSet<>();
        }
        return extractAttributes(alias.child());
    }

    /**
     * 递归核心方法：遍历表达式树，提取所有原始字段（Attribute/AttributeReference）
     * 通用递归逻辑自动处理聚合内DISTINCT，无需单独Distinct类判断
     * @param expr 待解析的表达式节点
     * @return 去重后的原始字段名集合
     */
    private static Set<String> extractAttributes(Expression expr) {
        Set<String> attributes = new HashSet<>();
        if (expr == null) {
            return attributes;
        }

        // 1. 核心：原始表字段标识（Attribute/AttributeReference，包含所有表字段引用）
        if (expr instanceof Attribute) {
            attributes.add(((Attribute) expr).name());
            return attributes;
        }

        // 2. 聚合表达式包装：解包获取内部真实聚合函数（Count/Avg/Sum等，Spark聚合标准包装）
        if (expr instanceof AggregateExpression) {
            AggregateExpression aggExpr = (AggregateExpression) expr;
            attributes.addAll(extractAttributes(aggExpr.aggregateFunction()));
            return attributes;
        }

        // 3. 平均值聚合：Avg/Average 专属处理，直接解析其子节点
        if (expr instanceof Average) {
            Average average = (Average) expr;
            attributes.addAll(extractAttributes(average.child()));
            return attributes;
        }

        // 4. 计数聚合：Count 专属处理（支持Count(*)、Count(col)、Count(DISTINCT col)、Count(col1,col2)）
        if (expr instanceof Count) {
            Count count = (Count) expr;
            attributes.addAll(extractCountFields(count));
            return attributes;
        }

        // 5. 嵌套别名：Alias内部嵌套Alias，递归解包到原始字段
        if (expr instanceof Alias) {
            Alias nestedAlias = (Alias) expr;
            attributes.addAll(extractAttributes(nestedAlias.child()));
            return attributes;
        }

        // 6. 通用表达式：递归遍历所有子节点（自动处理DISTINCT、cast、+/*、CheckOverflow等所有嵌套）
        // 核心修复：Scala Seq → Java List，解决Foreach not applicable编译报错
        Seq<Expression> scalaChildren = expr.children();
        if (scalaChildren != null && !scalaChildren.isEmpty()) {
            List<Expression> javaChildren = JavaConverters.seqAsJavaList(scalaChildren);
            for (Expression childExpr : javaChildren) {
                attributes.addAll(extractAttributes(childExpr));
            }
        }

        return attributes;
    }

    /**
     * Count聚合专属解析：适配所有Count形态，通用递归自动处理Count(DISTINCT col)
     * @param count Spark原生Count聚合实例
     * @return Count依赖的原始字段名集合，Count(*)返回空集合
     */
    private static Set<String> extractCountFields(Count count) {
        Set<String> fieldNames = new HashSet<>();
        Seq<Expression> countChildren = count.children();
        if (countChildren == null || countChildren.isEmpty()) {
            return fieldNames; // Count(*) 无入参，返回空集合
        }

        // 转换Scala Seq为Java List，安全遍历Count所有入参（修复遍历报错）
        List<Expression> javaChildren = JavaConverters.seqAsJavaList(countChildren);
        for (Expression expr : javaChildren) {
            fieldNames.addAll(extractAttributes(expr)); // 递归处理，自动解析DISTINCT内字段
        }
        return fieldNames;
    }

    /**
     * 便捷工具方法：与原有调用习惯完全兼容，直接解析Alias来源字段
     * @param alias 待解析Alias实例
     * @return 原始字段名集合
     */
    public static Set<String> parser(Alias alias) {
        return parseAliasDependencies(alias);
    }
}