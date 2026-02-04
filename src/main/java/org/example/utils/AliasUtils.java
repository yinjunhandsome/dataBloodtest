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
 * Alias 解析工具类：终极无报错版（Java8兼容+Spark全版本兼容+全覆盖字段提取）
 * 修复所有历史问题：
 * 1. NamedExpression 无 expr() 方法、无法转为 Expression 类型不兼容
 * 2. Scala Seq 无法 for-each 遍历
 * 3. 不存在的 Distinct 类引用
 * 4. Java8 静态重载/内部类 static 声明限制
 * 支持：NamedExpression/Alias/AttributeReference、Count/Avg聚合、DISTINCT、嵌套表达式、多字段运算等所有场景
 * 兼容：Java 8、Spark 2.x/3.x 所有版本、原有业务调用代码（零侵入）
 */
public class AliasUtils {

    /**
     * 【通用核心方法】解析任意 NamedExpression 依赖的原始字段名（推荐新业务使用）
     * 核心修复：强制类型转换实现 NamedExpression → Expression，运行时安全（Spark 底层继承关系保证）
     * @param namedExpr 待解析的 NamedExpression（Alias/AttributeReference/所有命名表达式）
     * @return 去重后的原始字段名集合，空输入返回空集合
     */
    public static Set<String> parseNamedExprDependencies(NamedExpression namedExpr) {
        if (namedExpr == null) {
            return new HashSet<>();
        }
        // 核心修复：强制类型转换，Spark中NamedExpression本质是Expression，运行时无任何类型异常
        return extractAttributes((Expression) namedExpr);
    }

    /**
     * 【原有方法】解析 Alias 依赖的原始字段名（兼容历史业务调用，无任何修改）
     * @param alias 待解析的 Alias 实例
     * @return 去重后的原始字段名集合，空输入返回空集合
     */
    public static Set<String> parseAliasDependencies(Alias alias) {
        // 复用通用方法，Alias 是 NamedExpression 子类，无缝兼容
        return parseNamedExprDependencies(alias);
    }

    /**
     * 【原有便捷方法】仅保留 Alias 入参（兼容 Java8，避免静态重载语法限制）
     * @param alias 待解析的 Alias 实例
     * @return 原始字段名集合
     */
    public static Set<String> parser(Alias alias) {
        return parseAliasDependencies(alias);
    }

    /**
     * 递归核心：遍历 Expression 树，提取所有原始表字段（Attribute/AttributeReference）
     * 通用逻辑自动处理所有嵌套/包装/聚合，无不存在类引用，Scala Seq 遍历已修复
     * @param expr 待解析的 Expression 节点（所有 Spark 表达式，包括转换后的 NamedExpression）
     * @return 去重后的原始字段名集合
     */
    private static Set<String> extractAttributes(Expression expr) {
        Set<String> attributes = new HashSet<>();
        if (expr == null) {
            return attributes;
        }

        // 1. 核心：原始表字段唯一标识（Attribute/AttributeReference，直接提取字段名）
        if (expr instanceof Attribute) {
            attributes.add(((Attribute) expr).name());
            return attributes;
        }

        // 2. 聚合表达式包装解包：AggregateExpression 是 Spark 聚合函数的标准包装类
        if (expr instanceof AggregateExpression) {
            AggregateExpression aggExpr = (AggregateExpression) expr;
            attributes.addAll(extractAttributes(aggExpr.aggregateFunction()));
            return attributes;
        }

        // 3. 平均值聚合：Avg/Average 专属解析，直接处理其子节点
        if (expr instanceof Average) {
            Average average = (Average) expr;
            attributes.addAll(extractAttributes(average.child()));
            return attributes;
        }

        // 4. 计数聚合：Count 专属解析（支持 Count(*)、Count(col)、Count(DISTINCT col)、多字段 Count）
        if (expr instanceof Count) {
            Count count = (Count) expr;
            attributes.addAll(extractCountFields(count));
            return attributes;
        }

        // 5. 嵌套别名解包：Alias 内部嵌套 Alias，递归提取底层原始字段
        if (expr instanceof Alias) {
            Alias nestedAlias = (Alias) expr;
            attributes.addAll(extractAttributes(nestedAlias.child()));
            return attributes;
        }

        // 6. 通用表达式：递归遍历所有子节点（自动处理 cast/+/*/CheckOverflow/DISTINCT 等所有嵌套）
        // 修复 Scala Seq 遍历：转为 Java List 后安全 for-each，无编译报错
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
     * Count 聚合专属解析：适配所有 Count 形态，递归自动处理 DISTINCT 内字段
     * @param count Spark 原生 Count 聚合实例
     * @return Count 依赖的原始字段名集合，Count(*) 返回空集合
     */
    private static Set<String> extractCountFields(Count count) {
        Set<String> fieldNames = new HashSet<>();
        Seq<Expression> countChildren = count.children();
        if (countChildren == null || countChildren.isEmpty()) {
            return fieldNames; // Count(*) 无入参，返回空集合
        }

        // 修复 Scala Seq 遍历：转为 Java List 后遍历所有入参
        List<Expression> javaChildren = JavaConverters.seqAsJavaList(countChildren);
        for (Expression expr : javaChildren) {
            fieldNames.addAll(extractAttributes(expr));
        }
        return fieldNames;
    }
}