package org.example.utils;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 字段级血缘实体类
 * 表示单个字段的完整血缘链路
 */
public class FieldLineage {
    /**
     * 字段名称
     */
    private String fieldName;

    /**
     * 所属表/节点名称
     */
    private String tableName;

    /**
     * 字段类型（列、计算字段、聚合字段等）
     */
    private FieldType fieldType;

    /**
     * 直接依赖的上游字段（递归结构，形成血缘链）
     */
    private List<FieldLineage> dependencies;

    /**
     * 表达式（如果是计算字段，存储计算表达式）
     */
    private String expression;

    public enum FieldType {
        COLUMN,          // 普通列
        CALCULATED,      // 计算字段（如 a + b）
        AGGREGATE,       // 聚合字段（如 SUM(x)）
        ALIAS,           // 别名字段
        CONSTANT,        // 常量
        WINDOW_FUNCTION, // 窗口函数
        LITERAL          // 字面量
    }

    public FieldLineage() {
        this.dependencies = new ArrayList<>();
        this.fieldType = FieldType.COLUMN;
    }

    public FieldLineage(String fieldName, String tableName) {
        this();
        this.fieldName = fieldName;
        this.tableName = tableName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    public void setFieldType(FieldType fieldType) {
        this.fieldType = fieldType;
    }

    public List<FieldLineage> getDependencies() {
        return dependencies;
    }

    public void setDependencies(List<FieldLineage> dependencies) {
        this.dependencies = dependencies != null ? dependencies : new ArrayList<>();
    }

    public void addDependency(FieldLineage dependency) {
        if (dependency != null) {
            this.dependencies.add(dependency);
        }
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    /**
     * 获取完整字段路径
     */
    public String getFullName() {
        return tableName != null ? tableName + "." + fieldName : fieldName;
    }

    /**
     * 获取所有源头字段（递归追溯）
     */
    public Set<String> getAllSourceFields() {
        Set<String> sources = new HashSet<>();
        collectAllSourceFields(this, sources);
        return sources;
    }

    private void collectAllSourceFields(FieldLineage field, Set<String> sources) {
        if (field == null) {
            return;
        }

        // 如果没有依赖，说明是源头字段
        if (field.getDependencies().isEmpty()) {
            sources.add(field.getFullName());
            return;
        }

        // 递归收集依赖字段的源头
        for (FieldLineage dep : field.getDependencies()) {
            collectAllSourceFields(dep, sources);
        }
    }

    /**
     * 获取血缘链路描述
     */
    public String getLineageDescription() {
        StringBuilder sb = new StringBuilder();
        buildLineageDescription(this, sb, 0);
        return sb.toString();
    }

    private void buildLineageDescription(FieldLineage field, StringBuilder sb, int level) {
        if (field == null) {
            return;
        }

        // Java 8 兼容的缩进生成方式
        StringBuilder indentBuilder = new StringBuilder();
        for (int i = 0; i < level; i++) {
            indentBuilder.append("  ");
        }
        String indent = indentBuilder.toString();

        sb.append(indent).append("→ ").append(field.getFullName());

        if (field.getFieldType() != FieldType.COLUMN) {
            sb.append(" [").append(field.getFieldType()).append("]");
        }

        if (field.getExpression() != null && !field.getExpression().isEmpty()) {
            sb.append(" (").append(field.getExpression()).append(")");
        }

        sb.append("\n");

        for (FieldLineage dep : field.getDependencies()) {
            buildLineageDescription(dep, sb, level + 1);
        }
    }

    @Override
    public String toString() {
        return "FieldLineage{" +
                "fieldName='" + fieldName + '\'' +
                ", tableName='" + tableName + '\'' +
                ", fieldType=" + fieldType +
                ", dependencies=" + dependencies.size() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldLineage that = (FieldLineage) o;
        return Objects.equals(fieldName, that.fieldName) &&
                Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, tableName);
    }
}
