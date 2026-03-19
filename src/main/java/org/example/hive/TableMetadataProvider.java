package org.example.hive;

import java.util.List;

/**
 * 表元数据提供者接口
 * 用于在解析SQL时提供表的字段信息，避免依赖数据库查询
 */
public interface TableMetadataProvider {

    /**
     * 获取指定表的字段列表
     * @param tableFullName 完整表名，格式: db.table
     * @return 字段名列表
     */
    List<String> getTableColumns(String tableFullName);

    /**
     * 检查表是否存在
     * @param tableFullName 完整表名，格式: db.table
     * @return 是否存在
     */
    boolean tableExists(String tableFullName);
}
