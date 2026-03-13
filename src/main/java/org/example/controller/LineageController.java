package org.example.controller;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.example.utils.FieldLineage;
import org.example.utils.LogicalPlanLineageParser;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Set;

/**
 * 血缘分析 Controller
 * 提供 SQL 解析和字段级血缘查询接口
 */
@RestController
@RequestMapping("/api/lineage")
public class LineageController {

    @Resource
    private LogicalPlanLineageParser lineageParser;

    /**
     * 解析 SQL 并返回字段级血缘
     *
     * @param request SQL 请求
     * @return 字段级血缘结果
     */
    @PostMapping("/parse")
    public LineageResponse parseSql(@RequestBody SqlRequest request) {
        String sql = request.getSql();
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("SQL 不能为空");
        }

        try {
            Map<String, Set<String>> lineage = lineageParser.parse(sql);
            return LineageResponse.success(lineage);
        } catch (Exception e) {
            return LineageResponse.error(e.getMessage());
        }
    }

    /**
     * SQL 请求类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SqlRequest {
        /**
         * 待解析的 SQL 语句
         */
        private String sql;
    }

    /**
     * 血缘响应类
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LineageResponse {
        /**
         * 是否成功
         */
        private boolean success;

        /**
         * 响应消息
         */
        private String message;

        /**
         * 字段级血缘数据：目标字段 -> 源字段集合
         */
        private Map<String, Set<String>> data;

        /**
         * 创建成功响应
         */
        public static LineageResponse success(Map<String, Set<String>> data) {
            return new LineageResponse(true, "解析成功", data);
        }

        /**
         * 创建错误响应
         */
        public static LineageResponse error(String message) {
            return new LineageResponse(false, message, null);
        }
    }
}
