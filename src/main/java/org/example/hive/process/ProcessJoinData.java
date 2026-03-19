package org.example.hive.process;


import org.example.hive.bean.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ProcessJoinData {
    public static Map<String, ParseColumnResult> process(List<ParseJoinResult> joinResults) {
        Map<String, ParseColumnResult> parseFromResult = new HashMap<>();
        for (int i = 0; i < joinResults.size(); i++) {
            ParseJoinResult parseJoinResult = joinResults.get(i);
            // 处理 FROM
            if (parseJoinResult.getParseTableResults() != null) {
                List<ParseTableResult> parseTableResults = parseJoinResult.getParseTableResults();
                parseFromResult.putAll(ProcessTabrefData.process(parseTableResults));
            }
            // 递归处理JOIN
            if (parseJoinResult.getParseJoinResults() != null) {
                parseFromResult.putAll(ProcessJoinData.process(parseJoinResult.getParseJoinResults()));
            }
            // 处理 SUBQUERY
            if (parseJoinResult.getParseSubQueryResults() != null) {
                List<ParseSubQueryResult> parseSubQueryResults = parseJoinResult.getParseSubQueryResults();
                parseFromResult.putAll(ProcessSubQueryData.process(parseSubQueryResults));
            }
            // 处理 WITH - 创建副本避免污染原始数据
            if (parseJoinResult.getParseWithResults() != null) {
                List<ParseWithResult> parseWithResults = parseJoinResult.getParseWithResults();
                for (ParseWithResult parseWithResult : parseWithResults) {
                    String subQueryAliasName = parseWithResult.getAliasName();
                    if (subQueryAliasName == null) {
                        subQueryAliasName = parseWithResult.getTableName();
                    }
                    Map<String, ParseColumnResult> parseColumnResultMap = parseWithResult.getParseSubQueryResults();

                    for(Map.Entry<String, ParseColumnResult> entry : parseColumnResultMap.entrySet()){
                        String columnAliasName = entry.getKey();
                        ParseColumnResult original = entry.getValue();

                        // 创建 ParseColumnResult 的副本
                        ParseColumnResult copy = new ParseColumnResult();
                        copy.setAliasName(original.getAliasName());
                        copy.setIndex(original.getIndex());
                        copy.setFromTableColumnSet(new java.util.HashSet<>(original.getFromTableColumnSet()));
                        copy.setAggregate(original.isAggregate());

                        parseFromResult.put(subQueryAliasName + "." + columnAliasName, copy);
                    }
                }
            }
        }
        return parseFromResult;
    }
}
