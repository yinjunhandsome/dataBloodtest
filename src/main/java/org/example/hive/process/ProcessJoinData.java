package org.example.hive.process;


import org.apache.log4j.Logger;
import org.example.hive.bean.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ProcessJoinData {
    private static Logger logger = Logger.getLogger(ProcessJoinData.class);

    public static Map<String, ParseColumnResult> process(List<ParseJoinResult> joinResults) {
        Map<String, ParseColumnResult> parseFromResult = new HashMap<>();
        for (int i = 0; i < joinResults.size(); i++) {
            ParseJoinResult parseJoinResult = joinResults.get(i);
            // 处理 FROM
            if (parseJoinResult.getParseTableResults() != null) {
                List<ParseTableResult> parseTableResults = parseJoinResult.getParseTableResults();
                logger.debug("ProcessJoinData.process: Processing parseTableResults, size=" + parseTableResults.size());
                Map<String, ParseColumnResult> tabrefResult = ProcessTabrefData.process(parseTableResults);
                logger.debug("ProcessJoinData.process: ProcessTabrefData returned " + tabrefResult.size() + " fields");
                parseFromResult.putAll(tabrefResult);
            }

            // 递归处理JOIN
            if (parseJoinResult.getParseJoinResults() != null) {
                Map<String, ParseColumnResult> joinResult = ProcessJoinData.process(parseJoinResult.getParseJoinResults());
                logger.debug("ProcessJoinData.process: Recursive JOIN returned " + joinResult.size() + " fields");
                parseFromResult.putAll(joinResult);
            }

            // 处理 SUBQUERY
            if (parseJoinResult.getParseSubQueryResults() != null) {
                List<ParseSubQueryResult> parseSubQueryResults = parseJoinResult.getParseSubQueryResults();
                logger.debug("ProcessJoinData.process: Processing parseSubQueryResults, size=" + parseSubQueryResults.size());

                for (ParseSubQueryResult subQueryResult : parseSubQueryResults) {
                    logger.debug("ProcessJoinData.process:   subquery aliasName=" + subQueryResult.getAliasName() +
                               ", fieldCount=" + subQueryResult.getParseSubQueryResults().size());

                    Map<String, ParseColumnResult> subQueryResultMap = ProcessSubQueryData.process(java.util.Collections.singletonList(subQueryResult));
                    logger.debug("ProcessJoinData.process:   ProcessSubQueryData returned " + subQueryResultMap.size() + " fields with prefix " + subQueryResult.getAliasName());

                    for (Map.Entry<String, ParseColumnResult> entry : subQueryResultMap.entrySet()) {
                        logger.debug("ProcessJoinData.process:     Adding field: " + entry.getKey());
                    }
                    parseFromResult.putAll(subQueryResultMap);
                }
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

        logger.debug("ProcessJoinData.process: Completed, returning " + parseFromResult.size() + " fields");
        for (String key : parseFromResult.keySet()) {
            logger.debug("ProcessJoinData.process:   " + key);
        }

        return parseFromResult;
    }
}
