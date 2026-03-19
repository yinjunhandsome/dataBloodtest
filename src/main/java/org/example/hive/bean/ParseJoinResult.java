package org.example.hive.bean;

import lombok.Data;

import java.util.List;

@Data
public class ParseJoinResult {
    private List<ParseTableResult> parseTableResults;
    private List<ParseJoinResult> parseJoinResults;
    private List<ParseSubQueryResult> parseSubQueryResults;
    private List<ParseWithResult> parseWithResults;
}
