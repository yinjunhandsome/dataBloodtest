package org.example.hive.bean;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class ParseJoinResult {
    private List<ParseTableResult> parseTableResults = new ArrayList<>();
    private List<ParseJoinResult> parseJoinResults = new ArrayList<>();
    private List<ParseSubQueryResult> parseSubQueryResults = new ArrayList<>();
    private List<ParseWithResult> parseWithResults = new ArrayList<>();
}
