package org.example.hive.bean;

import lombok.Data;

import java.util.Map;

@Data
public class ParseWithResult {
    private String aliasName;
    private String tableName;
    private Map<String, ParseColumnResult> parseSubQueryResults;
}
