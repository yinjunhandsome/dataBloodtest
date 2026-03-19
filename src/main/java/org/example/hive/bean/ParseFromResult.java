package org.example.hive.bean;

import lombok.Data;

import java.util.Map;

/*
 * {
 *   "columnName": {
 *       "tableAliasName": ParseColumnResult,
 *       ...
 *       }
 *   },
 *   ...
 * }
 * */

@Data
public class ParseFromResult {
    private Map<String, Map<String, ParseColumnResult>> fromResult;
}
