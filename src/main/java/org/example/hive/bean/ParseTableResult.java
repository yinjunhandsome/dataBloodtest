package org.example.hive.bean;

import lombok.Data;

import java.util.List;

@Data
public class ParseTableResult {
    private String aliasName;
    private String tableName;
    private String dbName;
    private String tableFullName;
    private List<String> columnNameList;
    // If this TOK_TABREF references an existing subquery, store the subquery alias here
    private String subQueryRef;

    // Explicit getters and setters for subQueryRef to ensure compilation
    public String getSubQueryRef() {
        return subQueryRef;
    }

    public void setSubQueryRef(String subQueryRef) {
        this.subQueryRef = subQueryRef;
    }
}
