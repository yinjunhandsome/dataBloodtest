package org.example.hive.bean;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class ParseJoinOnRelation {
    List<ParseJoinOnSingleRelation> onColumnList = new ArrayList<>();

    public void setOnOneResult(ParseJoinOnSingleRelation parseJoinOnSingleRelation) {
        onColumnList.add(parseJoinOnSingleRelation);
    }
}
