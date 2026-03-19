package org.example.hive.bean;

import lombok.Data;

import java.util.Set;

@Data
public class ParseJoinOnSingleRelation {
    Set<String> leftColumnSet;
    Set<String> rightColumnSet;
}
