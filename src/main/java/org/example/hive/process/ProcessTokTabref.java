package org.example.hive.process;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.log4j.Logger;
import org.example.hive.HiveMetaStoreMetadataProvider;
import org.example.hive.bean.ParseTableResult;


public class ProcessTokTabref {
    private static Logger logger = Logger.getLogger(ProcessTokTabref.class);
    public static ParseTableResult process(ASTNode ast) {
        String dbName = "default";
        String tableName;
        if (ast.getChild(0).getChildCount() == 1) {
            tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(0));
            logger.error("hive table has no db name: " + BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(0)));
        } else {
            dbName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(0));
            tableName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0).getChild(1));
        }
        String tableFullName = dbName + "." + tableName;
        // 判断是否有别名
        String tableAliasName;
        if (ast.getChild(1) != null) {
            tableAliasName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(1));
        } else {
            // 没有别名用库+表名
            tableAliasName = dbName + "__" + tableName;
        }
        // 生成 ParseTableResult
        ParseTableResult parseTableResult = new ParseTableResult();
        parseTableResult.setAliasName(tableAliasName);
        parseTableResult.setDbName(dbName);
        parseTableResult.setTableName(tableName);
        parseTableResult.setTableFullName(tableFullName);
        HiveMetaStoreMetadataProvider hiveMetaStoreMetadataProvider=new HiveMetaStoreMetadataProvider();
        parseTableResult.setColumnNameList(hiveMetaStoreMetadataProvider.getTableColumns(tableFullName));
        return parseTableResult;
    }
}
