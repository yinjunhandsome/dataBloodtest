package org.example.hive;

import java.util.Map;
import java.util.Set;

/**
 * HiveSqlLineageParser 使用示例
 */
public class HiveSqlLineageParserExample {

    public static void main(String[] args) {
        // 创建Hive MetaStore元数据提供者
        String metaStoreUris = "thrift://hdp-metastore-etl.58dns.org:9083";
        HiveMetaStoreMetadataProvider metaStoreProvider = new HiveMetaStoreMetadataProvider(metaStoreUris);
        HiveSqlLineageParser parser = new HiveSqlLineageParser(metaStoreProvider);

        try {
            // 示例1: 简单的SELECT查询
            String sql1="with mf as(select page_id,mofang_page_id from hdp_zhuanzhuan_dim_global.dim_zpm_page_info_full_1d_0p where page_id like '%MF%' and page_id in('OMF8256','FMF9778','TMF7880') group by 1,2),page as(select dt,actiontype,token,count(*) as pv from hdp_zhuanzhuan_dw_global.dw_log_lego_action_1d where dt='${outFileSuffix}' and pagetype='zpmshow' and actiontype in('OMF8256','FMF9778','TMF7880') and region in('o','f','t') group by 1,2,3),module as(select dt,actiontype,'轮播位' as module,token,count(*) as pv from hdp_zhuanzhuan_dw_global.dw_log_lego_action_1d where dt='${outFileSuffix}' and pagetype='Areaexposure' and actiontype in('OMF8256','FMF9778','TMF7880') and region in('o','f','t') and datapool['sectionId'] in('2081531','2081564','2081552') group by 1,2,3,4 union all select dt,actiontype,datapool['sortName'] as module,token,count(*) as pv from hdp_zhuanzhuan_dw_global.dw_log_lego_action_1d where dt='${outFileSuffix}' and pagetype='zpmclick' and actiontype in('OMF8256','FMF9778','TMF7880') and region in('o','f','t') and datapool['sectionId'] in('2030500','2026018','2026021') and datapool['sortName'] not in('商品','包袋','腕表','首饰配饰','鞋服','标签') and datapool['sortName'] is not null group by 1,2,3,4)insert OVERWRITE table hdp_ubu_zhuanzhuan_ads_lux.ads_lux_zz_qcjl_explore_inc_1d PARTITION(dt='${outFileSuffix}') select a.dt,a.type,case when a.actiontype='OMF8256' then '奢品馆-精选' when a.actiontype='FMF9778' then '奢品馆-包袋' when a.actiontype='TMF7880' then '奢品馆-腕表' else '其他' end as source,b.mofang_page_id,a.module,a.token,a.pv from (select '页面' as type,dt,actiontype,'页面' as module,token,pv from page union all select '模块' as type,dt,actiontype,module,token,pv from module) a left join mf b on b.page_id=a.actiontype";
            String[] split = sql1.split(";");
            System.out.println("=== 示例1: 简单查询 ===");
            System.out.println("SQL: " + sql1);
            Map<String, Set<String>> result1 = parser.parse(sql1);
            printResult(result1);

        } finally {
            // 关闭MetaStore连接
            metaStoreProvider.close();
        }
    }

    private static void printResult(Map<String, Set<String>> lineageData) {
        if (lineageData.isEmpty()) {
            System.out.println("未产生字段血缘关系(可能是SELECT查询，不是INSERT/CREATE)");
            return;
        }

        System.out.println("字段血缘关系:");
        for (Map.Entry<String, Set<String>> entry : lineageData.entrySet()) {
            System.out.println("  " + entry.getKey() + " <- " + entry.getValue());
        }
    }
}
