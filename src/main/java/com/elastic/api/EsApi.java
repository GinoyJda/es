package com.elastic.api;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.metrics.avg.AvgBuilder;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * Date: 17-9-6
 * Time: 上午9:52
 * To change this template use File | Settings | File Templates.
 */
public class EsApi {

    public TransportClient client;

    public EsApi( TransportClient client){
            this.client = client;
    }

    /**
     * 创建索引
     */
    public  void createIndex(BulkRequestBuilder brb1){

            for(int i = 0 ;i<10;i++){
                String json =  "{\"gs\":"+(10-i)+",\"study\": "+i+"}";
                IndexRequest request = client.prepareIndex("test", "test").setSource(json).request();
                brb1.add(request);
            }
            BulkResponse bulkResponse = brb1.execute().actionGet();
    }

    /**
     * 查看集群信息
     */
    public  void getClusterInfo() {
        List<DiscoveryNode> nodes = client.connectedNodes();
        for (DiscoveryNode node : nodes) {
            System.out.println(node.toString());
        }
    }


    /**
     * 单个更新
     */
    public  void singleUpdate(String id,String json) {
        client.prepareUpdate().setIndex("test").setType("test").setId(id).setDoc(json).execute().actionGet();
    }


    public void queryBuilders(){
        //查询所有
//        QueryBuilder qb = QueryBuilders.matchAllQuery();
        //单字段 词条查询
//        QueryBuilder qb =   QueryBuilders.termsQuery("study_gs",new int[]{1,2,3});
        //时间段查询
//        QueryBuilders.rangeQuery("time").from(1490926962223.0).to(1490926965000.0) ;
        //组合查询or
//        QueryBuilder qb =  (QueryBuilders.boolQuery()
//                .should(QueryBuilders.termsQuery("study",new int[]{1})))
//                .should(QueryBuilders.termsQuery("gs",new int[]{4}));
        //组合查询and
//        QueryBuilder qb =  QueryBuilders.boolQuery()
//                .must(QueryBuilders.termsQuery("study",new int[]{1})) ;
         //模糊查询
        QueryBuilder qb = QueryBuilders.wildcardQuery("study","1");

        sysOut(qb,null);

    }

    /**
     * 删除一条数据
     */
    public void deleteDoc(String id) {
        DeleteResponse deleteResponse  = this.client
                .prepareDelete()
                .setIndex("test")
                .setType("test")
                .setId(id)
                .get();
        System.out.println(deleteResponse.isFound()); // true表示成功
    }


    /**
     * 统一输出方法
     */
    public void sysOut(QueryBuilder qb,AvgBuilder ab ){
        SearchResponse actionGet = null;
        if(null == ab){
             actionGet = client.prepareSearch("test")
                    .setTypes("test")
                    .setQuery(qb)
                    .setSize(20)
                    .execute()
                    .actionGet();
        }else{
             actionGet = client.prepareSearch("test")
                    .setTypes("test")
                    .setQuery(qb)
                    .addAggregation(ab)
                    .setSize(20)
                    .execute()
                    .actionGet();
        }


        SearchHits hits =  actionGet.getHits();
        System.out.println("Hints:"+hits.getTotalHits());
        for (SearchHit hit : hits.getHits()){ //getHits 的使用
            System.out.println(hit.getSourceAsString());//这样可以获得属性的值
//            System.out.println(hit.getSource().get("study_gs"));
        }
    }



}
