package com.elastic.api;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;


public class EsClient {
    private static TransportClient client;
    static{
        String hostString1 = "10.2.4.15";
        String hostString2 = "10.2.4.42";
        String hostString3 = "10.2.4.43";
        int port = 9300;
        Settings settings = Settings.settingsBuilder()
                .put("cluster.name", "SOC-15")
                .put("transport.tcp.compress", true)
                .build();

        TransportAddress[] addressArr = new TransportAddress[3];
        try {
            addressArr[0] = new InetSocketTransportAddress(InetAddress.getByName(hostString1), port);
            addressArr[1] = new InetSocketTransportAddress(InetAddress.getByName(hostString2), port);
            addressArr[2] = new InetSocketTransportAddress(InetAddress.getByName(hostString3), port);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        client = TransportClient.builder().settings(settings).build().addTransportAddresses(addressArr);

    }

    //测试
    public static void main(String[] args) {
        EsApi ea = new EsApi(client);
        //创建索引
//        BulkRequestBuilder brb = client.prepareBulk();
//        ea.createIndex( brb);
        //获取节点信息
//        ea.getClusterInfo();
//        ea.singleUpdate("485313b09de0491ea2bacb524965613a","{\"yjd\": 10,\"study_yjd\": 10}");
        //删除
//        ea.deleteDoc("AV5V7ZmXKBTIVYuVW0Zt");
        //查询
//        ea.queryBuilders();
        close();
    }

    public static void  close()
    {
        try {
            client.close();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                client.close();
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
    }
}