package com.elastic.api;

import org.elasticsearch.client.transport.TransportClient;

/**
 * ES批量Api
 */
public class MultiDocumentApi {
    public TransportClient client;

    public MultiDocumentApi( TransportClient client){
        this.client = client;
    }


}
