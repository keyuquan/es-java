package com.es;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import static org.elasticsearch.index.query.QueryBuilders.*;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class clientTest {

    public static void main(String[] args) throws Exception {
        // 一. Client
        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
        TransportClient client = new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));
        System.out.println(client);

        // 二. Document APIs
        //Index API
        IndexResponse indexResponse = client.prepareIndex("twitter", "_doc", "8")
                .setSource(jsonBuilder()
                        .startObject()
                        .field("user", "kimchy8")
                        .field("post_date", new Date())
                        .field("message", "trying out Elasticsearch")
                        .endObject()
                )
                .get();
        System.out.println(indexResponse);

        //Get API
        GetResponse response = client.prepareGet("twitter", "_doc", "2").get();
        System.out.println(response);

        //Delete API
        DeleteResponse deleteResponse = client.prepareDelete("twitter", "_doc", "1").get();
        System.out.println(deleteResponse);

        //Delete By Query API
        BulkByScrollResponse bulkByScrollResponse = DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("user", "kimchy3"))
                .source("twitter")
                .get();
        long deleted = bulkByScrollResponse.getDeleted();
        System.out.println(deleted);

        //Update API
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("twitter");
        updateRequest.type("_doc");
        updateRequest.id("2");
        updateRequest.doc(jsonBuilder()
                .startObject()
                .field("user", "user22")
                .endObject());
        UpdateResponse updateResponse = client.update(updateRequest).get();
        System.out.println(updateResponse);

        //Multi Get API
        //Bulk API
        //Using Bulk Processor
        //Update By Query API
        UpdateByQueryRequestBuilder updateByQuery = UpdateByQueryAction.INSTANCE.newRequestBuilder(client);
        updateByQuery.source("my_index")
                .filter(QueryBuilders.termQuery("user", "kimchy"))
                .script(new Script(
                        ScriptType.INLINE,
                        "painless",
                        "ctx._source.age +=1",
                        Collections.<String, Object>emptyMap()));
        BulkByScrollResponse bulkByScrollResponse2 = updateByQuery.get();
        System.out.println(bulkByScrollResponse2);

        //Reindex API

        // 三. Search API
        // Using scrolls in Java
        QueryBuilder qb = termQuery("user", "kimchy");

        SearchResponse scrollResp = client.prepareSearch("my_index")
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setScroll(new TimeValue(60000))
                .setQuery(qb)
                .setSize(100).get(); //max of 100 hits will be returned for each scroll
        do {
            for (SearchHit hit : scrollResp.getHits().getHits()) {
                System.out.println(hit);
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
        }
        while (scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.

        // MultiSearch API

        // Using Aggregations

        //Terminate After

        //Search Template

        // 四. Query DSL
        // Match All Query
        // Full text queries
        // Term level queries
        // Compound queries
        // Joining queries
        // Geo queries
        // Specialized queries
        // Span queries

        client.close();
    }
}
