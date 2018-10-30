package com.jeninfo.hadoopservice.service;

import com.jeninfo.hadoopservice.model.Article;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.*;
import io.searchbox.indices.DeleteIndex;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author chenzhou
 * @date 2018/10/30 20:37
 * @description
 */
@Service
public class EsService {
    @Autowired
    private JestClient jestClient;

    /**
     * 创建索引
     *
     * @param index
     * @param indexName
     * @param type
     */
    public void createIndex(Object index, String indexName, String type) {
        Index build = new Index.Builder(index).index(indexName).type(type).build();
        try {
            JestResult result = jestClient.execute(build);
            System.out.println(result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量操作
     *
     * @param index
     * @param indexName
     * @param typeName
     */
    public void bulkIndex(List<Index> index, String indexName, String typeName) {
        Bulk bulk = new Bulk.Builder()
                .defaultIndex(indexName)
                .defaultType(typeName)
                .addAction(index).build();
        try {
            BulkResult bulkResult = jestClient.execute(bulk);
            System.out.println("------->> " + bulkResult.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除指定索引
     *
     * @param indexName
     */
    public void deleteIndex(String indexName) {
        DeleteIndex deleteIndex = new DeleteIndex.Builder(indexName).build();
        try {
            JestResult result = jestClient.execute(deleteIndex);
            System.out.println(result.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void createSearch(String queryString) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SearchSourceBuilder query = searchSourceBuilder.query(QueryBuilders.queryStringQuery(queryString));
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        //高亮title
        highlightBuilder.field("title");
        //高亮content
        highlightBuilder.field("content");
        //高亮标签
        highlightBuilder.preTags("<em>").postTags("</em>");
        //高亮内容长度
        highlightBuilder.fragmentSize(200);
        searchSourceBuilder.highlighter(highlightBuilder);
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("articleindex")
                .build();
        try {
            SearchResult result = jestClient.execute(search);
            System.out.println("本次查询共查到：" + result.getTotal() + "篇文章！");
            List<SearchResult.Hit<Article, Void>> hits = result.getHits(Article.class);
            hits.stream().forEach(hit -> {
                Article source = hit.source;
                System.out.println("=====================");
                System.out.println("标题：" + source.getTitle());
                System.out.println("内容：" + source.getContent());
                System.out.println("url：" + source.getUrl());
                System.out.println("来源：" + source.getSource());
                System.out.println("作者：" + source.getAuthor());
            });

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
