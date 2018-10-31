package com.jeninfo.hadoopservice.service;

import com.jeninfo.hadoopservice.model.Article;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.cluster.Health;
import io.searchbox.cluster.NodesInfo;
import io.searchbox.cluster.NodesStats;
import io.searchbox.core.*;
import io.searchbox.indices.*;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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

    /**
     * 查询
     *
     * @param queryString
     */
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

    /**
     * 查询所有
     */
    public void searchAll() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("articleindex")
                .build();
        try {
            SearchResult result = jestClient.execute(search);
            System.out.println("本次查询共查到：" + result.getTotal() + "篇文章！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取文档
     *
     * @param index
     * @param type
     * @param id    文档id
     */
    public void getDocument(String index, String type, String id) {
        Get get = new Get.Builder(index, id).type(type).build();
        JestResult result = null;
        try {
            result = jestClient.execute(get);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Article article = result.getSourceAsObject(Article.class);
        System.out.println(article.getTitle() + "," + article.getContent());
    }

    /**
     * 删除文档
     *
     * @param index
     * @param type
     * @param id
     */
    public void deleteDocument(String index, String type, String id) {
        Delete delete = new Delete.Builder(id).index(index).type(type).build();
        JestResult result = null;
        try {
            result = jestClient.execute(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(result.getJsonString());
    }

    /**
     * 修改文档
     *
     * @param index
     * @param type
     * @param id
     */
    public void updateDocument(String index, String type, String id) {
        Article article = new Article();
        article.setId(Integer.parseInt(id));
        article.setTitle("中国3颗卫星拍到阅兵现场高清照");
        article.setContent("据中国资源卫星应用中心报道，9月3日，纪念中国人民抗日战争暨世界反法西斯战争胜利70周年大阅兵在天安门广场举行。资源卫星中心针对此次盛事，综合调度在轨卫星，9月1日至3日连续三天持续观测首都北京天安门附近区域，共计安排5次高分辨率卫星成像。在阅兵当日，高分二号卫星、资源三号卫星及实践九号卫星实现三星联合、密集观测，捕捉到了阅兵现场精彩瞬间。为了保证卫星准确拍摄天安门及周边区域，提高数据处理效率，及时制作合格的光学产品，资源卫星中心运行服务人员从卫星观测计划制定、复核、优化到系统运行保障、光学产品图像制作，提前进行了周密部署，并拟定了应急预案，为圆满完成既定任务奠定了基础。");
        article.setPubdate(new Date());
        article.setAuthor("匿名");
        article.setSource("新华网");
        article.setUrl("http://news.163.com/15/0909/07/B32AGCDT00014JB5.html");
        String script = "{" +
                "    \"doc\" : {" +
                "        \"title\" : \"" + article.getTitle() + "\"," +
                "        \"content\" : \"" + article.getContent() + "\"," +
                "        \"author\" : \"" + article.getAuthor() + "\"," +
                "        \"source\" : \"" + article.getSource() + "\"," +
                "        \"url\" : \"" + article.getUrl() + "\"," +
                "        \"pubdate\" : \"" + new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(article.getPubdate()) + "\"" +
                "    }" +
                "}";
        Update update = new Update.Builder(script).index(index).type(type).id(id).build();
        JestResult result = null;
        try {
            result = jestClient.execute(update);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(result.getJsonString());
    }

    /**
     * 节点状态
     */
    public void nodesStats() {
        NodesStats nodesStats = new NodesStats.Builder().build();
        JestResult result = null;
        try {
            result = jestClient.execute(nodesStats);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(result.getJsonString());
    }

    /**
     * 查看集群健康状态
     */
    public void health() {
        Health health = new Health.Builder().build();
        JestResult result = null;
        try {
            result = jestClient.execute(health);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(result.getJsonString());
    }

    /**
     * 节点信息
     */
    public void nodesInfo() {
        NodesInfo nodesInfo = new NodesInfo.Builder().build();
        JestResult result = null;
        try {
            result = jestClient.execute(nodesInfo);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(result.getJsonString());
    }

    /**
     * 判断索引目录是否存在
     *
     * @param index
     */
    public void indicesExists(String index) {
        IndicesExists indicesExists = new IndicesExists.Builder(index).build();
        JestResult result = null;
        try {
            result = jestClient.execute(indicesExists);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(result.getJsonString());
    }

    /**
     * 刷新索引
     */
    public void flush() {
        Flush flush = new Flush.Builder().build();
        JestResult result = null;
        try {
            result = jestClient.execute(flush);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(result.getJsonString());
    }

    /**
     * 优化索引
     */
    public void optimize() {
        Optimize optimize = new Optimize.Builder().build();
        JestResult result = null;
        try {
            result = jestClient.execute(optimize);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(result.getJsonString());
    }

    /**
     * 关闭索引
     */
    public void closeIndex() {
        CloseIndex closeIndex = new CloseIndex.Builder("article").build();
        JestResult result = null;
        try {
            result = jestClient.execute(closeIndex);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(result.getJsonString());
    }

    /**
     * 清缓存
     */
    public void clearCache() {
        ClearCache closeIndex = new ClearCache.Builder().build();
        JestResult result = null;
        try {
            result = jestClient.execute(closeIndex);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(result.getJsonString());
    }
}
