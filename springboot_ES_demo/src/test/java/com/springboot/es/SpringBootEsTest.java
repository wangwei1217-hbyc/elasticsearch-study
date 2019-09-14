package com.springboot.es;

import com.springboot.es.entity.Item;
import com.springboot.es.repository.ItemRepository;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.aggregation.impl.AggregatedPageImpl;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 1、查询某个字段中模糊包含目标字符串，使用matchQuery
 * 2、term匹配，即不分词匹配，这个是最严格的匹配，属于低级查询，不进行分词的.你传来什么值就会拿你传的值去做完全匹配
 * 3、multi_match多个字段匹配某字符串
 *   -如果我们希望title，content两个字段去匹配某个字符串，只要任何一个字段包括该字符串即可，就可以使用multimatch。
 * Created on 2019/9/11 0011.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {SpringbootESApplication.class})
public class SpringBootEsTest {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;
    @Autowired
    private ItemRepository itemRepository;

    @Test
    public void createIndex(){
        elasticsearchTemplate.createIndex(Item.class);
        elasticsearchTemplate.putMapping(Item.class);
        QueryBuilders.rangeQuery("");

    }

    @Test
    public void deleteIndex(){
        elasticsearchTemplate.deleteIndex(Item.class);
        System.out.println("====over=====");
    }
    @Test
    public void addDoc() throws Exception{
        Item item = new Item();
        item.setId(1L);
        item.setTitle("微星游戏笔记本GL62-1017N");
        item.setBrand("微星msi");
        item.setCategory("笔记本电脑");
        item.setPrice(6999d);
        item.setImages("http://www.baidu.com/");
        String date = "2019-09-10 12:12:24";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        item.setDate(sdf.parse(date));
        itemRepository.save(item);
        System.out.println("----完成------");
    }

    @Test
    public void batchInsertDoc() throws Exception{
        List<Item> items = new ArrayList<Item>(){
            {
//                add(new Item(2L,"摩托罗拉g5s手机","智能手机","摩托罗拉",899d,"http://image.com/phone_2.jpg",sdf.parse("2018-05-10 10:11:12")));
//                add(new Item(3L,"联想k5 pro手机","智能手机","联想",999d,"http://image.com/phone_3.jpg",sdf.parse("2019-06-01 16:10:00")));
//                add(new Item(4L,"华硕飞行堡垒三代","笔记本电脑","华硕",7999d,"http://image.com/microcomputer_4.jpg",sdf.parse("2017-12-12 14:00:00")));

//                add(new Item(5L,"华为mate10","智能手机","华为",1999d,"http://image.com/phone_5.jpg",sdf.parse("2018-11-01 10:11:12")));
//                add(new Item(6L,"荣耀v10","智能手机","荣耀",1799d,"http://image.com/phone_6.jpg",sdf.parse("2018-01-01 09:10:00")));
                add(new Item(7L,"红米5A","智能手机","RedMi",499d,"http://image.com/phone_7.jpg",sdf.parse("2017-10-01 10:10:00")));
                add(new Item(8L,"RedMi note7","智能手机","红米",1099d,"http://image.com/phone_8.jpg",sdf.parse("2017-12-20 10:10:00")));
            }
        };
        itemRepository.saveAll(items);
        System.out.println("----完成------");
    }

    @Test
    public void findAll(){
//        Iterable<Item> all = itemRepository.findAll();
//        for(Item item : all){
//            System.out.println(item);
//        }
        /*
        ascending()升序
        descending()降序
         */
        Iterable<Item> list = itemRepository.findAll(Sort.by("price").descending());
        for(Item item : list){
            System.out.println(item);
        }
    }

    /*
    自定义方法
     */
    @Test
    public void findByPriceBetween(){
        List<Item> itemList = itemRepository.findByPriceBetween(6000d, 8000d);
        for(Item item : itemList){
            System.out.println(item);
        }
    }

    /*
    全文检索：
     * 单字符串模糊查询，默认排序。将从所有字段中查找包含传来的word分词后字符串的数据集
     *
     * 查询结果：
Item{id=7, title='红米5A', category='智能手机', brand='RedMi', price=499.0, images='http://image.com/phone_7.jpg', date=Sun Oct 01 10:10:00 GMT+08:00 2017}
Item{id=8, title='RedMi note7', category='智能手机', brand='红米', price=1099.0, images='http://image.com/phone_8.jpg', date=Wed Dec 20 10:10:00 GMT+08:00 2017}
     */
    @Test
    public void testFullQuery(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withIndices("items");
        queryBuilder.withQuery(QueryBuilders.queryStringQuery("RedMi"));
        List<Item> items = elasticsearchTemplate.queryForList(queryBuilder.build(), Item.class);
        for(Item item : items){
            System.out.println(item);
        }
    }


    /*
    -matchQuery底层采用的是词条匹配查询.某字段按字符串模糊查询.
    完全包含查询:如果我们希望必须是满足“摩托罗拉g5s手机”才能被检索出来，就需要设置一下Operator。
    设置成Operator.AND

    -无论是matchQuery，multiMatchQuery，queryStringQuery等，都可以设置operator。默认为Or。
     */
    @Test
    public void testMatchQuery(){
        // 构建查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 添加基本分词查询

        queryBuilder.withQuery(QueryBuilders.matchQuery("title","摩托罗拉g5s手机").operator(Operator.AND));
        Page<Item> items = itemRepository.search(queryBuilder.build());
        //总页数
        int totalPages = items.getTotalPages();
        // 总条数
        long totalElements = items.getTotalElements();
        System.out.println("totalPages=" + totalPages+",totalElements="+totalElements);
        for(Item item : items){
            System.out.println(item);
        }
    }

    @Test
    public void testMatchTitleCustom(){
        List<Item> items = itemRepository.findByTitleCustom("摩托罗拉g5s手机");
        System.out.println(items);
    }
    /*
    * termQuery:功能更强大，除了匹配字符串以外，还可以匹配
                int/long/double/float/....
     */
    @Test
    public void testTermQuery(){
        // 构建查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 添加基本分词查询
        queryBuilder.withQuery(QueryBuilders.termQuery("price",999d));
        Page<Item> items = itemRepository.search(queryBuilder.build());
        for(Item item : items){
            System.out.println(item);
        }
    }
    /*
    布尔查询
     */
    @Test
    public void testBooleanQuery(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        //满足条件：title-包含摩托罗拉手机，并且brand-摩托罗拉
        queryBuilder.withQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("title","摩托罗拉手机"))
                                                        .must(QueryBuilders.matchQuery("brand","摩托罗拉")));
        Page<Item> items = itemRepository.search(queryBuilder.build());
        for(Item item : items){
            System.out.println(item);
        }
    }
    /*
    模糊查询
     */
    @Test
    public void testFuzzyQuery(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withQuery(QueryBuilders.fuzzyQuery("title","手机"));
        Page<Item> items = itemRepository.search(queryBuilder.build());
        for(Item item : items){
            System.out.println(item);
        }
        System.out.println(">>>>>查询完毕<<<<<");
    }

    @Test
    public void testPageQuery(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withQuery(QueryBuilders.termQuery("category","智能手机"));
        //Elasticsearch中的分页是从第0页开始
        queryBuilder.withPageable(PageRequest.of(0,2));
        //排序。指定排序字段和排序方式
        queryBuilder.withSort(SortBuilders.fieldSort("id").order(SortOrder.ASC));
        Page<Item> items = itemRepository.search(queryBuilder.build());
        System.out.println("总条数："+items.getTotalElements());
        System.out.println("总页数："+items.getTotalPages());
        System.out.println("当前页："+items.getNumber());
        System.out.println("每页显示条数："+items.getSize());

        for(Item item : items){
            System.out.println(item);
        }
        System.out.println(">>>>>查询完毕<<<<<");
    }

    /*
    聚合
     */
    @Test
    public void testAgg(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        //不查询任何结果
//        queryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{""},null));
        // 1、添加一个新的聚合，聚合类型为terms，聚合名称为brands，聚合字段为brand
        queryBuilder.addAggregation(AggregationBuilders.terms("brands").field("brand"));
        //2、查询,需要把结果强转为AggregatedPage类型
        AggregatedPage<Item> aggPage = (AggregatedPage<Item>) itemRepository.search(queryBuilder.build());
        // 3、解析
        // 3.1、从结果中取出名为brands的那个聚合，
        // 因为是利用String类型字段来进行的term聚合，所以结果要强转为StringTerm类型
        StringTerms agg = (StringTerms) aggPage.getAggregation("brands");
        //3.2、获取桶
        List<StringTerms.Bucket> buckets = agg.getBuckets();
        //3.3 遍历
        for(StringTerms.Bucket bucket : buckets){
            // 3.4、获取桶中的key，即品牌名称
            System.out.println(bucket.getKeyAsString());
            // 3.5、获取桶中的文档数量
            System.out.println(bucket.getDocCount());
        }
    }

    /*
    嵌套聚合，求平均值
     */
    @Test
    public void testSubAgg(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 1、添加一个新的聚合，聚合类型为terms，聚合名称为brands，聚合字段为brand
        // 在品牌聚合桶内进行嵌套聚合，求平均值
        queryBuilder.addAggregation(AggregationBuilders.terms("brands").field("brand")
                            .subAggregation(AggregationBuilders.avg("priceAvg").field("price")));
        //2、查询,需要把结果强转为AggregatedPage类型
        AggregatedPage<Item> aggPage = (AggregatedPage<Item>) itemRepository.search(queryBuilder.build());
        // 3、解析
        // 3.1、从结果中取出名为brands的那个聚合，
        // 因为是利用String类型字段来进行的term聚合，所以结果要强转为StringTerm类型
        StringTerms agg = (StringTerms) aggPage.getAggregation("brands");
        //3.2、获取桶
        List<StringTerms.Bucket> buckets = agg.getBuckets();
        // 3.3、遍历
        for(StringTerms.Bucket bucket : buckets){
            // 3.4、获取桶中的key，即品牌名称  3.5、获取桶中的文档数量
            System.out.println("品牌："+bucket.getKeyAsString()+"，共"+bucket.getDocCount()+"台");
            // 3.5.获取子聚合结果：
            InternalAvg priceAvg = (InternalAvg) bucket.getAggregations().asMap().get("priceAvg");
            System.out.println("平均售价: "+priceAvg.getValue());
            System.out.println("===========================");
        }

    }

    /*
    rangeQuery:范围查询
    示例：时间范围查询.注：传入long型时间戳才能匹配
     */
    @Test
    public void testRangeQuery() throws Exception{
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        queryBuilder.withIndices("items");
        String start = "2017-01-01 00:00:00";
        String end = "2017-12-31 23:59:59";
        Date startDate = sdf.parse(start);
        Date endDate = sdf.parse(end);
        queryBuilder.withQuery(QueryBuilders.rangeQuery("date")
                .from(startDate.getTime())
                .to(endDate.getTime())
                .includeLower(true)
                .includeUpper(true));
        List<Item> items = elasticsearchTemplate.queryForList(queryBuilder.build(), Item.class);
        for(Item item : items){
            System.out.println(item);
        }
    }

    /*
    高亮查询
     */
    @Test
    public void testHighlightQuery(){
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        //设置要检索的索引库
        queryBuilder.withIndices("items");
        queryBuilder.withQuery(QueryBuilders.queryStringQuery("RedMi"));

        //高亮设置
        List<String> hightlightFields = new ArrayList<String>();
        hightlightFields.add("title");
        hightlightFields.add("category");
        hightlightFields.add("brand");
        hightlightFields.add("price");

        HighlightBuilder.Field[] fields = new HighlightBuilder.Field[hightlightFields.size()];
        for(int x=0;x<hightlightFields.size();x++){
            fields[x]= new HighlightBuilder.Field(hightlightFields.get(x))
                        .preTags("<em>").postTags("</em>");
        }

        queryBuilder.withHighlightFields(fields);

        queryBuilder.withPageable(PageRequest.of(0,1000));

        AggregatedPage<Item> page = elasticsearchTemplate.queryForPage(queryBuilder.build(), Item.class, new SearchResultMapper() {
            @Override
            public <T> AggregatedPage<T> mapResults(SearchResponse response, Class<T> clazz, Pageable pageable) {
                List<Item> list = new ArrayList<Item>();
                SearchHit[] hits = response.getHits().getHits();
                if (hits.length <= 0) {
                    return null;
                }
                for (SearchHit searchHit : hits) {
                    Item item = new Item();
                    item.setId(Long.valueOf(searchHit.getId()));
                    item.setTitle(String.valueOf(searchHit.getSourceAsMap().get("title")));
                    item.setCategory(String.valueOf(searchHit.getSourceAsMap().get("category")));
                    item.setBrand(String.valueOf(searchHit.getSourceAsMap().get("brand")));
                    item.setPrice((Double) searchHit.getSourceAsMap().get("price"));
                    item.setImages(String.valueOf(searchHit.getSourceAsMap().get("images")));

                    Object date = searchHit.getSourceAsMap().get("date");
                    if (date != null) {
                        item.setDate(new Date(Long.valueOf(String.valueOf(date))));
                    }
                    // 反射调用set方法将高亮内容设置进去
                    for (String field : hightlightFields) {
                        HighlightField highlightField = searchHit.getHighlightFields().get(field);
                        if (highlightField != null) {
                            String setMethodName = parSetName(field);
                            Class<? extends Item> itemClazz = item.getClass();
                            Method[] methods = itemClazz.getMethods();
                            for (Method method : methods) {
                                if (setMethodName.equals(method.getName())) {
                                    String highlightStr = highlightField.getFragments()[0].toString();
                                    try {
                                        method.invoke(item, highlightStr);
                                    } catch (IllegalAccessException e) {
                                        e.printStackTrace();
                                    } catch (InvocationTargetException e) {
                                        e.printStackTrace();
                                    }
                                    list.add(item);
                                    break;
                                }
                            }
                        }
                    }
                }

                if (list.size() > 0) {
                    AggregatedPage<T> result = new AggregatedPageImpl<T>((List<T>) list, pageable, response.getHits().getTotalHits());
                    return result;
                }

                return null;
            }
        });
        System.out.println("总记录数："+page.getTotalElements());
        System.out.println("总页数："+page.getTotalPages());
        System.out.println("当前页："+page.getNumber());

        page.stream().forEach(System.out::println);
    }

    /**
     * 拼接在某属性的 set方法
     *
     * @param fieldName
     * @return String
     */
    private static String parSetName(String fieldName) {
        if (null == fieldName || "".equals(fieldName)) {
            return null;
        }
        int startIndex = 0;
        if (fieldName.charAt(0) == '_')
            startIndex = 1;
        return "set" + fieldName.substring(startIndex, startIndex + 1).toUpperCase()
                + fieldName.substring(startIndex + 1);
    }


}
