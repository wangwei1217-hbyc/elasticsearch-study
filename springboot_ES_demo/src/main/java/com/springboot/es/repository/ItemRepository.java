package com.springboot.es.repository;

import com.springboot.es.entity.Item;
import org.springframework.data.elasticsearch.annotations.Query;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

/**
 * Spring Data 的另一个强大功能，是根据方法名称自动实现功能.
 * Keyword	Sample
     And	findByNameAndPrice
     Or	    findByNameOrPrice
     Is	    findByName
     Not	findByNameNot
     Between	findByPriceBetween
     LessThanEqual	findByPriceLessThan
     GreaterThanEqual	findByPriceGreaterThan
     Before	findByPriceBefore
     After	findByPriceAfter
     Like	findByNameLike
     StartingWith	findByNameStartingWith
     EndingWith	findByNameEndingWith
     Contains/Containing	findByNameContaining
     In	findByNameIn(Collection<String>names)
     NotIn	findByNameNotIn(Collection<String>names)
     Near	findByStoreNear
     True	findByAvailableTrue
     False	findByAvailableFalse
     OrderBy	findByAvailableTrueOrderByNameDesc
 ————————————————
 * Created on 2019/9/11 0011.
 */
public interface ItemRepository extends ElasticsearchRepository<Item,Long> {
    List<Item> findByPriceBetween(double price1, double price2);
    @Query("{\"match_phrase\":{\"title\":\"?0\"}}")
    List<Item> findByTitleCustom(String keyword);
}
