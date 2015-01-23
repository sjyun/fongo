package com.github.fakemongo.integration;

import com.github.fakemongo.Fongo;
import com.mongodb.Mongo;
import java.util.ArrayList;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Test;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.stereotype.Repository;

public class SpringQueryTest {

  // Issue #92 : mixed source of date (Integer vs Long) problem.
  @Test
  public void should_mixed_data_works_in_spring() throws Exception {
    //Given
    ConfigurableApplicationContext ctx = new AnnotationConfigApplicationContext(FongoConfig.class);
    QueryRepository repository = ctx.getBean(QueryRepository.class);

    //When
    DomainObject object = new DomainObject();
    ArrayList<Long> longList = new ArrayList<Long>();
    longList.add(25L);
    longList.add(10L);
    object.setLongList(longList);

    repository.save(object);

    //Then

    //Queries generated from method names always work
    assertThat(repository.findByLongList(longList)).isNotNull();
    assertThat(repository.findByLongList(10L)).isNotNull();
    assertThat(repository.findByLongList(longList)).hasSize(1);
    assertThat(repository.findByLongList(10L)).hasSize(1);

    //Hand written queries do not work on Fongo
    assertThat(repository.findLongListWithQuery(longList)).isNotNull();
    assertThat(repository.findLongListWithQuery(10L)).isNotNull();
    assertThat(repository.findLongListWithQuery(longList)).hasSize(1);
    assertThat(repository.findLongListWithQuery(10L)).hasSize(1);
    ctx.close();
  }

  @Configuration
  @EnableMongoRepositories
  public static class FongoConfig extends AbstractMongoConfiguration {

    @Override
    protected String getDatabaseName() {
      return "FongoDB";
    }

    @Override
    public Mongo mongo() throws Exception {
      return new Fongo(getDatabaseName()).getMongo();
    }

  }

  @Document
  public static class DomainObject {

    @Id
    private String _id;
    private List<Long> longList;

    public List<Long> getLongList() {
      return longList;
    }

    public void setLongList(List<Long> longList) {
      this.longList = longList;
    }

    public String get_id() {
      return _id;
    }

    public void set_id(String _id) {
      this._id = _id;
    }
  }

}

@Repository
interface QueryRepository extends MongoRepository<SpringQueryTest.DomainObject, String> {

  @Query("{'longList' : ?0}")
  List<? extends SpringQueryTest.DomainObject> findLongListWithQuery(Long foo);

  @Query("{'longList' : ?0}")
  List<? extends SpringQueryTest.DomainObject> findLongListWithQuery(List<Long> foo);

  List<? extends SpringQueryTest.DomainObject> findByLongList(Long foo);

  List<? extends SpringQueryTest.DomainObject> findByLongList(List<Long> foo);
}
