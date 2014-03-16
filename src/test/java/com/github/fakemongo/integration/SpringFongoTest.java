package com.github.fakemongo.integration;

import com.github.fakemongo.Fongo;
import com.mongodb.Mongo;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.bson.types.ObjectId;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.geo.Point;
import org.springframework.data.mongodb.core.index.GeoSpatialIndexed;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.hateoas.Identifiable;

public class SpringFongoTest {

  @Test
  public void dBRefFindWorks() {
    ApplicationContext ctx = new AnnotationConfigApplicationContext(MongoConfig.class);
    MongoOperations mongoOperations = (MongoOperations) ctx.getBean("mongoTemplate");

    MainObject mainObject = new MainObject();

    ReferencedObject referencedObject = new ReferencedObject();

    mainObject.setReferencedObject(referencedObject);

    mongoOperations.save(referencedObject);
    mongoOperations.save(mainObject);

    MainObject foundObject = mongoOperations.findOne(
        new Query(Criteria.where("referencedObject.$id").is(ObjectId.massageToObjectId(referencedObject.getId()))),
        MainObject.class);

    assertNotNull("should have found an object", foundObject);
    assertEquals("should find a ref to an object", referencedObject.getId(), foundObject.getReferencedObject().getId());
  }

  @Test
  public void testGeospacialIndexed() {
    // Given
    ApplicationContext ctx = new AnnotationConfigApplicationContext(MongoConfig.class);
    MongoOperations mongoOperations = (MongoOperations) ctx.getBean("mongoTemplate");

    GeoSpatialIndexedWrapper object = new GeoSpatialIndexedWrapper();
    object.setGeo(new double[]{12.335D, 13.546D});

    // When
    mongoOperations.save(object);

    // Then
    assertEquals(object, mongoOperations.findOne(
        new Query(Criteria.where("id").is(ObjectId.massageToObjectId(object.getId()))),
        GeoSpatialIndexedWrapper.class));
    assertEquals(object, mongoOperations.findOne(
        new Query(Criteria.where("geo").is(object.getGeo())),
        GeoSpatialIndexedWrapper.class));
    assertEquals(object, mongoOperations.findOne(
        new Query(Criteria.where("geo").is(new Point(object.getGeo()[0], object.getGeo()[1]))),
        GeoSpatialIndexedWrapper.class));
  }

  @Test
  public void testMongoRepository() {
    // Given
    ApplicationContext ctx = new AnnotationConfigApplicationContext(MongoConfig.class);
    TestRepository mongoRepository = ctx.getBean(TestRepository.class);

    ReferencedObject referencedObject = new ReferencedObject();

    // When
    mongoRepository.save(referencedObject);

    // Then
    Assert.assertEquals(referencedObject, mongoRepository.findOne(referencedObject.getId()));
  }

  @Test
  public void testMongoRepositoryFindAll() {
    // Given
    ApplicationContext ctx = new AnnotationConfigApplicationContext(MongoConfig.class);
    TestRepository mongoRepository = ctx.getBean(TestRepository.class);
    mongoRepository.save(new ReferencedObject("a"));
    mongoRepository.save(new ReferencedObject("b"));
    mongoRepository.save(new ReferencedObject("c"));
    mongoRepository.save(new ReferencedObject("d"));

    // When
    Page<ReferencedObject> result = mongoRepository.findAll(new PageRequest(0, 10, Sort.Direction.DESC, "_id"));

    // Then
    assertEquals(Arrays.asList(
        new ReferencedObject("d"),
        new ReferencedObject("c"),
        new ReferencedObject("b"),
        new ReferencedObject("a")), result.getContent());
  }

  @Test
  public void testMongoRepositoryFindAllSortId() {
    // Given
    ApplicationContext ctx = new AnnotationConfigApplicationContext(MongoConfig.class);
    TestRepository mongoRepository = ctx.getBean(TestRepository.class);
    mongoRepository.save(new ReferencedObject("d"));
    mongoRepository.save(new ReferencedObject("c"));
    mongoRepository.save(new ReferencedObject("b"));
    mongoRepository.save(new ReferencedObject("a"));

    // When
    List<ReferencedObject> result = new ArrayList<ReferencedObject>(mongoRepository.findAll(new Sort("_id")));

    // Then
    assertEquals(Arrays.asList(
        new ReferencedObject("a"),
        new ReferencedObject("b"),
        new ReferencedObject("c"),
        new ReferencedObject("d")), result);
  }

  @Configuration
  @EnableMongoRepositories
  public static class MongoConfig extends AbstractMongoConfiguration {

    @Override
    protected String getDatabaseName() {
      return "db";
    }

    @Override
    @Bean
    public Mongo mongo() throws Exception {
      return new Fongo("spring-test").getMongo();
    }

    @Bean
    protected String getMappingBasePackage() {
      return TestRepository.class.getPackage().getName();
    }
  }

  @Document
  public static class ReferencedObject implements Serializable, Identifiable<String> {

    private static final long serialVersionUID = 1L;
    @Id
    private String id;

    public ReferencedObject() {
    }

    public ReferencedObject(String id) {
      this.id = id;
    }

    @Override
    public String getId() {
      return this.id;
    }

    @Override
    public String toString() {
      return "ReferencedObject{" +
          "id='" + id + '\'' +
          '}';
    }

    public void setId(String id) {
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ReferencedObject)) return false;

      ReferencedObject that = (ReferencedObject) o;

      if (id != null ? !id.equals(that.id) : that.id != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      return id != null ? id.hashCode() : 0;
    }
  }

  @Document
  public static class MainObject implements Serializable, Identifiable<String> {

    private static final long serialVersionUID = 1L;

    @Id
    private String id;

    @DBRef
    private ReferencedObject referencedObject;

    @Override
    public String getId() {
      return this.id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public ReferencedObject getReferencedObject() {
      return referencedObject;
    }

    public void setReferencedObject(ReferencedObject referencedObject) {
      this.referencedObject = referencedObject;
    }
  }

  @Document
  public class GeoSpatialIndexedWrapper {
    @Id
    private String id;

    @GeoSpatialIndexed
    private double[] geo = new double[]{0D, 0D};

    public String getId() {
      return id;
    }

    public void setId(String id) {
      this.id = id;
    }

    public double[] getGeo() {
      return geo;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof GeoSpatialIndexedWrapper)) return false;

      GeoSpatialIndexedWrapper that = (GeoSpatialIndexedWrapper) o;

      if (!id.equals(that.id)) return false;
      if (!Arrays.equals(geo, that.geo)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = id.hashCode();
      result = 31 * result + Arrays.hashCode(geo);
      return result;
    }

    public void setGeo(double[] geo) {
      this.geo = geo;
    }

  }
}
