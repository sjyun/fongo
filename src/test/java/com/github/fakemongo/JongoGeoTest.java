package com.github.fakemongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fakemongo.junit.FongoRule;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.geojson.Feature;
import org.geojson.FeatureCollection;
import org.geojson.GeoJsonObject;
import org.jongo.Jongo;
import org.jongo.MongoCollection;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

public class JongoGeoTest {

  public final FongoRule fongoRule = new FongoRule(!true);

  public final ExpectedException exception = ExpectedException.none();

  @Rule
  public RuleChain ruleChain = RuleChain.outerRule(exception).around(fongoRule);

  private Jongo jongo;
  private MongoCollection collection;

  public static class JongoGeo {
    private GeoJsonObject geoJsonObject;

    private Map<String, Object> properties;

    private JongoGeo() {
    }

    public JongoGeo(GeoJsonObject geoJsonObject, Map<String, Object> properties) {
      this.geoJsonObject = geoJsonObject;
      this.properties = properties;
    }
  }

  @Before
  public void before() throws IOException {
    this.jongo = new Jongo(fongoRule.getDB());
    this.collection = jongo.getCollection("test").withWriteConcern(WriteConcern.UNACKNOWLEDGED);
    InputStream inputStream = JongoGeoTest.class.getResourceAsStream("/correspondance-code-insee-code-postal.geojson");
    this.collection.insert(FluentIterable.from(new ObjectMapper().readValue(inputStream, FeatureCollection.class).getFeatures()).transform(new Function<Feature, JongoGeo>() {
      @Override
      public JongoGeo apply(Feature input) {
        return new JongoGeo(input.getGeometry(), input.getProperties());
      }
    }).toArray(JongoGeo.class));
    inputStream.close();
  }

  @Test
  @Ignore
  public void should_throw_exception_when_no_index_found() throws IOException {
    // Given
    exception.expect(MongoException.class);

    // When
    this.collection.findOne("{geoJsonObject:{$near:{$geometry:{type:'Point', coordinates:[#,#]}}}}",
        0.0966905733575, 48.9903291551).as(JongoGeo.class);

    // Then
  }

  @Test
  public void should_save_geojson_and_retrieve() throws IOException, InterruptedException {
    // Given
    this.collection.ensureIndex("{geoJsonObject:'2dsphere'}");

    // When
    final JongoGeo jongoGeo = this.collection.findOne("{geoJsonObject:{$near:{$geometry:{type:'Point', coordinates:[#,#]}}}}",
        0.0966905733575, 48.9903291551).as(JongoGeo.class);

    // Then
    Assertions.assertThat(jongoGeo).isNotNull();
    Assertions.assertThat(jongoGeo.properties.get("nom_region")).isEqualTo("BASSE-NORMANDIE");
  }
}