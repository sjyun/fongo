package com.github.fakemongo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fakemongo.impl.Util;
import com.github.fakemongo.junit.FongoRule;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DBObject;
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

    private Integer _id;

    private GeoJsonObject geoJsonObject;

    private Map<String, Object> properties;

    private JongoGeo() {
    }

    public JongoGeo(int _id, GeoJsonObject geoJsonObject, Map<String, Object> properties) {
      this._id = _id;
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
      int i = 1;

      @Override
      public JongoGeo apply(Feature input) {
        return new JongoGeo(i++, input.getGeometry(), input.getProperties());
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

  @Test
  public void should_save_geojson_and_retrieve_it_in_limit_of_two_zone() throws IOException, InterruptedException {
    // Given
    this.collection.ensureIndex("{geoJsonObject:'2dsphere'}");

    // When
    final JongoGeo jongoGeo = this.collection.findOne("{geoJsonObject:{$near:{$geometry:{type:'Point', coordinates:[#,#]}}}}",
        0.082676086940117,
        48.971015023255745).as(JongoGeo.class);

    // Then
    Assertions.assertThat(jongoGeo).isNotNull();
    Assertions.assertThat(jongoGeo.properties.get("nom_region")).isEqualTo("BASSE-NORMANDIE");
    Assertions.assertThat(jongoGeo.properties.get("insee_com")).isEqualTo("14580");
  }


  @Test
  public void should_command_geoNear_works() {
    // Given
    this.collection.ensureIndex("{geoJsonObject:'2dsphere'}");

    // geoNear
    CommandResult commandResult = fongoRule.getDB().command(new BasicDBObject("geoNear", collection.getName()).append("near", Util.list(2.265D, 48.791D)).append("spherical", true).append("limit", 2));
    commandResult.throwOnError();

    DBObject results = (DBObject) commandResult.get("results");

    Assertions.assertThat(FongoGeoTest.roundDis(results)).isEqualTo(fongoRule.parseDBObject("[ { \"dis\" : 0.008519 , \"obj\" : { \"_id\" : 9, \"geoJsonObject\" : { \"type\" : \"Polygon\" , " +
        "\"coordinates\" : [ [ [ 3.009581824037699 , 48.712390700123976] , [ 3.003398737182201 , 48.7266469900686] , [ 2.999197877033133 , 48.728014159061416] , " +
        "[ 3.013778068803887 , 48.750858478274914] , [ 3.020414797597052 , 48.74911268108732] , [ 3.035716757850915 , 48.75066505650057] , " +
        "[ 3.048809592894014 , 48.7451830313863] , [ 3.051832352440385 , 48.73511732058203] , [ 3.060883996585647 , 48.73163229634592] , " +
        "[ 3.062008480797561 , 48.72825888695353] , [ 3.062055596844271 , 48.72214277148325] , [ 3.029240281696784 , 48.72226325446333] , " +
        "[ 3.018131852753545 , 48.715672639469545] , [ 3.009581824037699 , 48.712390700123976]]]} , " +
        "\"properties\" : { \"nom_region\" : \"ILE-DE-FRANCE\" , \"nom_dept\" : \"SEINE-ET-MARNE\" , \"statut\" : \"Commune simple\" , \"code_reg\" : \"11\" , " +
        "\"code_comm\" : \"469\" , \"z_moyen\" : 110 , \"insee_com\" : \"77469\" , \"code_dept\" : \"77\" , \"geo_point_2d\" : [ 48.7330599565 , 3.02878980311] , " +
        "\"postal_code\" : \"77131\" , \"id_geofla\" : 35839 , \"code_cant\" : \"27\" , \"superficie\" : 1174 , \"nom_comm\" : \"TOUQUIN\" , \"code_arr\" : \"3\" , \"population\" : 1.1}}} ," +
        " { \"dis\" : 0.015439 , \"obj\" : { \"_id\" : 7, \"geoJsonObject\" : { \"type\" : \"Polygon\" , " +
        "\"coordinates\" : [ [ [ 3.312985670258975 , 49.35534039677243] , [ 3.315282334513571 , 49.36186932587646] , [ 3.312229572838674 , 49.36455591381193] , " +
        "[ 3.298727918057014 , 49.36930913093956] , [ 3.288850703516769 , 49.367858975245355] , [ 3.29938667090546 , 49.37845800009276] , [ 3.298083207700087 , 49.38159825603996] , " +
        "[ 3.306748067097461 , 49.39168874551181] , [ 3.315241970237687 , 49.39579263877373] , [ 3.325202975693527 , 49.39645838737316] , [ 3.33397998593156 , 49.392892999959905] , " +
        "[ 3.343883670798671 , 49.39326973675718] , [ 3.350965183740227 , 49.397797589419525] , [ 3.355334265519143 , 49.39628375577655] , [ 3.357789672352293 , 49.394883298304535] , " +
        "[ 3.34959085871579 , 49.37995024590531] , [ 3.340214619644688 , 49.37775678323064] , [ 3.338597462701098 , 49.374678198577826] , [ 3.347817766459731 , 49.37221608467359] , " +
        "[ 3.350752737837221 , 49.36253567080708] , [ 3.329477733705776 , 49.36931913868136] , [ 3.322932783702096 , 49.35723785255026] , [ 3.312985670258975 , 49.35534039677243]]]} ," +
        " \"properties\" : { \"nom_region\" : \"PICARDIE\" , \"nom_dept\" : \"AISNE\" , \"statut\" : \"Sous-préfecture\" , \"code_reg\" : \"22\" , \"code_comm\" : \"722\" , \"z_moyen\" : 49 , " +
        "\"insee_com\" : \"02722\" , \"code_dept\" : \"02\" , \"geo_point_2d\" : [ 49.3791742979 , 3.32471758491] , \"postal_code\" : \"02200\" , \"id_geofla\" : 23385 , \"code_cant\" : \"99\" ," +
        " \"superficie\" : 1233 , \"nom_comm\" : \"SOISSONS\" , \"code_arr\" : \"4\" , \"population\" : 28.8}}}]"));
  }
}