package com.github.fakemongo.impl.geo;

import com.github.davidmoten.geo.GeoHash;
import com.github.fakemongo.impl.ExpressionParser;
import com.github.fakemongo.impl.Util;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.operation.distance.DistanceOp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class GeoUtil {

  public static final double EARTH_RADIUS = 6374892.5; // common way : 6378100D;

  //  Precision, Distance of Adjacent Cell in Meters
  //      1,     5003530
  //      2,     625441
  //      3,     123264
  //      4,     19545
  //      5,     3803
  //      6,     610
  //      7,     118
  //      8,     19
  //      9,     3.71
  //      10,    0.6
  public static final int SIZE_GEOHASH = 5; // Size of the geohash. 12 = more accurate.

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  private GeoUtil() {
  }

  public static class GeoDBObject extends BasicDBObject {
    private final String geoHash;
    private final LatLong latLong;

    public GeoDBObject(DBObject object, String indexKey) {
      List<LatLong> latLongs = GeoUtil.latLon(Util.split(indexKey), object);
//      BasicDBList list = (BasicDBList) object.get(indexKey);
//      this.latLong = new LatLong((Double) list.get(1), (Double) list.get(0));
      this.latLong = latLongs.get(0);
      this.geoHash = GeoUtil.encodeGeoHash(this.getLatLong());
      this.putAll(object);
    }

    public String getGeoHash() {
      return geoHash;
    }

    public LatLong getLatLong() {
      return latLong;
    }

    @Override
    public int hashCode() {
      return getGeoHash().hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof GeoDBObject)) return false;

      GeoDBObject that = (GeoDBObject) o;

      if (!getGeoHash().equals(that.getGeoHash())) return false;

      return true;
    }

    @Override
    public String toString() {
      return "GeoDBObject{" +
          "geoHash='" + getGeoHash() + '\'' +
          ", latLong=" + getLatLong() +
          '}';
    }
  }

  public static boolean geowithin(LatLong p1, Geometry geometry) {
    Coordinate coordinate = new Coordinate(p1.getLon(), p1.getLat());
    return DistanceOp.isWithinDistance(GEOMETRY_FACTORY.createPoint(coordinate), geometry, 0D);
  }

  public static double distanceInRadians(LatLong p1, LatLong p2, boolean spherical) {
    double distance;
    if (spherical) {
      distance = distanceSpherical(p1, p2);
    } else {
      distance = distance2d(p1, p2);
    }
    return distance;
  }

  // Take me a day before I see this : https://github.com/mongodb/mongo/blob/ba239918c950c254056bf589a943a5e88fd4144c/src/mongo/db/geo/shapes.cpp
  public static double distance2d(LatLong p1, LatLong p2) {
    double a = p1.getLat() - p2.getLat();
    double b = p1.getLon() - p2.getLon();

    // Avoid numerical error if possible...
    if (a == 0) return Math.abs(b);
    if (b == 0) return Math.abs(a);

    return Math.sqrt((a * a) + (b * b));
  }

  public static double distanceSpherical(LatLong p1, LatLong p2) {
    double p1lat = Math.toRadians(p1.getLat()); // e
    double p1long = Math.toRadians(p1.getLon());    // f
    double p2lat = Math.toRadians(p2.getLat());         // g
    double p2long = Math.toRadians(p2.getLon());             // h

    double sinx1 = Math.sin(p1lat), cosx1 = Math.cos(p1lat);
    double siny1 = Math.sin(p1long), cosy1 = Math.cos(p1long);
    double sinx2 = Math.sin(p2lat), cosx2 = Math.cos(p2lat);
    double siny2 = Math.sin(p2long), cosy2 = Math.cos(p2long);

    double crossProduct = cosx1 * cosx2 * cosy1 * cosy2 + cosx1 * siny1 * cosx2 * siny2 + sinx1 * sinx2;
    if (crossProduct >= 1D || crossProduct <= -1D) {
      return crossProduct > 0 ? 0 : Math.PI;
    }

    return Math.acos(crossProduct);
  }

  /**
   * Retrieve LatLon from an object.
   * <p/>
   * Object can be:
   * - [lon, lat]
   * - {lat:lat, lng:lon}
   *
   * @param path
   * @param object
   * @return
   */
  public static List<LatLong> latLon(List<String> path, DBObject object) {
    ExpressionParser expressionParser = new ExpressionParser();
    List<LatLong> result = new ArrayList<LatLong>();

    List objects;
    if (path.isEmpty()) {
      objects = Collections.singletonList(object);
    } else {
      objects = expressionParser.getEmbeddedValues(path, object);
    }
    for (Object value : objects) {
      LatLong latLong = getLatLong(value);
      if (latLong != null) {
        result.add(latLong);
      }
    }
    return result;
  }

  public static LatLong getLatLong(Object value) {
    LatLong latLong = null;
    if (value instanceof BasicDBList) {
      BasicDBList list = (BasicDBList) value;
      if (list.size() == 2) {
        latLong = new LatLong(((Number) list.get(1)).doubleValue(), ((Number) list.get(0)).doubleValue());
      }
    } else if (value instanceof DBObject) {
      DBObject dbObject = (DBObject) value;
      if (dbObject.containsField("lng") && dbObject.containsField("lat")) {
        latLong = new LatLong(((Number) dbObject.get("lat")).doubleValue(), ((Number) dbObject.get("lng")).doubleValue());
      } else if (dbObject.containsField("x") && dbObject.containsField("y")) {
        latLong = new LatLong(((Number) dbObject.get("x")).doubleValue(), ((Number) dbObject.get("y")).doubleValue());
      } else if (dbObject.containsField("latitude") && dbObject.containsField("longitude")) {
        latLong = new LatLong(((Number) dbObject.get("latitude")).doubleValue(), ((Number) dbObject.get("longitude")).doubleValue());
      }
    } else if (value instanceof double[]) {
      double[] array = (double[]) value;
      if (array.length == 2) {
        latLong = new LatLong(((Number) array[0]).doubleValue(), ((Number) array[1]).doubleValue());
      }
    }
    return latLong;
  }

  public static Geometry getGeometry(DBObject dbObject) {
    if (dbObject.containsField("$box")) {
      BasicDBList coordinates = (BasicDBList) dbObject.get("$box");
      return createBox(coordinates);
    } else if (dbObject.containsField("$center")) {
      throw new IllegalArgumentException("$center not implemented in fongo");
      // TODO
    } else if (dbObject.containsField("$centerSphere")) {
      throw new IllegalArgumentException("$centerSphere not implemented in fongo");
      // TODO
    } else if (dbObject.containsField("$polygon")) {
      BasicDBList coordinates = (BasicDBList) dbObject.get("$polygon");
      return createPolygon(coordinates);
      // TODO
    } else if (dbObject.containsField("$geometry")) {
      String type = (String) dbObject.get("type");
      BasicDBList coordinates = (BasicDBList) dbObject.get("$coordinates");
      throw new IllegalArgumentException("$geometry not implemented in fongo");
      // TODO
    }
    return null;
  }

  private static Geometry createBox(BasicDBList coordinates) {
    Coordinate[] t = parseCoordinates(coordinates);

    return GEOMETRY_FACTORY.toGeometry(new Envelope(t[0], t[1]));
  }

  private static Geometry createPolygon(BasicDBList coordinates) {
    Coordinate[] t = parseCoordinates(coordinates);
    if (!t[0].equals(t[t.length - 1])) {
      Coordinate[] another = new Coordinate[t.length + 1];
      System.arraycopy(t, 0, another, 0, t.length);
      another[t.length] = t[0];
      t = another;
    }

    return GEOMETRY_FACTORY.createPolygon(t);
  }

  private static Coordinate[] parseCoordinates(BasicDBList coordinates) {
    Coordinate[] ret = new Coordinate[coordinates.size()];
    for (int i = 0, length = coordinates.size(); i < length; i++) {
      LatLong latLong = getLatLong(coordinates.get(i));
      ret[i] = new Coordinate(latLong.getLon(), latLong.getLat());
    }

    return ret;
  }

  public static String encodeGeoHash(LatLong latLong) {
    return encodeGeoHash(latLong, SIZE_GEOHASH);
  }

  public static String encodeGeoHash(LatLong latLong, int sizeHash) {
    return GeoHash.encodeHash(latLong, sizeHash); // The more, the merrier.
  }

  public static LatLong decodeGeoHash(String geoHash) {
    return new LatLong(GeoHash.decodeHash(geoHash));
  }

  public static List<String> neightbours(String geoHash) {
    List<String> results = new ArrayList<String>();
    try {
      results.add(GeoHash.left(geoHash));
    } catch (Exception e) {
    }
    try {
      results.add(GeoHash.right(geoHash));
    } catch (Exception e) {
    }
    try {
      results.add(GeoHash.top(geoHash));
    } catch (Exception e) {
    }
    try {
      results.add(GeoHash.bottom(geoHash));
    } catch (Exception e) {
    }
    return results;
  }

}
