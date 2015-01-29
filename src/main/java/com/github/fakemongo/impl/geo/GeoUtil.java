package com.github.fakemongo.impl.geo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fakemongo.impl.ExpressionParser;
import com.github.fakemongo.impl.Util;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.operation.distance.DistanceOp;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.geojson.GeoJsonObject;
import org.geojson.LngLatAlt;
import org.geojson.Point;
import org.geojson.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GeoUtil {
  private static final Logger LOG = LoggerFactory.getLogger(GeoUtil.class);

  public static final double EARTH_RADIUS = 6374892.5; // common way : 6378100D;

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  private GeoUtil() {
  }

  public static class GeoDBObject extends BasicDBObject {
    private final LatLong latLong;
    private final Geometry geometry;

    public GeoDBObject(DBObject object, String indexKey) {
      List<LatLong> latLongs = GeoUtil.latLon(Util.split(indexKey), object);
      this.latLong = latLongs.get(0);
      this.geometry = GeoUtil.toGeometry(Util.extractField(object, indexKey));
      this.putAll(object);
    }

    public LatLong getLatLong() {
      return latLong;
    }

    public Geometry getGeometry() {
      return geometry;
    }

    @Override
    public int hashCode() {
      return geometry.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof GeoDBObject)) return false;

      GeoDBObject that = (GeoDBObject) o;

      return geometry.equals(that.geometry);
    }

    @Override
    public String toString() {
      return "GeoDBObject{" +
          "geometry='" + geometry + '\'' +
          ", latLong=" + getLatLong() +
          '}';
    }
  }

  public static boolean geowithin(LatLong p1, Geometry geometry) {
    Coordinate coordinate = new Coordinate(p1.getLon(), p1.getLat());
    return DistanceOp.isWithinDistance(createGeometryPoint(coordinate), geometry, 0D);
  }

  public static com.vividsolutions.jts.geom.Point createGeometryPoint(Coordinate coordinate) {
    return GEOMETRY_FACTORY.createPoint(coordinate);
  }

  public static double distanceInRadians(Geometry p1, Geometry p2, boolean spherical) {
    final Coordinate[] coordinates = DistanceOp.nearestPoints(p1, p2);

    return GeoUtil.distanceInRadians(coordinates[0], coordinates[1], spherical);
  }

  public static double distanceInRadians(Coordinate p1, Coordinate p2, boolean spherical) {
    double distance;
    if (spherical) {
      distance = distanceSpherical(p1, p2);
    } else {
      distance = distance2d(p1, p2);
    }
    return distance;
  }

  // Take me a day before I see this : https://github.com/mongodb/mongo/blob/ba239918c950c254056bf589a943a5e88fd4144c/src/mongo/db/geo/shapes.cpp
  public static double distance2d(Coordinate p1, Coordinate p2) {
    double a = p1.x - p2.x;
    double b = p1.y - p2.y;

    // Avoid numerical error if possible...
    if (a == 0) return Math.abs(b);
    if (b == 0) return Math.abs(a);

    return Math.sqrt((a * a) + (b * b));
  }

  public static double distanceSpherical(Coordinate p1, Coordinate p2) {
    double p1lat = Math.toRadians(p1.x); // e
    double p1long = Math.toRadians(p1.y);    // f
    double p2lat = Math.toRadians(p2.x);         // g
    double p2long = Math.toRadians(p2.y);             // h

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
      LatLong latLong = latLong(value);
      if (latLong != null) {
        result.add(latLong);
      }
    }
    return result;
  }

  public static LatLong latLong(Object value) {
    LatLong latLong = null;
    if (value instanceof List) {
      List list = (List) value;
      if (list.size() == 2) {
        latLong = new LatLong(((Number) list.get(1)).doubleValue(), ((Number) list.get(0)).doubleValue());
      }
    } else if (value instanceof DBObject) {
      DBObject dbObject = (DBObject) value;
      if (dbObject.containsField("type")) {
        // GeoJSON
        try {
          GeoJsonObject object = new ObjectMapper().readValue(JSON.serialize(value), GeoJsonObject.class);
          if (object instanceof Point) {
            Point point = (Point) object;
            latLong = new LatLong(point.getCoordinates().getLatitude(), point.getCoordinates().getLongitude());
          } else if (object instanceof Polygon) {
            Polygon point = (Polygon) object;
            latLong = new LatLong(point.getCoordinates().get(0).get(0).getLatitude(), point.getCoordinates().get(0).get(0).getLongitude());
          } else {
            throw new IllegalArgumentException("type " + object + " not correctly handle in Fongo");
          }
        } catch (IOException e) {
          LOG.warn("don't kown how to handle " + value);
        }
      } else if (dbObject.containsField("lng") && dbObject.containsField("lat")) {
        latLong = new LatLong(((Number) dbObject.get("lat")).doubleValue(), ((Number) dbObject.get("lng")).doubleValue());
      } else if (dbObject.containsField("x") && dbObject.containsField("y")) {
        latLong = new LatLong(((Number) dbObject.get("x")).doubleValue(), ((Number) dbObject.get("y")).doubleValue());
      } else if (dbObject.containsField("latitude") && dbObject.containsField("longitude")) {
        latLong = new LatLong(((Number) dbObject.get("latitude")).doubleValue(), ((Number) dbObject.get("longitude")).doubleValue());
      }
    } else if (value instanceof double[]) {
      double[] array = (double[]) value;
      if (array.length >= 2) {
        latLong = new LatLong(((Number) array[0]).doubleValue(), ((Number) array[1]).doubleValue());
      }
    }
    return latLong;
  }

  public static Geometry toGeometry(Object object) {
    if (object instanceof DBObject) {
      return toGeometry((DBObject) object);
    }
    return createGeometryPoint(toCoordinate(latLong(object)));
  }

  public static Geometry toGeometry(DBObject dbObject) {
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
    } else if (dbObject.containsField("type")) {
      try {
        GeoJsonObject geoJsonObject = new ObjectMapper().readValue(JSON.serialize(dbObject), GeoJsonObject.class);
        if (geoJsonObject instanceof Point) {
          Point point = (Point) geoJsonObject;
          return createGeometryPoint(toCoordinate(point.getCoordinates()));
        } else if (geoJsonObject instanceof Polygon) {
          Polygon polygon = (Polygon) geoJsonObject;
          return GEOMETRY_FACTORY.createPolygon(toCoordinates(polygon.getCoordinates()));
        }
      } catch (IOException e) {
        LOG.warn("cannot handle " + JSON.serialize(dbObject));
      }
    } else {
      LatLong latLong = latLong(dbObject);
      if (latLong != null) {
        return createGeometryPoint(toCoordinate(latLong));
      }
    }
//    if (true)
//      throw new IllegalArgumentException("can't handle " + JSON.serialize(dbObject));
    return null;
  }

  private static Coordinate[] toCoordinates(List<List<LngLatAlt>> lngLatAlts) {
    List<Coordinate> coordinates = new ArrayList<Coordinate>();
    for (List<LngLatAlt> lineStrings : lngLatAlts) {
      for (LngLatAlt lngLatAlt : lineStrings) {
        coordinates.add(toCoordinate(lngLatAlt));
      }
    }
    return coordinates.toArray(new Coordinate[0]);
  }

  public static Coordinate toCoordinate(LatLong lngLatAlt) {
    return new Coordinate(lngLatAlt.getLat(), lngLatAlt.getLon());
  }

  public static Coordinate toCoordinate(LngLatAlt lngLatAlt) {
    return new Coordinate(lngLatAlt.getLatitude(), lngLatAlt.getLongitude(), lngLatAlt.getAltitude());
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
      LatLong latLong = latLong(coordinates.get(i));
      ret[i] = new Coordinate(latLong.getLon(), latLong.getLat());
    }

    return ret;
  }
}
