package com.github.fakemongo.impl.geo;

/**
 * Encapsulate external library if we need to remove her.
 */
public class LatLong extends com.github.davidmoten.geo.LatLong implements Comparable<LatLong> {

  public LatLong(double lat, double lon) {
    super(lat, lon);
  }

  public LatLong(com.github.davidmoten.geo.LatLong latLong) {
    super(latLong.getLat(), latLong.getLon());
  }

  @Override
  public int compareTo(LatLong o) {
    int cLat = Double.compare(this.getLat(), o.getLat());
    if (cLat == 0) {
      return Double.compare(this.getLon(), o.getLon());
    }
    return cLat;
  }
}
