package com.github.fakemongo.impl.geo;

/**
 * Encapsulate external library if we need to remove her.
 */
public class LatLong implements Comparable<LatLong> {
  private final double lat;
  private final double lon;

  public LatLong(double lat, double lon) {
    this.lat = lat;
    this.lon = lon;
  }

  public double getLat() {
    return this.lat;
  }

  public double getLon() {
    return this.lon;
  }

  @Override
  public int compareTo(LatLong o) {
    int cLat = Double.compare(this.getLat(), o.getLat());
    if (cLat == 0) {
      return Double.compare(this.getLon(), o.getLon());
    }
    return cLat;
  }

  public String toString() {
    return "LatLong [lat=" + this.lat + ", lon=" + this.lon + "]";
  }
}
