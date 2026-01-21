package com.supercode.infra.cache.pojo;

import java.util.Objects;

public class GeoCoord {

    private double longitude;
    private double latitude;

    public GeoCoord(double longitude, double latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GeoCoord)) {
            return false;
        }
        GeoCoord geoCoord = (GeoCoord) o;
        return Double.compare(geoCoord.longitude, longitude) == 0 &&
                Double.compare(geoCoord.latitude, latitude) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(longitude, latitude);
    }
}
