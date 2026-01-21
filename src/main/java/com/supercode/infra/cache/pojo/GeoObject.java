package com.supercode.infra.cache.pojo;

import java.util.Objects;

public class GeoObject {

    private String name;
    private GeoCoord coord;

    public GeoObject(String name, GeoCoord coord) {
        this.name = name;
        this.coord = coord;
    }

    public GeoObject(String name, double longitude, double latitude) {
        this.name = name;
        this.coord = new GeoCoord(longitude, latitude);
    }

    public String getName() {
        return name;
    }

    public GeoCoord getCoord() {
        return coord;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GeoObject)) {
            return false;
        }
        GeoObject geoObject = (GeoObject) o;
        return Objects.equals(name, geoObject.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
