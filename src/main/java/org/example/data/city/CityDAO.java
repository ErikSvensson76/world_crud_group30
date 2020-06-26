package org.example.data.city;

import org.example.entity.City;

import java.util.List;
import java.util.Optional;

public interface CityDAO {
    Optional<City> findById(int id);
    List<City> findByCode(String code);
    List<City> findByName(String name);
    List<City> findAll();
    City create(City city);
    City update(City city);
    int delete(City city);
}
