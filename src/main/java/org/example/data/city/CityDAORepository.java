package org.example.data.city;

import org.example.entity.City;

import java.util.List;
import java.util.Optional;

public class CityDAORepository implements CityDAO {
    @Override
    public Optional<City> findById(int id) {
        return Optional.empty();
    }

    @Override
    public List<City> findByCode(String code) {
        return null;
    }

    @Override
    public List<City> findByName(String name) {
        return null;
    }

    @Override
    public List<City> findAll() {
        return null;
    }

    @Override
    public City create(City city) {
        return null;
    }

    @Override
    public City update(City city) {
        return null;
    }

    @Override
    public int delete(City city) {
        return 0;
    }
}
