package org.example.data.city;

import org.example.data.MyDataSource;
import org.example.entity.City;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CityDAORepository implements CityDAO {

    public static final String FIND_BY_ID = "SELECT * from city WHERE id = ?";
    public static final String FIND_BY_CODE = "SELECT * FROM city WHERE CountryCode = ?";
    public static final String FIND_BY_NAME_LIKE = "SELECT * FROM city WHERE name LIKE ?";

    @Override
    public Optional<City> findById(int id) {
        Optional<City> cityOptional = Optional.empty();

        try(Connection connection = MyDataSource.getConnection();
            PreparedStatement statement = createFindByIdStatement(connection, FIND_BY_ID, id);
            ResultSet resultSet = statement.executeQuery()){

            while(resultSet.next()){
                cityOptional = Optional.of(createCityFromResultSet(resultSet));
            }

        }catch (SQLException ex){
            ex.printStackTrace();
        }

        return cityOptional;
    }

    private City createCityFromResultSet(ResultSet resultSet) throws SQLException {
        return new City(
                resultSet.getInt("ID"),
                resultSet.getString("Name"),
                resultSet.getString("CountryCode"),
                resultSet.getString("District"),
                resultSet.getInt("Population")
        );
    }

    private PreparedStatement createFindByIdStatement(Connection connection, String findById, int id) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(FIND_BY_ID);
        statement.setInt(1, id);
        return statement;
    }

    @Override
    public List<City> findByCode(String code) {
        List<City> result = new ArrayList<>();

        try(Connection connection = MyDataSource.getConnection();
        PreparedStatement statement = createFindByCodeStatement(connection, FIND_BY_CODE, code);
        ResultSet resultSet = statement.executeQuery()){

            while(resultSet.next()){
                result.add(createCityFromResultSet(resultSet));
            }

        }catch (SQLException ex){
            ex.printStackTrace();
        }
        return result;
    }

    private PreparedStatement createFindByCodeStatement(Connection connection, String findByCode, String code) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(findByCode);
        statement.setString(1, code.toUpperCase());
        return statement;
    }

    @Override
    public List<City> findByName(String name) {
        List<City> result = new ArrayList<>();
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try{
            connection = MyDataSource.getConnection();
            statement = connection.prepareStatement(FIND_BY_NAME_LIKE);
            statement.setString(1, name.concat("%"));
            resultSet = statement.executeQuery();

            while (resultSet.next()){
                result.add(createCityFromResultSet(resultSet));
            }
        }catch (SQLException ex){
            ex.printStackTrace();
        }finally {
            closeResources(connection, statement, resultSet);
        }
        return result;
    }

    @Override
    public List<City> findAll() {
        List<City> result = new ArrayList<>();
        try(Connection connection = MyDataSource.getConnection();
            PreparedStatement statement = connection.prepareStatement("SELECT * FROM city");
            ResultSet resultSet = statement.executeQuery()){

            while(resultSet.next()){
                result.add(createCityFromResultSet(resultSet));
            }

        }catch (SQLException exception){
            exception.printStackTrace();
        }
        return result;
    }

    @Override
    public City create(City city) {
        if(city.getId() != 0){
            throw new IllegalArgumentException("Exception because param city had invalid id " + city.getId());
        }
        City persisted = null;
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet keySet = null;
        try{
            connection = MyDataSource.getConnection();
            statement = connection.prepareStatement("INSERT INTO city (Name, CountryCode, District, Population) VALUES (?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, city.getName());
            statement.setString(2, city.getCountryCode());
            statement.setString(3, city.getDistrict());
            statement.setInt(4, city.getPopulation());
            statement.execute();

            keySet = statement.getGeneratedKeys();
            while(keySet.next()){
                persisted = new City(
                        keySet.getInt(1),
                        city.getName(),
                        city.getCountryCode(),
                        city.getDistrict(),
                        city.getPopulation()
                );
            }
        } catch (SQLException exception) {
            exception.printStackTrace();
        }finally {
            closeResources(connection, statement, keySet);
        }
        return persisted == null ? city : persisted;
    }

    private void closeResources(Connection connection, PreparedStatement statement, ResultSet resultSet) {
        try{
            if(resultSet != null){
                resultSet.close();
            }
            if(statement != null){
                statement.close();
            }
            if(connection != null){
                connection.close();
            }
        }catch (SQLException ex){
            ex.printStackTrace();
        }
    }


    /*
    UPDATE table_name
    SET column1 = value1, column2 = value2, ...
    WHERE condition;
     */
    public static final String UPDATE_CITY = "UPDATE city SET Name = ?, CountryCode = ?, District = ?, Population = ? WHERE ID = ?";

    @Override
    public City update(City city) {
        if(city.getId() == 0){
            throw new IllegalArgumentException("City cant be updated because it is not yet persisted");
        }
        try(Connection connection = MyDataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement(UPDATE_CITY)){

            statement.setString(1, city.getName());
            statement.setString(2, city.getCountryCode());
            statement.setString(3, city.getDistrict());
            statement.setInt(4, city.getPopulation());
            statement.setInt(5, city.getId());

            statement.execute();

        }catch (SQLException ex){
            ex.printStackTrace();
        }
        return city;
    }

    /*
    DELETE FROM table_name WHERE condition;
     */
    public static final String DELETE_CITY = "DELETE FROM city WHERE ID = ?";
    @Override
    public int delete(City city) {
       int rowsAffected = 0;
       try(Connection connection = MyDataSource.getConnection();
       PreparedStatement statement = connection.prepareStatement(DELETE_CITY)){
           statement.setInt(1, city.getId());
           rowsAffected = statement.executeUpdate();

       }catch (SQLException ex){
           ex.printStackTrace();
       }
       return rowsAffected;
    }
}
