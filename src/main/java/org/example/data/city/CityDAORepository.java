package org.example.data.city;

import org.example.data.MyDataSource;
import org.example.entity.City;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CityDAORepository implements CityDAO {

    private static final String FIND_BY_ID = "SELECT * from city WHERE id = ?";
    private static final String FIND_BY_CODE = "SELECT * FROM city WHERE CountryCode = ?";
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
        return result;
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
