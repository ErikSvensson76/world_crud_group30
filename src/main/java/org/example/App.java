package org.example;

import org.example.data.city.CityDAO;
import org.example.data.city.CityDAORepository;

public class App
{
    public static void main( String[] args ) {

        CityDAO dao = new CityDAORepository();

        System.out.println(dao.findById(600));


    }
}
