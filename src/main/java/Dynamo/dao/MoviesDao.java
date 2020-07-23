package Dynamo.dao;

import java.util.*;

import Dynamo.model.Movies;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.*;


public interface MoviesDao {

    void deleteMovie(String Name, String ID);

    void updateMovie(String name, Movies movie);

    void putMovie(String Name, Movies movie);

    List<Movies> findMovieById(String Name, String filter);

    List<Movies> findAll(String Name);

    List<Movies> findMovieByCategory(String Name, String filter);

    void deleteTable(String Name);

    ListTablesResult getTables();

    Table createTable(String Name);

    void getTableInformation(String Name);


}














