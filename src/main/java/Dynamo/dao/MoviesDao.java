package Dynamo.dao;

import java.util.*;

import Dynamo.model.Movies;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.*;


public interface MoviesDao {

    void deleteMovie( String ID);

    void updateMovie( Movies movie);

    void putMovie( Movies movie);

    List<Movies> findMovieById( String filter);

    List<Movies> findAll();

    List<Movies> findMovieByCategory( String filter);

    void deleteTable();

    ListTablesResult getTables();

    Table createTable(String Name);

    void getTableInformation();


}














