package Dynamo.dao;

import java.util.*;

import Dynamo.model.Movies;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.*;


public interface MoviesDao {

    void deleteItem(String Name, String ID);

    void updateItem(String name, Movies movie);

    void putItem(String Name, Movies movie);

    List<Movies> filterTableWithID(String Name, String filter);

    List<Movies> getAllItems(String Name);

    List<Movies> filterTableWithCategory(String Name, String filter);

    void deleteTable(String Name);

    ListTablesResult getTables();

    Table createtable(String Name);

    void getTableInformation(String Name);


}














