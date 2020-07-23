package Dynamo;

import Dynamo.dao.MoviesDao;
import Dynamo.model.Movies;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.junit.Assert.*;


public class MoviesDaoTest extends DynamoApplicationTests {
    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
            .build();

    static DynamoDB dynamoDB = new DynamoDB(client);

    @Autowired
    private MoviesDao moviesDao;


    @Before
    public void setUp() {
        moviesDao.createtable("Movies");


    }

    @Test
    void createTableTest() {
        TableDescription tableDescription = dynamoDB.getTable("Movies").describe();
        assertEquals("ACTIVE", tableDescription.getTableStatus());


    }

    @Test
    void putItemTest() {
        TableDescription tableDescription = dynamoDB.getTable("Movies").describe();
        assertEquals("ACTIVE", tableDescription.getTableStatus());
        Movies movie = new Movies("1", "Forte", "Comédie", "2020", "France");
        moviesDao.putItem("Movies", movie);
        Movies movie1 = new Movies("2", "Limitless", "Action", "2011", "US");
        moviesDao.putItem("Movies", movie1);
        assertEquals(2, tableDescription.getItemCount().intValue());

    }

    @Test
    void filterItemTest() {
        List<Movies> items = moviesDao.filterTableWithCategory("Movies", "Action");
        System.out.println("hey" + items);
        assertEquals(1, items.size());

    }

    @Test
    void filterItemwithIdTest() {
        List<Movies> items = moviesDao.filterTableWithID("Movies", "2");
        assertEquals("Action", items.get(0).getCategory());

    }

    @Test
    void deleteItemTest() {
        moviesDao.deleteItem("Movies", "2");
        TableDescription tableDescription = dynamoDB.getTable("Movies").describe();

        assertEquals(1, tableDescription.getItemCount().intValue());


    }

    @Test
    void getallitemsTest() {
        moviesDao.getAllItems("Movies");
    }


}