package Dynamo;

import Dynamo.model.Movies;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import org.junit.Before;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.Assert.*;




public class MoviesDaoTest extends  DynamoApplicationTests{
    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
            .build();

    static DynamoDB dynamoDB = new DynamoDB(client);
    MoviesDao moviesDao = new MoviesDao();


    @Before
    public void setUp(){

    }

    @Test
    void createTableTest(){
             moviesDao.createtable("Movies");
        // creation of a table to test if it's created or not
        TableDescription tableDescription = dynamoDB.getTable("Movies").describe();
        assertEquals(0,tableDescription.getItemCount().intValue());
        assertEquals("ACTIVE", tableDescription.getTableStatus());


    }

    @Test
    void putItemTest(){
        TableDescription tableDescription = dynamoDB.getTable("Movies").describe();
        assertEquals("ACTIVE", tableDescription.getTableStatus());
        Movies movie = new Movies("8","Forte","Comédie","2020","France");
        moviesDao.putItem("Movies", movie);
        assertEquals(3, tableDescription.getItemCount().intValue());

    }
    @Test
    void filterItemTest(){
        List<Movies> items= moviesDao.filterTableWithCategory("Movies","Romance");
       System.out.println("hey"+ items);
       assertEquals(1,items.size());

    }
    @Test
    void filterItemwithIdTest(){
        List<Movies> items= moviesDao.filterTableWithID("Movies","2");
        assertEquals("Romance",items.get(0).getCategory());

    }
    @Test
    void getItemsTest(){
     //   GetItemResult items = tables.getItems("Jobs", 1 ,"Boulanger");

       // assertEquals("500", items.getItem().values().toArray()[1].toString().substring(4,7));
    }
    @Test
    void deleteItemTest()
    {
        moviesDao.deleteItem("Movies", "2");
        TableDescription tableDescription = dynamoDB.getTable("Movies").describe();

        assertEquals(3 , tableDescription.getItemCount().intValue());


    }
    @Test
    void getallitemsTest(){
        moviesDao.getAllItems("Jobs");
    }
    @Test
    void findbyId(){

    }

}
