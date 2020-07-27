package Dynamo;

import Dynamo.dao.MoviesDao;
import Dynamo.model.Movies;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class MoviesDaoTest extends DynamoApplicationTests {

    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
            .build();

    static DynamoDB dynamoDB = new DynamoDB(client);

    @Autowired
    private MoviesDao moviesDao;


    @BeforeEach
    public void setUp() {
        // recreate the table before each tests
        moviesDao.deleteTable("Movies");
        moviesDao.createTable("Movies");
<<<<<<< HEAD


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

        assertEquals(4, tableDescription.getItemCount().intValue());

    }

    @Test
    void filterItemTest() {
        List<Movies> items = moviesDao.findMovieByCategory("Movies", "Action");
        System.out.println("hey" + items);
        assertEquals(3, items.size());

=======
        // insert 2 movies
        Movies movie = new Movies("1", "Forte", "Comédie", "2020", "France");
        moviesDao.putMovie("Movies", movie);
        Movies movie1 = new Movies("2", "Limitless", "Action", "2011", "US");
        moviesDao.putMovie("Movies", movie1);
>>>>>>> origin/master
    }

    @Test
    void getAllItemsTest() {
        List<Movies> movies = moviesDao.findAll("Movies");
        assertNotNull(movies);
        assertEquals(2, movies.size());
    }

<<<<<<< HEAD
    @Test
    void deleteItemTest() {
        TableDescription tableDescription = dynamoDB.getTable("Movies").describe();

        assertEquals(4, tableDescription.getItemCount().intValue());


    }

    @Test
    void getallitemsTest() {
        moviesDao.findAll("Movies");
    }


=======
>>>>>>> origin/master
}
