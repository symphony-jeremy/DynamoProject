package Dynamo;

import Dynamo.dao.MoviesDao;
import Dynamo.model.Movies;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class MoviesDaoTest extends DynamoApplicationTests {

    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("test", "test")))
            .build();

    static DynamoDB dynamoDB = new DynamoDB(client);
    @Autowired
    private MoviesDao moviesDao;

    @BeforeEach
    public void init(){
        // insert some data

        // insert 2 movies
        Movies movie = new Movies("1", "Forte", "Comédie", "2020", "France");
        moviesDao.putMovie( movie);
        Movies movie1 = new Movies("2", "Limitless", "Action", "2011", "US");
        moviesDao.putMovie(movie1);

        moviesDao.deleteMovie("1");
    }

    @Test
    void getAllItemsTest() {
        List<Movies> movies = moviesDao.findAll();
        assertNotNull(movies);
        assertEquals(1, movies.size());
    }


}
