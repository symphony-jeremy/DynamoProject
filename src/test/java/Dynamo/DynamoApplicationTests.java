package Dynamo;

import Dynamo.dao.MoviesDao;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class DynamoApplicationTests {

    private static DynamoDBProxyServer server;

    @Autowired
    private MoviesDao moviesDao;

    @BeforeAll
    public static void setupClass() throws Exception {
        System.setProperty("sqlite4java.library.path", "native-libs");
        String port = "8000";
        server = ServerRunner.createServerFromCommandLineArgs(
                new String[]{"-inMemory", "-port", port});
        System.out.println("starting dynamoDB...");
        server.start();
        System.out.println("starting dynamoDB [DONE]");

    }

    @AfterAll
    public static void teardownClass() throws Exception {
        System.out.println("stopping dynamoDB ...");
        server.stop();
        System.out.println("stopping dynamoDB [DONE]");
    }

    @BeforeEach
    public void before(){
        moviesDao.createTable("Movies");
    }

    @AfterEach
    public void after(){
        moviesDao.deleteTable();
    }



}
