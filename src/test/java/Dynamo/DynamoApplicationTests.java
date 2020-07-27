package Dynamo;

import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class DynamoApplicationTests {

    private static DynamoDBProxyServer server;

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

}
