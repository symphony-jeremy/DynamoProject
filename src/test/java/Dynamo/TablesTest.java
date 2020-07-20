package Dynamo;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import static org.junit.Assert.*;




public class TablesTest extends  DynamoApplicationTests{
    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
            .build();

    static DynamoDB dynamoDB = new DynamoDB(client);
    Tables tables= new Tables();
    Items items = new Items();

    @Before
    public void setUp(){
        tables.createtable("Jobs", "Name");

    }

    @Test
    void createTableTest(){
    // creation of a table to test if it's created or not
        TableDescription tableDescription = dynamoDB.getTable("BookStore").describe();
        assertEquals(0,tableDescription.getItemCount().intValue());
        assertEquals("ACTIVE", tableDescription.getTableStatus());


    }

    @Test
    void putItemTest(){
        TableDescription tableDescription = dynamoDB.getTable("Jobs").describe();
        assertEquals("ACTIVE", tableDescription.getTableStatus());
        items.putItem("Jobs",1, "Name" , "Boulanger" , "Ville", "Distance" , "Nice",500);
        items.putItem("Jobs",2, "Name" , "Police" , "Ville", "Distance" , "Paris",1000);
        assertEquals(4, tableDescription.getItemCount().intValue());

    }
    @Test
    void filterItemTest(){
       ItemCollection<ScanOutcome> items= tables.filterTableWithDistance("Jobs");
        assertEquals(2, items.getAccumulatedItemCount());

    }
    @Test
    void getItemsTest(){
        GetItemResult items = tables.getItems("Jobs", 1 ,"Boulanger");
        System.out.println("Hello"+ items);
        assertEquals("500", items.getItem().values().toArray()[1].toString().substring(4,7));
    }
    @Test
    void deleteItemTest()
    {
        items.deleteItem("Jobs", 2, "Police");
        TableDescription tableDescription = dynamoDB.getTable("Jobs").describe();

        assertEquals(3 , tableDescription.getItemCount().intValue());


    }

}
