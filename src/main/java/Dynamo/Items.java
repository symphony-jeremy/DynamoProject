package Dynamo;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;

import java.util.HashMap;
import java.util.Map;

public class Items {




    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
            .build();

    static DynamoDB dynamoDB = new DynamoDB(client);

    // add items to the table
    void putItem( String Name, int ID_Job , String key ,String Titre, String att1 , String att2 , String name1 , int name2){
        Table table = dynamoDB.getTable(Name);

        try {
            Item item = new Item()
                    .withPrimaryKey("ID", ID_Job)
                    .withString(key, Titre)
                    .withString(att1, name1)
                    .withInt(att2, name2);

            table.putItem(item);


        } catch (Exception e) {
            System.err.println("Cannot create items.");
            System.err.println(e.getMessage());
        }

    }

    void updateItem(String name){

        /* Create an Object of UpdateItemRequest */
        UpdateItemRequest request = new UpdateItemRequest();

        /* Setting Table Name */
        request.setTableName(name);

        /* Setting Consumed Capacity */
        request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);

        /* To get old value of item's attributes before it is updated */
        request.setReturnValues(ReturnValue.UPDATED_OLD);

        /* Create a Map of Primary Key attributes */
        Map<String, AttributeValue> keysMap = new HashMap<>();
        keysMap.put("ID", (new AttributeValue()).withN("4"));
        keysMap.put("Titres", (new AttributeValue("New book")));
        request.setKey(keysMap);

        /* Create a Map of attributes to be updated */
        Map<String, AttributeValueUpdate> map = new HashMap<>();
        map.put("Categories", new AttributeValueUpdate(new AttributeValue("Romance"),"PUT"));
        request.setAttributeUpdates(map);

        try {
            /* Send Update Item Request */
            UpdateItemResult result = client.updateItem(request);

            System.out.println("Status : " + result.getSdkHttpMetadata().getHttpStatusCode());

            System.out.println("Consumed Capacity : " + result.getConsumedCapacity().getCapacityUnits());

            /* Printing Old Attributes Name and Values */
            if (result.getAttributes() != null) {
                result.getAttributes().entrySet().stream()
                        .forEach(e -> System.out.println(e.getKey() + " " + e.getValue()));
            }

        } catch (AmazonServiceException e) {

            System.out.println(e.getErrorMessage());

        }

    }
    void deleteItem(String Name , int ID , String name){
        /* Create an Object of DeleteItemRequest */
        DeleteItemRequest request = new DeleteItemRequest();

        /* Setting Table Name */
        request.setTableName(Name);

        /* Setting Consumed Capacity */
        request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);

        /* To get old value of item's attributes before it is deleted */
        request.setReturnValues(ReturnValue.ALL_OLD);

        /* Create a Map of Primary Key attributes */
        Map<String, AttributeValue> keysMap = new HashMap<>();
        keysMap.put("ID", (new AttributeValue()).withN(String.valueOf(ID)));
        keysMap.put("Name", (new AttributeValue(name)));
        request.setKey(keysMap);

        try {
            /* Send Delete Item Request */
            DeleteItemResult result = client.deleteItem(request);


            System.out.println("Consumed Capacity : " +  result.getConsumedCapacity().getCapacityUnits());

            /* Printing Old Attributes Name and Values */
            if(result.getAttributes() != null) {
                result.getAttributes().entrySet().stream()
                        .forEach( e -> System.out.println(e.getKey() + " " + e.getValue()));
            }

        } catch (AmazonServiceException e) {

            System.out.println(e.getErrorMessage());

        }
    }




}
