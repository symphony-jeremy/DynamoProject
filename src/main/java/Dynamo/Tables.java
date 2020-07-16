package Dynamo;
import java.io.IOException;
import java.util.*;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.*;

import javax.enterprise.inject.Any;
import java.util.Arrays;
import java.util.Iterator;

public class Tables {


    static  AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
            .build();

    static DynamoDB dynamoDB = new DynamoDB(client);
    static String Name = "Films";


    public static void main(String[] args) throws Exception {


         getItems(Name);   }


    static void getTableInformation(String Name ) {

        System.out.println("Describing " + Name);

        TableDescription tableDescription = dynamoDB.getTable(Name).describe();
        System.out.format(
                "Name: %s:\n" + "Status: %s \n" + "Provisioned Throughput (read capacity units/sec): %d \n"
                        + "Provisioned Throughput (write capacity units/sec): %d \n",
                tableDescription.getTableName(), tableDescription.getTableStatus(),
                tableDescription.getProvisionedThroughput().getReadCapacityUnits(),
                tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
        System.out.println("hey "+   tableDescription.getItemCount());
    }
    public static void createtable(String Name){

        /* Create an Object of CreateTableRequest */
        CreateTableRequest request = new CreateTableRequest();

        /* Setting Table Name */
        request.setTableName(Name);

        /* Create & Set a list of AttributeDefinition */
        List<AttributeDefinition> attributeDefinitions = Arrays.asList(
                new AttributeDefinition("ID", ScalarAttributeType.N),

                new AttributeDefinition("Titres", ScalarAttributeType.S));
        request.setAttributeDefinitions(attributeDefinitions);

        /* Create & Set a list of KeySchemaElement */
        List<KeySchemaElement> keySchema = Arrays.asList(
                new KeySchemaElement("ID", KeyType.HASH),
        new KeySchemaElement("Titres", KeyType.RANGE));


        request.setKeySchema(keySchema);

        /* Setting Provisioned Throughput */
        request.setProvisionedThroughput(new ProvisionedThroughput(1L, 1L));

        try {
            /* Send Create Table Request */
            Table result = dynamoDB.createTable(request);
            System.out.println(result.toString());



            /* Creating and Sending request using Fluent API - USER Table */
            Table resultFluent = dynamoDB.createTable((new CreateTableRequest())
                    .withTableName(Name)
                    .withAttributeDefinitions(new AttributeDefinition("Id", ScalarAttributeType.N))
                    .withKeySchema(new KeySchemaElement("Id", KeyType.HASH))
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)));
            System.out.println("hey"+resultFluent);



        } catch (AmazonServiceException e) {

            System.out.println(e.getErrorMessage());

        }
    }




        static void putItem( String Name){
            Table table = dynamoDB.getTable(Name);

            try {
                Item item = new Item()
                        .withPrimaryKey("ID", 1)
                        .withString("Titres", "La vie est belle");
                table.putItem(item);

            } catch (Exception e) {
                System.err.println("Cannot create items.");
                System.err.println(e.getMessage());
            }

              }
            static void getTables(){
                try {

                    /* Creating ListTableRequest with limit 50 */
                    ListTablesRequest request = new ListTablesRequest();
                    request.withLimit(50);

                    ListTablesResult result = null;
                    String lastTable = null;

                    while(true) {

                        if(lastTable == null) {
                            /* Send First List Table Request */
                            result = client.listTables(request);
                        }else {
                            /* Send Subsequent List Table Request */
                            result = client.listTables(request.withExclusiveStartTableName(lastTable));
                        }

                        result.getTableNames().forEach(e -> {
                                    System.out.println(e);

                                }
                                                                        );

                        /* Getting name of last evaluated table */
                        lastTable = result.getLastEvaluatedTableName();
                        if(lastTable == null) {
                            break;
                        }

                    }

                } catch (AmazonServiceException e) {

                    System.out.println(e.getErrorMessage());

                }

            }


            static void deleteTable(String Name){
                DeleteTableRequest request = new DeleteTableRequest();

                /* Setting Table Name */
                request.setTableName(Name);

                try {
                    /* Send Delete Table Request */
                    DeleteTableResult result = client.deleteTable(request);

                    System.out.println("Status : " +  result.getSdkHttpMetadata().getHttpStatusCode());

                    System.out.println("Table Name : " +  result.getTableDescription().getTableName());

                    /* Creating and Sending request with Table Name only */
                    result = client.deleteTable(Name);

                    System.out.println("Status : " +  result.getSdkHttpMetadata().getHttpStatusCode());

                    System.out.println("Table Name : " +  result.getTableDescription().getTableName());

                } catch (AmazonServiceException e) {

                    System.out.println(e.getErrorMessage());

                }
            }

            static void getItems( String Name){
                GetItemRequest request = new GetItemRequest();

                /* Setting Table Name */
                request.setTableName(Name);

                /* Setting Consumed Capacity */
                request.setReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);

                /* Setting Name of attributes to Get */
                request.setProjectionExpression("ID, Titres");

                /* Setting Consistency Models */
                /* true for Strong Consistent & false for Eventually Consistent */
                request.setConsistentRead(true);

                /* Create a Map of Primary Key attributes */
                Map<String, AttributeValue> keysMap = new HashMap<>();
                keysMap.put("ID", new AttributeValue().withN(String.valueOf(1)));
                keysMap.put("Titres", (new AttributeValue("La vie est belle")));
                request.setKey(keysMap);

                try {
                    /* Send Get Item Request */
                    GetItemResult result = client.getItem(request);

                    System.out.println("Status : " +  result.getSdkHttpMetadata().getHttpStatusCode());


                    /* Printing Attributes Name and Values */
                    if (result.getItem() != null) {
                        result.getItem().entrySet().stream()
                                .forEach(e -> System.out.println(e.getKey() + " " + e.getValue().toString()));
                    }

                } catch (AmazonServiceException e) {

                    System.out.println("Cannot retrieve"+e.getErrorMessage());

                }
            }

            }










