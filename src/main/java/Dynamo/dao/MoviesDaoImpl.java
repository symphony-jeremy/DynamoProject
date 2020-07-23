package Dynamo.dao;

import Dynamo.model.Movies;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import org.springframework.stereotype.Repository;

import java.util.*;

@Repository
public class MoviesDaoImpl implements MoviesDao {


    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
            .build();

    static DynamoDB dynamoDB = new DynamoDB(client);

    ListTablesResult res = null;
    Table createdTable;
    List<Movies> movies = new ArrayList<Movies>();
    List<Movies> moviesFiltered = new ArrayList<Movies>();
    List<Movies> moviesFilteredById = new ArrayList<Movies>();


    @Override
    public void getTableInformation(String Name) {

        System.out.println("Describing " + Name);

        TableDescription tableDescription = dynamoDB.getTable(Name).describe();
        System.out.format(
                "Name: %s:\n" + "Status: %s \n" + "Provisioned Throughput (read capacity units/sec): %d \n"
                        + "Provisioned Throughput (write capacity units/sec): %d \n",
                tableDescription.getTableName(), tableDescription.getTableStatus(),
                tableDescription.getProvisionedThroughput().getReadCapacityUnits(),
                tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
        System.out.println("hey " + tableDescription.getItemCount());
    }
    // creation of tables

    @Override
    public Table createtable(String Name) {

        /* Create an Object of CreateTableRequest */
        CreateTableRequest request = new CreateTableRequest();

        /* Setting Table Name */
        request.setTableName(Name);

        /* Create & Set a list of AttributeDefinition */
        List<AttributeDefinition> attributeDefinitions = Arrays.asList(
                new AttributeDefinition("ID_Movie", ScalarAttributeType.S),
                new AttributeDefinition("Title", ScalarAttributeType.S));

        request.setAttributeDefinitions(attributeDefinitions);

        /* Create & Set a list of KeySchemaElement */
        List<KeySchemaElement> keySchema = Arrays.asList(
                new KeySchemaElement("ID_Movie", KeyType.HASH),
                new KeySchemaElement("Title", KeyType.RANGE));


        request.setKeySchema(keySchema);

        /* Setting Provisioned Throughput */
        request.setProvisionedThroughput(new ProvisionedThroughput(1L, 1L));

        try {
            /* Send Create Table Request */
            createdTable = dynamoDB.createTable(request);



            /* Creating and Sending request using Fluent API - USER Table */
            Table resultFluent = dynamoDB.createTable((new CreateTableRequest())
                    .withTableName(Name)
                    .withAttributeDefinitions(new AttributeDefinition("ID_Movie", ScalarAttributeType.S), new AttributeDefinition("Title", ScalarAttributeType.S))
                    .withKeySchema(new KeySchemaElement("ID_Movie", KeyType.HASH), new KeySchemaElement("Title", KeyType.RANGE))

                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)));
            System.out.println("hey" + resultFluent);


        } catch (AmazonServiceException e) {

            System.out.println(e.getErrorMessage());

        }
        return createdTable;
    }
    // get all tables

    @Override
    public ListTablesResult getTables() {
        try {

            /* Creating ListTableRequest with limit 50 */
            ListTablesRequest request = new ListTablesRequest();
            request.withLimit(50);

            String lastTable = null;

            while (true) {

                if (lastTable == null) {
                    /* Send First List Table Request */
                    res = client.listTables(request);
                } else {
                    /* Send Subsequent List Table Request */
                    res = client.listTables(request.withExclusiveStartTableName(lastTable));
                }

                res.getTableNames().forEach(e -> {
                            System.out.println(e);

                        }
                );

                /* Getting name of last evaluated table */
                lastTable = res.getLastEvaluatedTableName();
                if (lastTable == null) {
                    break;
                }

            }

        } catch (AmazonServiceException e) {

            System.out.println(e.getErrorMessage());

        }
        return res;

    }
    // delete the table from the database

    @Override
    public void deleteTable(String Name) {
        DeleteTableRequest request = new DeleteTableRequest();

        /* Setting Table Name */
        request.setTableName(Name);

        try {
            /* Send Delete Table Request */
            DeleteTableResult result = client.deleteTable(request);

            System.out.println("Status : " + result.getSdkHttpMetadata().getHttpStatusCode());

            System.out.println("Table Name : " + result.getTableDescription().getTableName());

            /* Creating and Sending request with Table Name only */
            result = client.deleteTable(Name);

            System.out.println("Status : " + result.getSdkHttpMetadata().getHttpStatusCode());

            System.out.println("Table Name : " + result.getTableDescription().getTableName());

        } catch (AmazonServiceException e) {

            System.out.println(e.getErrorMessage());

        }
    }
    // retrieve items from the table

    @Override
    public List<Movies> filterTableWithCategory(String Name, String filter) {
        Table table = dynamoDB.getTable(Name);
        Map<String, AttributeValue> expressionAttributeValues =
                new HashMap<String, AttributeValue>();
        expressionAttributeValues.put(":category", new AttributeValue(filter));

        ScanRequest items = new ScanRequest().withTableName(Name)
                .withFilterExpression("Category = :category")
                .withProjectionExpression("ID_Movie, Title , Category ,  origin")
                .withExpressionAttributeValues(expressionAttributeValues);


        ScanResult result = client.scan(items);
        for (Map<String, AttributeValue> item : result.getItems()) {
            Movies movie = new Movies();
            movie.setId_Movie(item.get("ID_Movie").getS());
            movie.setTitle(item.get("Title").getS());
            movie.setCategory(item.get("Category").getS());
            movie.setOrigin(item.get("origin").getS());


            moviesFiltered.add(movie);
            System.out.println(item);
        }
        return moviesFiltered;

    }


    @Override
    public List<Movies> getAllItems(String Name) {
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(Name);
        ScanResult result = client.scan(scanRequest);


        for (Map<String, AttributeValue> item : result.getItems()) {
            Movies movie = new Movies(item.get("ID_Movie").getS(), item.get("Title").getS(), item.get("Category").getS(), item.get("year").getS(), item.get("origin").getS());
            movies.add(movie);
            System.out.println(item.get("ID_Movie").getS());
        }
        return movies;
    }


    @Override
    public List<Movies> filterTableWithID(String Name, String filter) {

        Map<String, AttributeValue> expressionAttributeValue =
                new HashMap<String, AttributeValue>();
        expressionAttributeValue.put(":id", new AttributeValue(filter));

        ScanRequest items = new ScanRequest().withTableName(Name)
                .withFilterExpression("ID_Movie = :id")
                .withProjectionExpression("ID_Movie, Title , Category ,  origin")
                .withExpressionAttributeValues(expressionAttributeValue);


        ScanResult resul = client.scan(items);
        for (Map<String, AttributeValue> item : resul.getItems()) {
            Movies movie = new Movies();
            movie.setId_Movie(item.get("ID_Movie").getS());
            movie.setTitle(item.get("Title").getS());
            movie.setCategory(item.get("Category").getS());
            movie.setOrigin(item.get("origin").getS());


            moviesFilteredById.add(movie);
            System.out.println(item);
        }
        return moviesFilteredById;

    }

    @Override
    public void putItem(String Name, Movies movie) {
        Table table = dynamoDB.getTable(Name);
        try {

            Item item = new Item().withPrimaryKey("ID_Movie", movie.getId_Movie()).withString("Title", movie.getTitle())
                    .withString("Category", movie.getCategory())
                    .withString("year", movie.getYear()).withString("origin", movie.getOrigin());

            PutItemOutcome outcome = table.putItem(item);


        } catch (Exception e) {
            System.err.println("Create items failed.");
            System.err.println(e.getMessage());

        }


    }

    @Override
    public void updateItem(String name, Movies movie) {

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
        keysMap.put("ID_Movie", new AttributeValue(movie.getId_Movie()));
        keysMap.put("Title", (new AttributeValue(movie.getTitle())));
        request.setKey(keysMap);

        /* Create a Map of attributes to be updated */
        Map<String, AttributeValueUpdate> map = new HashMap<>();
        map.put("Category", new AttributeValueUpdate(new AttributeValue(movie.getCategory()), "PUT"));
        map.put("origin", new AttributeValueUpdate(new AttributeValue(movie.getOrigin()), "PUT"));
        map.put("year", new AttributeValueUpdate(new AttributeValue(movie.getYear()), "PUT"));

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

    @Override
    public void deleteItem(String Name, String ID) {
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
        keysMap.put("ID_Movie", (new AttributeValue(ID)));

        request.setKey(keysMap);

        try {
            /* Send Delete Item Request */
            DeleteItemResult result = client.deleteItem(request);


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
}




