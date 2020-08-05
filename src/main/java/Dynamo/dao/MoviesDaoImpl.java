package Dynamo.dao;

import Dynamo.model.Movies;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
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
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("test", "test")))
            .build();

    static DynamoDB dynamoDB = new DynamoDB(client);

    ListTablesResult res = null;
    Table createdTable;
    String nameTable = "Movies";



    @Override
    public void getTableInformation() {


        TableDescription tableDescription = dynamoDB.getTable(nameTable).describe();
        System.out.format(
                "Name: %s:\n" + "Status: %s \n" + "Provisioned Throughput (read capacity units/sec): %d \n"
                        + "Provisioned Throughput (write capacity units/sec): %d \n",
                tableDescription.getTableName(), tableDescription.getTableStatus(),
                tableDescription.getProvisionedThroughput().getReadCapacityUnits(),
                tableDescription.getProvisionedThroughput().getWriteCapacityUnits());
        System.out.println("hey " + tableDescription.getItemCount());
    }

    @Override
    public Table createTable(   String Name) {

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
            // jeremy : commented because table is already created above
            /*
            Table resultFluent = dynamoDB.createTable((new CreateTableRequest())
                    .withTableName(Name)
                    .withAttributeDefinitions(new AttributeDefinition("ID_Movie", ScalarAttributeType.S), new AttributeDefinition("Title", ScalarAttributeType.S))
                    .withKeySchema(new KeySchemaElement("ID_Movie", KeyType.HASH), new KeySchemaElement("Title", KeyType.RANGE))

                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L)));
            System.out.println("hey" + resultFluent);
            */



        } catch (AmazonServiceException e) {

            System.out.println(e.getErrorMessage());

        }
        return createdTable;
    }

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

    @Override
    public void deleteTable() {
        DeleteTableRequest request = new DeleteTableRequest();

        /* Setting Table Name */
        request.setTableName(nameTable);

        try {
            /* Send Delete Table Request */
            DeleteTableResult result = client.deleteTable(request);

            System.out.println("Status : " + result.getSdkHttpMetadata().getHttpStatusCode());

            System.out.println("Table Name : " + result.getTableDescription().getTableName());


        } catch (AmazonServiceException e) {

            System.out.println(e.getErrorMessage());

        }
    }

    @Override
    public List<Movies> findMovieByCategory( String filter) {
        Table table = dynamoDB.getTable(nameTable);
        Map<String, AttributeValue> expressionAttributeValues =
                new HashMap<String, AttributeValue>();
        expressionAttributeValues.put(":category", new AttributeValue(filter));

        ScanRequest items = new ScanRequest().withTableName(nameTable)
                .withFilterExpression("Category = :category")
                .withProjectionExpression("ID_Movie, Title , Category ,  origin")
                .withExpressionAttributeValues(expressionAttributeValues);


        ScanResult result = client.scan(items);
        List<Movies> moviesFiltered = new ArrayList<>();
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
    public List<Movies> findAll() {
        ScanRequest scanRequest = new ScanRequest()
                .withTableName(nameTable);
        ScanResult result = client.scan(scanRequest);


        List<Movies> movies = new ArrayList<>();
        for (Map<String, AttributeValue> item : result.getItems()) {
            Movies movie = new Movies(item.get("ID_Movie").getS(), item.get("Title").getS(), item.get("Category").getS(), item.get("year").getS(), item.get("origin").getS());
            movies.add(movie);
            System.out.println(item.get("ID_Movie").getS());
        }
        return movies;
    }


    @Override
    public Movies findMovieById( String filter) {

        Map<String, AttributeValue> expressionAttributeValue =
                new HashMap<String, AttributeValue>();
        expressionAttributeValue.put(":id", new AttributeValue(filter));

        ScanRequest items = new ScanRequest().withTableName(nameTable)
                .withFilterExpression("ID_Movie = :id")
                .withProjectionExpression("ID_Movie, Title , Category ,  origin")
                .withExpressionAttributeValues(expressionAttributeValue);


        ScanResult resul = client.scan(items);
        Movies movie = new Movies();

        for (Map<String, AttributeValue> item : resul.getItems()) {
            movie.setId_Movie(item.get("ID_Movie").getS());
            movie.setTitle(item.get("Title").getS());
            movie.setCategory(item.get("Category").getS());
            movie.setOrigin(item.get("origin").getS());


            System.out.println(item);
        }
        return movie;

    }

    @Override
    public void putMovie( Movies movie) {
        Table table = dynamoDB.getTable(nameTable);
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
    public void updateMovie( Movies movie) {

        /* Create an Object of UpdateItemRequest */
        UpdateItemRequest request = new UpdateItemRequest();

        /* Setting Table Name */
        request.setTableName(nameTable);

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
    public void deleteMovie( String id) {

        HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("ID_Movie", new AttributeValue().withN(id));

        DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
                .withTableName(nameTable)
                .withKey(key)
                .withConditionExpression("ID_Movie = :val")
                //.withExpressionAttributeValues(expressionAttributeValues)
                .withReturnValues(ReturnValue.ALL_OLD);

//<<<<<<< HEAD
//=======
        /* Create a Map of Primary Key attributes */
        Map<String, AttributeValue> keysMap = new HashMap<>();
        keysMap.put("ID_Movie", (new AttributeValue(id)));
        keysMap.put("Title",(new AttributeValue(findMovieById(id).getTitle())));

//>>>>>>> master


        try {
            /* Send Delete Item Request */
            DeleteItemResult result = client.deleteItem(deleteItemRequest);


            System.out.println("Hello"+id);


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




