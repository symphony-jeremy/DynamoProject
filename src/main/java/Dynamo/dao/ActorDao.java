package Dynamo.dao;

import Dynamo.model.Actor;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public class ActorDao {

    private static final Logger log = LoggerFactory.getLogger(ActorDao.class);

    private final DynamoDB dynamoDB;

    private static final String TABLE_NAME = "actors";
    private static final String ATTRIBUTE_FIRSTNAME = "firstname";
    private static final String ATTRIBUTE_LASTNAME = "lastname";
    private static final String ATTRIBUTE_YEAR_OF_BIRTH = "yearOfBirth";

    @Autowired
    public ActorDao(DynamoDB dynamoDB) {
        this.dynamoDB = dynamoDB;
    }

    public TableDescription createActorTable() {
        try {
            TableDescription describe = this.dynamoDB.getTable(TABLE_NAME).describe();
            log.info("table {} already exists, no need to create it", TABLE_NAME);
            return describe;
        } catch (ResourceNotFoundException ex) {
            log.info("Let's create table {}", TABLE_NAME);
            Table table = dynamoDB.createTable(getCreateTableRequest());
            try {
                return table.waitForActive();
            } catch (InterruptedException e) {
                throw new RuntimeException("Error creating table " + TABLE_NAME, e);
            }
        }
    }

    public void deleteActorTable(){
        Table table = this.dynamoDB.getTable(TABLE_NAME);
        table.delete();
        try {
            table.waitForDelete();
        } catch (InterruptedException e) {
            throw new RuntimeException("Error deleting table " + TABLE_NAME, e);
        }
    }

    public void createActor(Actor actor) {
        Item item = new Item()
                .withPrimaryKey(
                        ATTRIBUTE_LASTNAME, actor.getLastname(),
                        ATTRIBUTE_FIRSTNAME, actor.getFirstname()
                )
                .with(ATTRIBUTE_YEAR_OF_BIRTH, actor.getYearOfBirth());

        this.dynamoDB.getTable(TABLE_NAME).putItem(item);
    }

    public List<Actor> getActor(String lastname, String firstname) {
        QuerySpec spec = new QuerySpec()
                .withKeyConditionExpression(ATTRIBUTE_LASTNAME + " = :v_lastname and " + ATTRIBUTE_FIRSTNAME + " = :v_firstname")
                .withValueMap(new ValueMap()
                        .withString(":v_lastname", lastname.trim().toLowerCase())
                        .withString(":v_firstname", firstname.trim().toLowerCase())
                );

        ItemCollection<QueryOutcome> outcome = this.dynamoDB.getTable(TABLE_NAME).query(spec);
        IteratorSupport<Item, QueryOutcome> iter = outcome.iterator();

        List<Actor> result = new ArrayList<>();

        while (iter.hasNext()) {
            Item item = iter.next();
            result.add(
                    new Actor(
                            item.getString(ATTRIBUTE_FIRSTNAME),
                            item.getString(ATTRIBUTE_LASTNAME),
                            item.getInt(ATTRIBUTE_YEAR_OF_BIRTH)
                    )
            );
        }

        return result;
    }

    private CreateTableRequest getCreateTableRequest() {
        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName(ATTRIBUTE_LASTNAME).withAttributeType("S"));
        attributeDefinitions.add(new AttributeDefinition().withAttributeName(ATTRIBUTE_FIRSTNAME).withAttributeType("S"));


        List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(new KeySchemaElement().withAttributeName(ATTRIBUTE_LASTNAME).withKeyType(KeyType.HASH));
        keySchema.add(new KeySchemaElement().withAttributeName(ATTRIBUTE_FIRSTNAME).withKeyType(KeyType.RANGE));

        CreateTableRequest request = new CreateTableRequest()
                .withTableName(TABLE_NAME)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(5L)
                        .withWriteCapacityUnits(6L));

        return request;
    }
}
