package Dynamo.dao;

import Dynamo.DynamoApplicationTests;
import Dynamo.model.Actor;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ActorDaoTest extends DynamoApplicationTests {

    @Autowired
    private ActorDao actorDao;

    private static final List<Actor> INITIAL_DATA = Arrays.asList(
            new Actor("mel", "gibson", 1956),
            new Actor("kirk", "douglas", 1916),
            new Actor("michael", "douglas", 1944)
    );

    @BeforeEach
    void setUp() {
        // create the table
        TableDescription actorTable = actorDao.createActorTable();
        assertNotNull(actorTable, "table creation should return a description");

        // insert some test data
        INITIAL_DATA.forEach(actor -> actorDao.createActor(actor));

    }

    @AfterEach
    void tearDown() {
        // delete table between each test (to guarantee test isolation)
        actorDao.deleteActorTable();
    }

    @Test
    public void queryActorTest(){
        List<Actor> actors = actorDao.getActor("douglas", "michael");
        assertEquals(1, actors.size(), "one actor should have been returned");

        Actor actor = actors.get(0);
        assertEquals("douglas", actor.getLastname());
        assertEquals("michael", actor.getFirstname());
        assertEquals(1944, actor.getYearOfBirth());
    }

    @Test
    public void createActorTest(){

        String firstname = "jennifer";
        String lastname = "lawrence";
        int year = 1990 ;
        // create a new actress
        Actor newActor = new Actor(firstname, lastname, year);
        this.actorDao.createActor(newActor);

        // check that we can query the new entry
        List<Actor> actors = actorDao.getActor(lastname, firstname);
        assertEquals(1, actors.size(), "one actor should have been returned");

        Actor actor = actors.get(0);
        assertEquals(lastname, actor.getLastname());
        assertEquals(firstname, actor.getFirstname());
        assertEquals(year, actor.getYearOfBirth());
    }



}