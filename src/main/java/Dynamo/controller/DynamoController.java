package Dynamo.controller;

import Dynamo.dao.MoviesDao;
import Dynamo.model.Movies;
import com.amazonaws.services.dynamodbv2.document.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class DynamoController {

    @Autowired
    private MoviesDao tab;


    @GetMapping("/getAlltables")
    public List<String> getTables() {

        return tab.getTables().getTableNames();
    }

    @PostMapping("/addItem")
    @ResponseStatus(HttpStatus.CREATED)
    public void addMovie( @RequestBody Movies movie) {


        tab.putMovie( movie);
    }

    @GetMapping("/Movies")
    public List<Movies> findAll() {
        return tab.findAll();
    }

    @PostMapping("/Tables/addTable")
    @ResponseStatus(HttpStatus.CREATED)
    public Table createTable(@RequestBody String Name) {
        return tab.createTable(Name);

    }

    @DeleteMapping("/delete")
    public String deleteTable() {
        tab.deleteTable();
        return " table deleted";
    }

    @GetMapping("/Movies/{id}")
    public List<Movies> findMovieById( @PathVariable("id") String ID) {

        return tab.findMovieById( ID);

    }

    @GetMapping("/filter/Movies/{filter}")
    public List<Movies> findMovieByCategory( @PathVariable("filter") String filter) {

        return tab.findMovieByCategory( filter);

    }


    @PutMapping("/update/Movies")
    public void updateMovie( @RequestBody Movies movie) {

        tab.updateMovie( movie);


    }

    @DeleteMapping("/delete/Movies/{id}")
    public String deleteMovie( @PathVariable String id) {
        tab.deleteMovie( id);
        return " item deleted";
    }

}
