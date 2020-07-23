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

    @PostMapping("/addItem/{Name}")
    @ResponseStatus(HttpStatus.CREATED)
    public void addMovie(@PathVariable("Name") String name, @RequestBody Movies movie) {


        tab.putMovie(name, movie);
    }

    @GetMapping("/Tables/{name}")
    public List<Movies> findAll(@PathVariable("name") String name) {
        return tab.findAll(name);
    }

    @PostMapping("/Tables/addTable")
    @ResponseStatus(HttpStatus.CREATED)
    public Table createTable(@RequestBody String Name) {
        return tab.createTable(Name);

    }

    @DeleteMapping("/delete/{name}")
    public String deleteTable(@PathVariable String name) {
        tab.deleteTable(name);
        return " table deleted";
    }

    @GetMapping("/Tables/{name}/{id}")
    public List<Movies> findMovieById(@PathVariable("name") String name, @PathVariable("id") String ID) {

        return tab.findMovieById(name, ID);

    }

    @GetMapping("/filter/{name}/{filter}")
    public List<Movies> findMovieByCategory(@PathVariable("name") String name, @PathVariable("filter") String filter) {

        return tab.findMovieByCategory(name, filter);

    }


    @PutMapping("/update/{name}")
    public void updateMovie(@PathVariable("name") String name, @RequestBody Movies movie) {

        tab.updateMovie(name, movie);


    }

    @DeleteMapping("/delete/{name}/{id}")
    public String deleteMovie(@PathVariable String name, @PathVariable String id) {
        tab.deleteMovie(name, id);
        return " item deleted";
    }

}
