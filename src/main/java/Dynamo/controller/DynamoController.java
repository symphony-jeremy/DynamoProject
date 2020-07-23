package Dynamo.controller;

import Dynamo.MoviesDao;
import Dynamo.model.Movies;
import com.amazonaws.services.dynamodbv2.document.*;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class DynamoController {
MoviesDao tab = new MoviesDao();




    @GetMapping("/getAlltables")
    public List<String> table(){

        return tab.getTables().getTableNames();
    }
    @PostMapping("/addItem/{Name}")
    @ResponseStatus(HttpStatus.CREATED)
    public void addItem(@PathVariable("Name") String name, @RequestBody Movies movie){


        tab.putItem(name,movie);
    }

    @GetMapping("/Tables/{name}")
    public List<Movies> getAllItems(@PathVariable("name") String name){ return tab.getAllItems(name);
    }

    @PostMapping("/Tables/addTable")
    @ResponseStatus(HttpStatus.CREATED)
    public Table createTable( @RequestBody String Name){
        return tab.createtable(Name);

    }
    @DeleteMapping("/delete/{name}")
    public String deleteTable (@PathVariable String name){
        tab.deleteTable(name);
        return  " table deleted";
    }

    @GetMapping("/Tables/{name}/{id}")
    public List<Movies> findMovieById(@PathVariable("name") String name , @PathVariable("id") String ID ){

        return  tab.filterTableWithID(name,ID);

    }
    @GetMapping("/filter/{name}/{filter}")
    public List<Movies> filterwithCategory(@PathVariable("name") String name , @PathVariable("filter") String filter ){

        return  tab.filterTableWithCategory(name,filter);

    }


     @PutMapping("/update/{name}")
    public void  updateMovie (@PathVariable("name") String name ,  @RequestBody Movies movie ){

       tab.updateItem(name , movie);


     }
    @DeleteMapping("/delete/{name}/{id}")
    public String deleteItem (@PathVariable String name,@PathVariable String id){
        tab.deleteItem(name , id );
        return  " item deleted";
    }

 }
