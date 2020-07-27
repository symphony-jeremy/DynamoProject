package Dynamo;


import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import Dynamo.controller.DynamoController;
import Dynamo.dao.MoviesDao;
import Dynamo.model.Movies;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.jupiter.api.Test;

import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import javax.servlet.http.Cookie;


@SpringBootTest
@AutoConfigureMockMvc
public class DynamoControllerTest {

    @Autowired
    private MockMvc mockMvc;

    DynamoController controller;



    String BASE_URL="http://localhost:8080/";

    @Test
    public void shouldReturnAList() throws Exception {
        mockMvc.perform(get("/getAlltables"))
                .andExpect(content().string("[\"Movies\"]"))
                .andExpect(status().is2xxSuccessful());

    }
    @Test
    public void shouldReturnAlistOfMovies() throws Exception {
        mockMvc.perform(get("/Tables/Movies"))
                .andExpect(status().isOk())
                .andExpect(status().is2xxSuccessful());

    }

    @Test
    public void shouldCreateAMovie() throws Exception {
        String url = BASE_URL + "Tables/Movies";
        Movies anObject = new Movies();
        anObject.setId_Movie("3");
        anObject.setOrigin("Fr");
        anObject.setCategory("Action");
        anObject.setTitle("Booth");
        anObject.setYear("1997");
        //... more
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.WRAP_ROOT_VALUE, false);
        ObjectWriter ow = mapper.writer().withDefaultPrettyPrinter();
        String requestJson=ow.writeValueAsString(anObject );

        mockMvc.perform(post("/addItem/Movies").contentType(MediaType.APPLICATION_JSON).content("{\"id_Movie\":\"3\",\"year\":\"2019\",\"origin\":\"France\",\"title\":\"Fort\",\"category\":\"Action\"}"))
                .andExpect(status().isCreated())
                .andExpect(status().is2xxSuccessful());



    }
    @Test
    public void shouldupdateMovie() throws Exception{
        mockMvc.perform(

                put("/update/Movies")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"id_Movie\":\"3\",\"year\":\"2019\",\"origin\":\"France\",\"title\":\"Fort\",\"category\":\"Drame\"}")
                        .cookie(new Cookie("AUTH_TOKEN", "TOKEN_SUPER_USER_VALUE")))
                .andExpect(status().isOk())
                .andDo(print());

    }

    @Test
    public void shouldReturnAlistOfMovieswithCategory() throws Exception {
        mockMvc.perform(get("/filter/Movies/Drame"))
                .andExpect(content().string("[{\"id_Movie\":\"3\",\"year\":null,\"origin\":\"France\",\"category\":\"Drame\",\"title\":\"Fort\"}]"))
                .andExpect(status().is2xxSuccessful());

    }

}
