package Dynamo.model;

public class Movies {

    private  String id_Movie;
    private  String Title;
    private String Category;
    private String year;
    private String origin;

    public Movies(String id_Movie, String Title , String Category ,String year , String origin) {
        this.id_Movie= id_Movie;
        this.Title = Title;
        this.Category=Category;
        this.year=year;
        this.origin=origin;}
        public Movies(){}

    public String getId_Movie() {
        return id_Movie;
    }

    public String getTitle() {
        return Title;
    }

    public String getCategory() {
        return Category;
    }
    public String getYear(){
        return year;
    }
    public String getOrigin(){
        return origin;
    }

    public void setId_Movie(String ID_Movie){
        this.id_Movie=ID_Movie;
    }
    public void setOrigin(String origin){
        this.origin=origin;
    }
    public void setYear(String year){
        this.year=year;
    }
    public void setCategory(String Category){
        this.Category=Category;
    }
    public void setTitle(String Title){
        this.Title=Title;
    }

}
