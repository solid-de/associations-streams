package edu.cnam.nfe101;

public class Association {
    private String city;
    private String postalCode;
    private String actDomain;

    public Association() {
    }

    public Association(String city, String postalCode, String actDomain) {
        this.city = city;
        this.postalCode = postalCode;
        this.actDomain = actDomain;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getPostalCode() {
        return postalCode;
    }

    public void setPostalCode(String postalCode) {
        this.postalCode = postalCode;
    }
    public String getActDomain() {
        return actDomain;
    }

    public void setActDomain(String actDomain) {
        this.actDomain = actDomain;
    }
}