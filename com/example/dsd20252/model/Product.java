package com.example.dsd20252.model;

import java.io.Serializable;

//Product object class
public class Product implements Serializable {
    private String productName;
    private String productType;
    private int availableAmount;
    private double price;
    private int soldAmount;
    private int initialAmount ;


    public String getProductName() { return productName; }
    public String getProductType() { return productType; }
    public int getAvailableAmount() { return availableAmount; }
    public double getPrice() { return price; }
    public int getSoldAmount() { return soldAmount; }
    public int getInitialAmount() { return initialAmount; }




    public void setProductName(String productName) { this.productName = productName; }
    public void setProductType(String productType) { this.productType = productType; }
    public void setAvailableAmount(int availableAmount) { this.availableAmount = availableAmount; }
    public void setPrice(double price) { this.price = price; }
    public void setSoldAmount(int soldAmount) { this.soldAmount = soldAmount; }
    public void setInitialAmount(int initialAmount ) {  this.initialAmount = initialAmount; }

}
