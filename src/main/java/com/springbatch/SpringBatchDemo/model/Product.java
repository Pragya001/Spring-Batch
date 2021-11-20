package com.springbatch.SpringBatchDemo.model;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name="product")
//@Document(collection = "products")
public class Product {
    private String productName;
    private String productDesc;

    @XStreamAlias("No-of-pieces")
    private Integer unit;
    private Integer price;

    @Override
    public String toString() {
        return "Product{" +
                "productName='" + productName + '\'' +
                ", productDesc='" + productDesc + '\'' +
                ", unit=" + unit +
                ", price=" + price +
                '}';
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductDesc() {
        return productDesc;
    }

    public void setProductDesc(String productDesc) {
        this.productDesc = productDesc;
    }

    public Integer getUnit() {
        return unit;
    }

    public void setUnit(Integer unit) {
        this.unit = unit;
    }

    public Integer getPrice() {
        return price;
    }

    public void setPrice(Integer price) {
        this.price = price;
    }
}
