package com.springbatch.SpringBatchDemo.service;

import com.springbatch.SpringBatchDemo.model.Product;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;

//@Service
public class ProductService {

    public ArrayList<Product> getAllProducts() {
        RestTemplate restTemplate = new RestTemplate();
        String url = "http://localhost:8090/products";
        Product[] products = restTemplate.getForObject(url, Product[].class);
        ArrayList<Product> products1 = new ArrayList<>();
        for(Product p : products) {
            products1.add(p);
        }
        return products1;
    }

    public Product getProduct() {
        RestTemplate restTemplate = new RestTemplate();
        String url = "http://localhost:8090/product";
        Product product = restTemplate.getForObject(url, Product.class);

        return product;
    }
}
