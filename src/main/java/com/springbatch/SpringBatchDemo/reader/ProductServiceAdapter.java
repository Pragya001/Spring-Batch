package com.springbatch.SpringBatchDemo.reader;

import com.springbatch.SpringBatchDemo.model.Product;
import com.springbatch.SpringBatchDemo.service.ProductService;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.ArrayList;

//@Component
public class ProductServiceAdapter implements InitializingBean {
    @Autowired
    private ProductService productService;
    private ArrayList<Product> products;

    @Override
    public void afterPropertiesSet() throws Exception {
        this.products = productService.getAllProducts();
    }

    public Product nextProduct() {
        if(products.size() > 0) {
            return products.remove(0);
        } else {
            return null;
        }
    }
}
