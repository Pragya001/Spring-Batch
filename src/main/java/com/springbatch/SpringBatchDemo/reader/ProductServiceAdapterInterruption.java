package com.springbatch.SpringBatchDemo.reader;

import com.springbatch.SpringBatchDemo.model.Product;
import com.springbatch.SpringBatchDemo.service.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

//@Component
public class ProductServiceAdapterInterruption {

    Logger log = LoggerFactory.getLogger(ProductServiceAdapterInterruption.class);

    @Autowired
    ProductService productService;

    public  Product nextProduct() throws InterruptedException {
        Product p = null;
        Thread.sleep(1000);
        try {
            p = productService.getProduct();
            log.info("Connected to webservice");
        } catch (Exception e) {
            log.error("Exception...." + e.getMessage());
            throw e;
        }
        return p;
    }
}
