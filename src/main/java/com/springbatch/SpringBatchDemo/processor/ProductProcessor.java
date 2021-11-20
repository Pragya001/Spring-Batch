package com.springbatch.SpringBatchDemo.processor;

import com.springbatch.SpringBatchDemo.model.Product;
import org.springframework.batch.item.ItemProcessor;

public class ProductProcessor implements ItemProcessor<Product, Product> {
    @Override
    public Product process(Product item) throws Exception {
//        if(item.getProductName().equals("Apple"))
//            throw new RuntimeException("Apple is not configured");
//        else
        Thread.sleep(300);
            item.setProductDesc(item.getProductDesc().toUpperCase());

        return item;
    }
}
