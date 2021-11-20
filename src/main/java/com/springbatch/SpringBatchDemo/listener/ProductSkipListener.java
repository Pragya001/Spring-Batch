package com.springbatch.SpringBatchDemo.listener;

import org.springframework.batch.core.annotation.OnSkipInProcess;
import org.springframework.batch.core.annotation.OnSkipInRead;
import org.springframework.batch.core.annotation.OnSkipInWrite;
import org.springframework.batch.item.file.FlatFileParseException;

import java.io.FileOutputStream;
import java.util.Date;

public class ProductSkipListener {

    private String readErrFileName = "error/read_skipped";
    private String processErrFileName = "error/process_skipped";
    private String writeErrFileName = "error/write_skipped";

    @OnSkipInRead
    public void onSkipRead(Throwable t) {
        if(t instanceof  FlatFileParseException) {
            FlatFileParseException ffex = (FlatFileParseException) t;
            onSkip(ffex.getInput(), readErrFileName);
        }
    }

    @OnSkipInProcess
    public void onSkipProcess(Object o, Throwable t) {
        if(t instanceof  RuntimeException) {
            onSkip(o, processErrFileName);
        }
    }

    @OnSkipInWrite
    public void onSkipWrite(Object o, Throwable t) {
        if(t instanceof RuntimeException) {
            onSkip(o, writeErrFileName);
        }
    }

    private void onSkip(Object o, String name) {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(name, true);
            fos.write((new Date()).toString().getBytes());
            fos.write(o.toString().getBytes());
            fos.write("\r\n".getBytes());
            fos.close();;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
