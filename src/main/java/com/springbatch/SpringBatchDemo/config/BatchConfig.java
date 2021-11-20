package com.springbatch.SpringBatchDemo.config;

import com.springbatch.SpringBatchDemo.listener.HWJobListener;
import com.springbatch.SpringBatchDemo.listener.HWStepListener;
import com.springbatch.SpringBatchDemo.listener.ProductSkipListener;
import com.springbatch.SpringBatchDemo.model.Product;
import com.springbatch.SpringBatchDemo.processor.InMemProcessor;
import com.springbatch.SpringBatchDemo.processor.ProductProcessor;
import com.springbatch.SpringBatchDemo.reader.ProductServiceAdapter;
import com.springbatch.SpringBatchDemo.reader.ProductServiceAdapterInterruption;
import com.springbatch.SpringBatchDemo.tasklet.*;
import com.springbatch.SpringBatchDemo.writer.ConsoleItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.batch.item.data.MongoItemWriter;
import org.springframework.batch.item.file.*;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.*;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.client.ResourceAccessException;

import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

@EnableBatchProcessing
@Configuration
public class BatchConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private HWJobListener hwJobListener;
    @Autowired
    private HWStepListener hwStepListener;
    @Autowired
    private InMemProcessor inMemProcessor;
    @Autowired
    private MongoTemplate mongoTemplate;
//    @Autowired
//    private ProductServiceAdapter productService;
//    @Autowired
//    private ProductServiceAdapterInterruption productServiceAdapterInt;

    @Bean
    public Step step1() { //Tasklet just needs a task to be given to it as a single function. Step1 is done using Tasklet.
        return stepBuilderFactory.get("step1")
                .tasklet(helloWorldTasklet())
//                .listener(hwStepListener)
                .build();
    }

    private Tasklet helloWorldTasklet() {
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                System.out.println("Hello World!");
                return RepeatStatus.FINISHED;
            }
        };
    }

    //Flat File Item Reader
    @StepScope //This is to read Job parameters before the job has actually started.
    @Bean
    public FlatFileItemReader flatFileItemReader(
            @Value("#{jobParameters['fileInput']}") //read the command parameters provided while running the application
            FileSystemResource fileInput) {
        FlatFileItemReader<Product> reader = new FlatFileItemReader<>();

        //Step 1- Tell reader where to read file from
        reader.setResource(fileInput);

        //Step 2- Set the line mapper and delimiter to be used while reading the file
        reader.setLineMapper(
                new DefaultLineMapper<Product>() {
                    {
                        setLineTokenizer( new DelimitedLineTokenizer() {
                            {
                                setNames(new String[]{"productName", "productDesc", "unit", "price"});
                            }
                        });

                        setFieldSetMapper(new BeanWrapperFieldSetMapper<Product>() {
                            {
                                setTargetType(Product.class);
                            }
                        });
                    }
                }
        );

        //Step 3- Tell reader to skip the header line from the file
        reader.setLinesToSkip(1);

        return reader;

    }

    //XML File Item Reader
    @StepScope
    @Bean
    public StaxEventItemReader xmlItemReader(
            @Value("#{jobParameters['xmlInputFile']}")
            FileSystemResource fileInput
    ) {
        StaxEventItemReader<Product> reader = new StaxEventItemReader<>();
        // where to read the input file
        reader.setResource(fileInput);

        // need to let reader know which tags
        reader.setFragmentRootElementName("product");

        // tell reader how to parse XML and which main object to be mapped
        reader.setUnmarshaller(new Jaxb2Marshaller(){
            {
                setClassesToBeBound(Product.class);
            }
        });


        return reader;
    }

    //Fix Flat File Item Reader
    @StepScope //This is to read Job parameters before the job has actually started.
    @Bean
    public FlatFileItemReader flatFixFileItemReader(
            @Value("#{jobParameters['fileInput']}")
                    FileSystemResource fileInput) {
        FlatFileItemReader<Product> reader = new FlatFileItemReader<>();

        //Step 1- Tell reader where to read file from
        reader.setResource(fileInput);

        //Step 2- Set the line mapper to be used while reading the file
        reader.setLineMapper(
                new DefaultLineMapper<Product>() {
                    {
                        setLineTokenizer( new FixedLengthTokenizer() {
                            {
                                setNames(new String[]{"productName", "productDesc", "unit", "price"});
                                setColumns(
                                        new Range(1,13),
                                        new Range(14,27),
                                        new Range(28,35),
                                        new Range(36,44)
                                );
                            }
                        });

                        setFieldSetMapper(new BeanWrapperFieldSetMapper<Product>() {
                            {
                                setTargetType(Product.class);
                            }
                        });
                    }
                }
        );

        //Step 3- Tell reader to skip the header line from the file
        reader.setLinesToSkip(1);

        return reader;

    }

    //Reading from Database
//    @Bean
//    public MongoItemReader mongoItemReader() {
//        MongoItemReader<Product> reader = new MongoItemReader<>();
//        reader.setTemplate(mongoTemplate);
//        reader.setSort(new HashMap<String, Sort.Direction>() {
//            {
//                put("_id", Sort.Direction.DESC);
//            }
//        });
//        reader.setQuery("{}");
//        reader.setTargetType(Product.class);
//
//        return reader;
//    }

    //Reading a JSON object from a file
    @StepScope
    @Bean
    public JsonItemReader jsonItemReader(
            @Value("#{jobParameters['fileInput']}")
                    FileSystemResource fileInput
    ) {
        JsonItemReader reader = new JsonItemReader();
        reader.setResource(fileInput);
        reader.setJsonObjectReader(new JacksonJsonObjectReader(Product.class));

        return reader;
    }

    //Reading from external webservice : Simple reader example
//    @Bean
//    public ItemReaderAdapter serviceReaderAdapter() {
//        ItemReaderAdapter reader = new ItemReaderAdapter();
//        reader.setTargetObject(productService);
//        reader.setTargetMethod("nextProduct");
//
//        return reader;
//    }

/*  Reading from external webservice : Interruption retry example
    @Bean
    public ItemReaderAdapter serviceReaderAdapter() {
        ItemReaderAdapter reader = new ItemReaderAdapter();
        reader.setTargetObject(productServiceAdapterInt);
        reader.setTargetMethod("nextProduct");

        return reader;
    }

 */

//    @Bean
//    public Step step2() { //CHunk based step needs 3 steps -> Reader, processor, writer.
//        return stepBuilderFactory.get("step2")
//                .<Integer, Integer>chunk(3)
////                .reader(flatFixFileItemReader(null)) //Let the reader use the default resource given as @Value to it.
////                .reader(mongoItemReader())
////                .reader(jsonItemReader(null))
//                .reader(serviceReaderAdapter())
//                .writer(new ConsoleItemWriter())
//                .build();
//    }

    @Bean
    public Step writerSteps() { //Chunk based step needs 3 steps -> Reader, processor, writer.
        return stepBuilderFactory.get("step2")
                .<Product, Product>chunk(3)
                .reader(flatFileItemReader(null))
//                .reader(serviceReaderAdapter())
                .processor(new ProductProcessor())
//                .writer(flatFileItemWriter(null))
                .writer(mongoItemWriter())
                .faultTolerant()
//                .retry(ResourceAccessException.class) //Retry has to be used with skip limit and not with skip(always) policy.
//                .retryLimit(3)
                .skip(FlatFileParseException.class)
//                .skipLimit(10)
//                .skip(FlatFileParseException.class) // we don't need this, since we already have a listener which will trace this exception.
                .skipLimit(3)
//                .skip(RuntimeException.class)
//                .skipPolicy(new AlwaysSkipItemSkipPolicy())
//                .listener(new ProductSkipListener())
                .build();
    }


    @Bean
    public Step multiThreadedStep() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(5);
        taskExecutor.setMaxPoolSize(5);
        taskExecutor.afterPropertiesSet();

        return stepBuilderFactory.get("multiThreadedStep")
                .<Product, Product>chunk(3)
                .reader(flatFileItemReader(null))
                .processor(new ProductProcessor())
                .writer(mongoItemWriter())
                .taskExecutor(taskExecutor)
                .build();
    }

    @Bean
    public AsyncItemProcessor asyncItemProcessor() {
        AsyncItemProcessor processor = new AsyncItemProcessor();
        processor.setDelegate(new ProductProcessor());
        processor.setTaskExecutor(new SimpleAsyncTaskExecutor());

        return processor;
    }

    @Bean
    public AsyncItemWriter asyncItemWriter() {
        AsyncItemWriter writer = new AsyncItemWriter();
        writer.setDelegate(flatFileItemWriter(null));

        return writer;
    }

    @Bean
    public Step asyncStep() {
        return stepBuilderFactory.get("asyncStep")
                .<Product, Product>chunk(3)
                .reader(flatFileItemReader(null))
                .processor(asyncItemProcessor())
                .writer(asyncItemWriter())
                .build();
    }


    @StepScope
    @Bean
    public FlatFileItemWriter flatFileItemWriter(
            @Value("#{jobParameters['fileOutput']}")
            FileSystemResource outputFile
    ) {
        FlatFileItemWriter writer = new FlatFileItemWriter<Product>();
        /*{
            @Override
            public String doWrite(List<? extends Product> items) {
                for (Product p:
                     items) {
                    if(p.getUnit() == 17)
                        throw new RuntimeException("Unit 17 is not acceptable");
                }
                return super.doWrite(items);
            }
        };
         */

        writer.setResource(outputFile);
        writer.setLineAggregator(new DelimitedLineAggregator() {
            {
                setDelimiter("|");
                setFieldExtractor(new BeanWrapperFieldExtractor() {
                    {
                        setNames(new String[]{"productName", "unit", "price", "productDesc"});
                    }
                });
            }
        });

        writer.setHeaderCallback(new FlatFileHeaderCallback() {
            @Override
            public void writeHeader(Writer writer) throws IOException {
                writer.write("productName|unit|price|productDesc");
            }
        });

        writer.setAppendAllowed(true); //default is false. If it was true, the output of file will be appended to the already existing file.

//        writer.setFooterCallback(new FlatFileFooterCallback() {
//            @Override
//            public void writeFooter(Writer writer) throws IOException {
//                writer.write("This file was created at: " + new SimpleDateFormat().format(new Date()));
//            }
//        });
        return writer;
    }


    @StepScope
    @Bean
    public StaxEventItemWriter xmlItemWriter(
            @Value("#{jobParameters['fileOutput']}")
            FileSystemResource outputFile) {
        XStreamMarshaller marshaller = new XStreamMarshaller();
        HashMap<String, Class> aliases = new HashMap<>(); //This is created to make the root element take name not as a whole class path but just name
        aliases.put("Product", Product.class);
        marshaller.setAliases(aliases);
        marshaller.setAutodetectAnnotations(true); //To detect the annotation that were kept on the fields of Product model file, to detect the names of fields to be used in xml tags.
        StaxEventItemWriter writer = new StaxEventItemWriter();

        writer.setResource(outputFile);
        writer.setMarshaller(marshaller);
        writer.setRootTagName("Products");

        return writer;
    }

    @Bean
    public MongoItemWriter mongoItemWriter() {
        MongoItemWriter writer = new MongoItemWriter();
        writer.setTemplate(mongoTemplate);
        writer.setCollection("products");

        return writer;
    }


    // ------------------ Parallel jobs ----------------------
    // Download File : Task 1
    // Process Downloaded file : Task 2
    // Business task 3 : Task 3
    // Business task 4 : Task 4
    // Clean up all the data : Task 5

    public Step downloadStep() {
        return stepBuilderFactory.get("downloadStep")
                .tasklet(new DownloadTasklet())
                .build();
    }

    public Step processFileStep() {
        return stepBuilderFactory.get("processFileStep")
                .tasklet(new ProcessFileTasklet())
                .build();
    }

    public Step biz3Step() {
        return stepBuilderFactory.get("business3Step")
                .tasklet(new BizTask3Tasklet())
                .build();
    }

    public Step biz4Step() {
        return stepBuilderFactory.get("business4Step")
                .tasklet(new BizTask4Tasklet())
                .build();
    }

    public Step pagerDutySTep() {
        return stepBuilderFactory.get("pagerDutyStep")
                .tasklet(new PagerDutyTasklet())
                .build();
    }

    public Step cleanUpStep() {
        return stepBuilderFactory.get("cleanUpStep")
                .tasklet(new CleanUpTasklet())
                .build();
    }

    public Flow splitFlow() {
        return new FlowBuilder<SimpleFlow>("splitFlow")
                .split(new SimpleAsyncTaskExecutor())
                .add(fileFlow(), biz1Flow(), biz2Flow())
                .build();
    }

    public Flow fileFlow() {
        return new FlowBuilder<SimpleFlow>("fileFlow")
                .start(downloadStep())
                .next(processFileStep())
                .build();
    }

    public Flow biz1Flow() {
        return new FlowBuilder<SimpleFlow>("biz1Flow")
                .start(biz3Step())
                .build();
    }

    public Flow biz2Flow() {
        return new FlowBuilder<SimpleFlow>("bizFLow2")
                .start(biz4Step())
                .from(biz4Step()).on("*").end()
                .on("FAILED")
                .to(pagerDutySTep())
                .build();
    }

    @Bean
    public Job jobExecutor() {
        return jobBuilderFactory.get("helloWorldJob")
//                .start(step1())
//                .next(step2())
//                .next(writerSteps())
//                .next(multiThreadedStep())
//                .next(asyncStep())
                .start(splitFlow())
                .next(cleanUpStep())
                .end()
                .build();
    }
}
