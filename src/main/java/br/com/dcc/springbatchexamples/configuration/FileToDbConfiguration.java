package br.com.dcc.springbatchexamples.configuration;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import br.com.dcc.springbatchexamples.domain.Customer;
import br.com.dcc.springbatchexamples.domain.mapper.CustomerFieldSetMapper;
import br.com.dcc.springbatchexamples.listener.SimpleChunkListener;

@Configuration
public class FileToDbConfiguration {

	@Bean
	public FlatFileItemReader<Customer> fileToDbReader() {

		FlatFileItemReader<Customer> reader = new FlatFileItemReader<>();
		reader.setLinesToSkip(1);
		reader.setResource(new ClassPathResource("customer.csv"));
		DefaultLineMapper<Customer> customerLineMapper = new DefaultLineMapper<>();
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setNames("email", "firstName", "lastName");
		customerLineMapper.setLineTokenizer(tokenizer);
		customerLineMapper.setFieldSetMapper(new CustomerFieldSetMapper());
		customerLineMapper.afterPropertiesSet();
		reader.setLineMapper(customerLineMapper);

		return reader;
	}

	@Bean
	public JdbcBatchItemWriter<Customer> fileToDbWriter(DataSource dataSource) {
		JdbcBatchItemWriter<Customer> itemWriter = new JdbcBatchItemWriter<>();
		itemWriter.setDataSource(dataSource);
		itemWriter.setSql("INSERT INTO CUSTOMER_INSERT (email, firstName, lastName) VALUES (:email, :firstName, :lastName)");
		itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
		itemWriter.afterPropertiesSet();
		return itemWriter;
	}

	@Bean
	public Step fileToDbStep1(StepBuilderFactory stepBuilderFactory, DataSource dataSource) {
		return stepBuilderFactory.get("FileToDbStep1")
				.<Customer, Customer>chunk(10)
				.listener(new SimpleChunkListener())
				.reader(fileToDbReader())
				.writer(fileToDbWriter(dataSource))
				.build();
	}

	@Bean
	public Job fileToDbJob(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, DataSource dataSource) {
		return jobBuilderFactory.get("FileToDbJob")
				.start(fileToDbStep1(stepBuilderFactory, dataSource))
				.build();

	}
}
