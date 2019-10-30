package biz.cits.solr.client;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.solr.SolrAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@SpringBootApplication(exclude = SolrAutoConfiguration.class)
public class App {

    @Value("${solr.host.url}")
    String solrHostUrl;

    @Value("${solr.cloud.solrUrls}")
    List<String> solrCloudSolrUrls;

    @Value("${solr.cloud.zkHosts}")
    List<String> solrCloudZkHosts;

    @Value("${solr.cloud.zkFolder}")
    Optional<String> solrCloudZkFolder;

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {
            String[] beanNames = ctx.getBeanDefinitionNames();
            Arrays.sort(beanNames);
            for (String beanName : beanNames) {
                System.out.println(beanName);
            }

        };
    }

    @Bean
    public HttpSolrClient httpSolrClient() {
        HttpSolrClient client = new HttpSolrClient.Builder(solrHostUrl)
                .withConnectionTimeout(10000)
                .withSocketTimeout(60000)
                .build();
        return client;
    }

    @Bean
    public CloudSolrClient cloudSolrClient() {
        CloudSolrClient client = new CloudSolrClient.Builder(solrCloudZkHosts, solrCloudZkFolder)
                .withConnectionTimeout(10000)
                .withSocketTimeout(60000)
                .build();
        return client;
    }


}