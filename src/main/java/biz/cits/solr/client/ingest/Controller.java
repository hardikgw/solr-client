package biz.cits.solr.client.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

@RestController
public class Controller {

    @Autowired
    HttpSolrClient client;


    @RequestMapping(value = "/create",
            method = RequestMethod.POST,
            consumes = "text/plain")
    public String index(@RequestBody String data) throws IOException, SolrServerException {
        indexDocument(data);
        Assert.notNull(client, "No Client");
        return "ok";
    }

    private void indexDocument(String data) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readValue(data, JsonNode.class);

        SolrInputDocument doc = new SolrInputDocument();
        json.forEach(x -> {
            x.fieldNames().forEachRemaining(y -> {
                        JsonNode yNode = x.get(y);
                        if (yNode.isArray()) {
                            yNode.forEach(yNodeChild -> {
                                doc.addChildDocument(getSolrDocument(yNodeChild));
                            });
                        } else if (yNode.isObject()) {
                            doc.addChildDocument(getSolrDocument(yNode));
                        } else {
                            if (yNode.toString().toLowerCase().indexOf("date") > 0) {

                            } else {
                                doc.addField(y, yNode.asText());
                            }
                        }
                    }
            );
            doc.addField("id", UUID.randomUUID().toString());
            try {
                client.add("test", doc);
                client.commit("test");
            } catch (SolrServerException | IOException e) {
                e.printStackTrace();
            }
        });

    }

    private SolrInputDocument getSolrDocument(JsonNode x) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", UUID.randomUUID().toString());
        x.fieldNames().forEachRemaining(y -> {
            doc.addField(y, x.get(y).textValue());
        });
        return doc;
    }

}