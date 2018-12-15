package biz.cits.solr.client.ingest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
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
        json.forEach(x -> {
            String id = UUID.randomUUID().toString();
            final SolrInputDocument doc = new SolrInputDocument();
            x.fieldNames().forEachRemaining(y -> {
                        JsonNode yNode = x.get(y);
                        if (yNode.isArray()) {
                            ArrayList<SolrInputDocument> childDocs = new ArrayList<>();
                            yNode.forEach(yNodeChild -> {
                                SolrInputDocument childDoc = getSolrDocument(yNodeChild);
                                childDoc.addField("doc_type", "child");
                                childDoc.addField("path", id + "---" + childDoc.getField("id"));
                                childDocs.add(childDoc);
                            });
                            doc.addChildDocuments(childDocs);
                        } else if (yNode.isObject()) {
                            SolrInputDocument childDoc = getSolrDocument(yNode);
                            childDoc.addField("doc_type", "child");
                            childDoc.addField("path", id + "---" + childDoc.getField("id"));
                            doc.addChildDocument(childDoc);
                        } else {
                            doc.addField(y, yNode.asText());
                        }
                    }
            );
            doc.addField("id", id);
            doc.addField("doc_type", "master");
            doc.addField("path", id);
            try {
                client.add("test", doc);
                client.commit("test");
            } catch (SolrServerException | IOException e) {
                e.printStackTrace();
            }
        });

    }

    private SolrInputDocument getNestedSolrDoc(JsonNode x, SolrInputDocument doc, int levelCtr) {
        int thisLevelCtr = levelCtr + 1;
        SolrInputDocument outDoc = new SolrInputDocument();
        outDoc.putAll(doc);
        x.fieldNames().forEachRemaining(y -> {
            JsonNode yNode = x.get(y);
            if (yNode.isArray()) {
                ArrayList<SolrInputDocument> childDocs = new ArrayList<>();
                yNode.forEach(yNodeChild -> {
                    SolrInputDocument childDoc = getNestedSolrDoc(yNodeChild, outDoc, thisLevelCtr);
                    childDoc.addField("id", UUID.randomUUID());
                    childDoc.addField("level", thisLevelCtr);
                    childDocs.add(childDoc);
                });
                outDoc.addChildDocuments(childDocs);
            } else if (yNode.isObject()) {
                SolrInputDocument childDoc = getNestedSolrDoc(yNode, outDoc, thisLevelCtr);
                childDoc.addField("level", thisLevelCtr);
                outDoc.addChildDocument(childDoc);
            } else {
                outDoc.addField(y, yNode.asText());
            }
        });
        return outDoc;
    }

    private SolrInputDocument getSolrDocument(JsonNode x) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", UUID.randomUUID().toString());
        x.fieldNames().forEachRemaining(y -> {
            JsonNode val = x.get(y);
            if (y.startsWith("claim_number") || y.indexOf("_id") > 0) {
                doc.addField(y, val.asText());
            } else if (val.isNumber()) {
                doc.addField(y, val.asDouble());
            } else {
                doc.addField(y, val.asText());
            }
        });

        return doc;
    }

}