package biz.cits.solr.client.proxy;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.params.CommonParams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Optional;

@RestController
public class SolrCloudProxy {

    @Autowired
    CloudSolrClient cloudSolrClient;

    @RequestMapping(value = "/query",
            method = RequestMethod.POST,
            consumes = "application/json", produces = "application/json")
    public String query(@RequestParam String collection, @RequestBody Optional<String> jsonQueryBody, @RequestParam Optional<String> jsonQueryParam) throws IOException, SolrServerException {
        String jsonQuery = "No Query Found";
        if (jsonQueryParam.isPresent()) {
            jsonQuery = jsonQueryParam.get();
        } else if (jsonQueryBody.isPresent()) {
            jsonQuery = jsonQueryBody.get();
        } else {
            return jsonQuery;
        }

        NoOpResponseParser responseParser = new NoOpResponseParser();

        responseParser.setWriterType("json");
        cloudSolrClient.setParser(responseParser);

        GenericSolrRequest solrRequest = new GenericSolrRequest(SolrRequest.METHOD.POST, "/select",null);
//        solrRequest.setBasicAuthCredentials("solr", "SolrRocks");
        solrRequest.setUseV2(true);
        RequestWriter.StringPayloadContentWriter contentWriter = new RequestWriter.StringPayloadContentWriter(jsonQuery, CommonParams.JSON_MIME);
        solrRequest.setContentWriter(contentWriter);
        SimpleSolrResponse response = solrRequest.process(cloudSolrClient, collection);
        String out = response.getResponse().toString();
        System.out.println(out);
        return out;
    }

}
