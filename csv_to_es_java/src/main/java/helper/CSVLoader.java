package helper;


import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created By: garvit
 * Date: 2/5/19
 * Package: helper;
 **/

public class CSVLoader {

    private RestHighLevelClient client;

    public CSVLoader(RestHighLevelClient client) {
        this.client = client;
    }

    public void CSVBulkImport(RestHighLevelClient client, String index, String type,
                              String fileName, boolean isHeaderIncluded) throws IOException, InterruptedException {

        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                System.out.println("Executing bulk "+ executionId + "with " + numberOfActions + " requests");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  BulkResponse response) {
                if (response.hasFailures()) {
                    System.out.println("Bulk " + executionId + " executed with failures");
                } else {
                    System.out.println("Bulk " + executionId + " completed in " + response.getTook().getMillis() + " milliseconds");
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                System.out.println("Failed to execute bulk" + failure);
            }
        };

        BulkProcessor bulkProcessor = BulkProcessor.builder(client::bulkAsync, listener).build();

        File file = new File(fileName);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
        String line = null;
        String[] headers = bufferedReader.readLine().split(",");

        while ((line = bufferedReader.readLine())!=null){
            if(line.trim().length()==0){
                continue;
            }
            String data [] = line.split(",");
            try {
                XContentBuilder xContentBuilder = jsonBuilder().startObject();
                for (int iterator = 0; iterator < headers.length; iterator++) {
                    xContentBuilder.field(headers[iterator], data[iterator]);
                }
                xContentBuilder.endObject();

                IndexRequest dataToIndex = new IndexRequest(index, type)
                        .source(xContentBuilder);

                bulkProcessor.add(dataToIndex);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        bufferedReader.close();
        boolean awaitClose = bulkProcessor.awaitClose(1L, TimeUnit.HOURS);
        System.out.println(awaitClose);
    }
}


   /*
    public void refreshIndices(){
        client.admin().indices()
                .prepareRefresh(indexName)
                .get(); //Refresh before search, so you will get latest indices result
    }

    public void search(){

        SearchResponse response = client.prepareSearch(indexName)
                .setTypes(indexTypeName)
                .get();
        //MatchAllDocQuery
        System.out.println("Total Hits : "+response.getHits().getTotalHits());
        System.out.println(response);
    }
    */
