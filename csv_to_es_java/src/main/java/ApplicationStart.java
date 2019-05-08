import helper.CSVLoader;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import utility.Constants;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.elasticsearch.client.RestClient.builder;

/**
 * Created By: garvit
 * Date: 7/5/19
 **/

public class ApplicationStart {
    public static void main(String[] args) throws IOException, InterruptedException {

        ApplicationStart applicationStart = new ApplicationStart();
        InputStream inputStream = applicationStart.getInputStream();
        Properties properties = new Properties();
        properties.load(inputStream);

        String[] hosts = properties.getProperty(Constants.HOSTS).split(",");
        String[] ports = properties.getProperty(Constants.PORTS).split(",");

        HttpHost[] httpHost = new HttpHost[hosts.length];
        for (int i = 0; i < hosts.length; i++) {
            httpHost[i] = new HttpHost(hosts[i], Integer.parseInt(ports[i]), "http");
        }

        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                httpHost
        ));

        CSVLoader csvLoader = new CSVLoader(client);
        // index, type, filename
        csvLoader.CSVBulkImport(client, args[0], args[1], args[2], true);
    }

    private InputStream getInputStream() {
        return this.getClass().getClassLoader()
                .getResourceAsStream("database.properties");
    }
}
