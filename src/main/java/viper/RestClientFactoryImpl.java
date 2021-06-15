package viper;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClientBuilder;

public class RestClientFactoryImpl implements org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory {
    @Override
    public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
        Header[] headers = new BasicHeader[]{new BasicHeader("Content-Type","application/json")};
        restClientBuilder.setDefaultHeaders(headers);
    }
}
