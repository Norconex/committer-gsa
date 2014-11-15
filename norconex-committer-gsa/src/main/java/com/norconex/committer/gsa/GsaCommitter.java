package com.norconex.committer.gsa;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.norconex.committer.core.AbstractMappedCommitter;
import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.ICommitOperation;

public class GsaCommitter extends AbstractMappedCommitter {

    private final CloseableHttpClient httpclient;
    
    private String feedUrl;

    public GsaCommitter() {
        super();
        httpclient = HttpClients.createDefault();
    }

    public String getFeedUrl() {
        return feedUrl;
    }

    public void setFeedUrl(String feedUrl) {
        this.feedUrl = feedUrl;
    }

    @Override
    protected void commitBatch(List<ICommitOperation> batch) {

        File xmlFile = null;
        try {
            xmlFile = File.createTempFile("batch", ".xml");
            FileOutputStream fout = new FileOutputStream(xmlFile);
            XmlOutput xmlOutput = new XmlOutput(fout);
            xmlOutput.write(batch);
            fout.close();
            
            HttpPost post = new HttpPost(feedUrl);
            MultipartEntityBuilder builder = MultipartEntityBuilder.create();
            builder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE);
            builder.addBinaryBody("data", xmlFile, 
                    ContentType.APPLICATION_XML, xmlFile.getName());
            builder.addTextBody("datasource", "committer");
            builder.addTextBody("feedtype", "full");
            
            HttpEntity entity = builder.build();
            post.setEntity(entity);
            CloseableHttpResponse response = httpclient.execute(post);
            StatusLine status = response.getStatusLine();
            if (status.getStatusCode() != 200) {
                throw new CommitterException(
                        "Invalid response to Committer HTTP request. "
                      + "Response code: " + status.getStatusCode()
                      + ". Response Message: " + status.getReasonPhrase());
            }
            
        } catch (Exception e) {
            throw new CommitterException(
                    "Cannot index document batch to GSA.", e);
        } finally {
            FileUtils.deleteQuietly(xmlFile);
        }
    }
    
    @Override
    protected void loadFromXml(XMLConfiguration xml) {
        setFeedUrl(xml.getString("feedUrl", null));
    }

    @Override
    protected void saveToXML(XMLStreamWriter writer) throws XMLStreamException {
    }
}
