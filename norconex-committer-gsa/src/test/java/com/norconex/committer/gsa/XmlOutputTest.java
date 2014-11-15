package com.norconex.committer.gsa;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import com.norconex.committer.core.IAddOperation;
import com.norconex.committer.core.ICommitOperation;
import com.norconex.commons.lang.map.Properties;

public class XmlOutputTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    
    @Test
    public void testWriteFile() throws Exception {
        
        File file = tempFolder.newFile();
        FileOutputStream out = new FileOutputStream(file);
        XmlOutput xmlOutput = new XmlOutput(out);
        
        Properties metadata = new Properties();
        metadata.addString("url", "http://www.corp.enterprise.com/hello01");
        metadata.addString("mimetype", "text/plain");
        metadata.addString("last-modified", "Tue, 6 Nov 2007 12:45:26 GMT");
        String content = "This is hello01";
        
        xmlOutput.write(Arrays.asList(
                buildMockAddOperation(metadata, content)));
        out.close();
        
        String xml = FileUtils.readFileToString(file, "UTF-8");
        assertEquals(xml,
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                + "<!DOCTYPE gsafeed PUBLIC \"-//Google//DTD GSA Feeds//EN\" \"\">"
                + "<gsafeed>"
                + "<header>"
                + "<datasource>sample</datasource>"
                + "<feedtype>full</feedtype>"
                + "</header>"
                + "<group>"
                + "<record "
                + "url=\"http://www.corp.enterprise.com/hello01\" "
                + "mimetype=\"text/plain\" "
                + "last-modified=\"Tue, 6 Nov 2007 12:45:26 GMT\">"
                + "<content>This is hello01</content>"
                + "</record>"
                + "</group>"
                + "</gsafeed>");
    }
    
    @Test
    public void testWriteMultipleRecords() throws Exception {
        
        File file = tempFolder.newFile();
        FileOutputStream out = new FileOutputStream(file);
        XmlOutput xmlOutput = new XmlOutput(out);
        
        Properties metadata1 = new Properties();
        metadata1.addString("url", "http://www.corp.enterprise.com/hello01");
        metadata1.addString("mimetype", "text/plain");
        metadata1.addString("last-modified", "Tue, 6 Nov 2007 12:45:26 GMT");
        String content1 = "This is hello01";
        
        Properties metadata2 = new Properties();
        metadata2.addString("url", "http://www.corp.enterprise.com/hello02");
        metadata2.addString("mimetype", "text/plain");
        metadata2.addString("last-modified", "Tue, 6 Nov 2009 22:45:26 GMT");
        String content2 = "This is hello02";
        
        xmlOutput.write(Arrays.asList(
                buildMockAddOperation(metadata1, content1),
                buildMockAddOperation(metadata2, content2)));

        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setFeature(
                "http://apache.org/xml/features/nonvalidating/load-external-dtd", 
                false);
        Document doc = dbf.newDocumentBuilder().parse(file);
        XPath xpath = XPathFactory.newInstance().newXPath();
        NodeList nodes = (NodeList) xpath.evaluate(
                "/gsafeed/group/record", doc, XPathConstants.NODESET);
        assertEquals(2, nodes.getLength());
    }
    
    private ICommitOperation buildMockAddOperation(
            final Properties metadata, final String content) {
        return new IAddOperation() {
            private static final long serialVersionUID = -3080062268217542318L;
            @Override
            public String getReference() {
                return metadata.getString("url");
            }
            @Override
            public void delete() {
            }
            @Override
            public Properties getMetadata() {
                return metadata;
            }
            @Override
            public InputStream getContentStream() throws IOException {
                return new ByteArrayInputStream(content.getBytes("UTF-8"));
            }
        };
    }
}
