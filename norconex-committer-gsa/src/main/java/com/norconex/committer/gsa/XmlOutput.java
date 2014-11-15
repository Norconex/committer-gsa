/* Copyright 2013 Norconex Inc.
 * 
 * This file is part of Norconex GSA Committer.
 * 
 * Norconex ElasticSearch GSA is free software: you can redistribute it
 * and/or modify it under the terms of the GNU General Public License as 
 * published by the Free Software Foundation, either version 3 of the License, 
 * or (at your option) any later version.
 * 
 * Norconex ElasticSearch GSA is distributed in the hope that it will be 
 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with Norconex GSA Committer. 
 * If not, see <http://www.gnu.org/licenses/>.
 */
package com.norconex.committer.gsa;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.CharEncoding;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.IAddOperation;
import com.norconex.committer.core.ICommitOperation;
import com.norconex.committer.core.IDeleteOperation;

public final class XmlOutput {

    private static final Logger LOG = LogManager.getLogger(XmlOutput.class);
    private final XMLStreamWriter writer;
    
    public XmlOutput(OutputStream out) throws XMLStreamException {
        XMLOutputFactory factory = XMLOutputFactory.newInstance();
        writer = factory.createXMLStreamWriter(
                out, CharEncoding.UTF_8);
    }

    public void write(List<ICommitOperation> batch)
            throws IOException, XMLStreamException {
        
        writer.writeStartDocument("UTF-8", "1.0");
        writer.writeDTD(
            "<!DOCTYPE gsafeed PUBLIC \"-//Google//DTD GSA Feeds//EN\" \"\">");
        writer.writeStartElement("gsafeed");
        writeHeader();
        writer.writeStartElement("group");

        int docAdded = 0;
        int docRemoved = 0;
        for (ICommitOperation op : batch) {
            if (op instanceof IAddOperation) {
                writeAdd((IAddOperation) op);
                docAdded++;
            } else if (op instanceof IDeleteOperation) {
                writeRemove((IDeleteOperation) op);
                docRemoved++;
            } else {
                throw new CommitterException("Unsupported operation:" + op);
            }
            writer.flush();
        }

        writer.writeEndElement(); // </group>
        writer.writeEndElement(); // </gsafeed>
        writer.writeEndDocument();
        writer.flush();
        writer.close();
        
        if (LOG.isInfoEnabled()) {
            LOG.info("Sent " + docAdded + " additions and " + docRemoved 
                    + " removals to GSA");
        }
    }
    
    private void writeHeader() throws XMLStreamException {
        // TODO get those values from elsewhere
        writer.writeStartElement("header");
        writeElement("datasource", "sample");
        writeElement("feedtype", "full");
        writer.writeEndElement();
    }
    
    private void writeElement(String name, String value) 
            throws XMLStreamException {
        writer.writeStartElement(name);
        writer.writeCharacters(value);
        writer.writeEndElement(); 
    }

    private void writeAdd(IAddOperation op)
            throws IOException, XMLStreamException {
        writer.writeStartElement("record");
        writer.writeAttribute("url", 
                op.getMetadata().getString("url"));
        writer.writeAttribute("mimetype", 
                op.getMetadata().getString("mimetype"));
        writer.writeAttribute("last-modified", 
                op.getMetadata().getString("last-modified"));
        writeContent(op);

        writer.writeEndElement();
    }

    private void writeContent(IAddOperation op) throws XMLStreamException,
            IOException {
        writer.writeStartElement("content");
        BufferedInputStream bufferedInput = null;
        byte[] buffer = new byte[1024 * 1024]; // 1MB
        try {
            bufferedInput = new BufferedInputStream(op.getContentStream());
            int bytesRead = 0;
            while ((bytesRead = bufferedInput.read(buffer)) != -1) {
                writer.writeCharacters(new String(buffer, 0, bytesRead));
            }
        } finally {
            IOUtils.closeQuietly(bufferedInput);
        }
        writer.writeEndElement();
    }
    
    private void writeRemove(IDeleteOperation op) 
            throws IOException, XMLStreamException {
        writer.writeStartElement("remove");
        writer.writeCharacters(op.getReference());
        writer.writeEndElement();
    }
}
