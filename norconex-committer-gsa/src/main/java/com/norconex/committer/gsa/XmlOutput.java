/* Copyright 2013-2014 Norconex Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.norconex.committer.gsa;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.CharEncoding;

import com.norconex.committer.core.CommitterException;
import com.norconex.committer.core.IAddOperation;
import com.norconex.committer.core.ICommitOperation;
import com.norconex.committer.core.IDeleteOperation;

public final class XmlOutput {

    private final XMLStreamWriter writer;

    public XmlOutput(OutputStream out) throws XMLStreamException {
        XMLOutputFactory factory = XMLOutputFactory.newInstance();
        writer = factory.createXMLStreamWriter(
                out, CharEncoding.UTF_8);
    }

    public Map<String, Integer> write(List<ICommitOperation> batch)
            throws IOException, XMLStreamException {

        writer.writeStartDocument("UTF-8", "1.0");
        writer.writeDTD(
                "<!DOCTYPE gsafeed PUBLIC \"-//Google//DTD GSA Feeds//EN\" \"\">");
        writer.writeStartElement("gsafeed");
        writeHeader();
        writer.writeStartElement("group");

        Map<String, Integer> stats = new HashMap<String, Integer>();
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

        stats.put("docAdded", docAdded);
        stats.put("docRemoved", docRemoved);

        return stats;

    }

    private void writeHeader() throws XMLStreamException {
        // TODO get those values from elsewhere
        writer.writeStartElement("header");
        writeElement("datasource", "GSA_Commiter");
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
                op.getReference());
        writer.writeAttribute("action", "add");
        writer.writeAttribute("mimetype", 
                op.getMetadata().getString("collector.content-type"));
        writer.writeAttribute("last-modified", 
                op.getMetadata().getString("Date"));
        if (op.getMetadata().getString("title") != null) writeMetadata(op);
        writeContent(op);

        writer.writeEndElement();
    }

    private void writeContent(IAddOperation op) throws XMLStreamException,
    IOException {
        writer.writeStartElement("content");
//                String encoding = op.getMetadata().getString(
//                        "collector.content-encoding");
//                if (encoding.equalsIgnoreCase("UTF-8")) encoding = "utf8";
//                writer.writeAttribute("encoding", encoding);
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

    private void writeMetadata(IAddOperation op) throws XMLStreamException {
        writer.writeStartElement("metadata");
        writer.writeStartElement("meta");
        writer.writeAttribute("name", "title");
        writer.writeAttribute("content", op.getMetadata().getString("title"));
        writer.writeEndElement();
        writer.writeEndElement();
    }

    private void writeRemove(IDeleteOperation op) 
            throws IOException, XMLStreamException {
        writer.writeStartElement("record");
        writer.writeAttribute("url", 
                op.getReference());
        writer.writeAttribute("action", "remove");
        writer.writeEndElement();
    }
}
