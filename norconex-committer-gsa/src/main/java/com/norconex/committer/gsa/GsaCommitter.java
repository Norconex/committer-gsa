package com.norconex.committer.gsa;

import java.io.File;

import com.norconex.committer.ICommitter;
import com.norconex.commons.lang.map.Properties;

public class GsaCommitter implements ICommitter {

    private static final long serialVersionUID = -5010744222391427858L;

    @Override
    public void queueAdd(String reference, File document, Properties metadata) {
    }

    @Override
    public void queueRemove(String reference, File document, Properties metadata) {
    }

    @Override
    public void commit() {
    }
}
