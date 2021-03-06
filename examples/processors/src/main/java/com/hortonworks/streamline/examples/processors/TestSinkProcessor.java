/**
  * Copyright 2017 Hortonworks.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at

  *   http://www.apache.org/licenses/LICENSE-2.0

  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
 **/


package com.hortonworks.streamline.examples.processors;


import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.exception.ConfigException;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TestSinkProcessor implements CustomProcessorRuntime {
    protected static final Logger LOG = LoggerFactory.getLogger(TestSinkProcessor.class);
    public static final String FILE_FIELD_NAME = "file";
    Map<String, Object> config;
    OutputStreamWriter writer;

    public void initialize(Map<String, Object> config) {
        try {
            this.config = config;
            LOG.info("Initializing with config field " + FILE_FIELD_NAME + " = " + this.config.get(FILE_FIELD_NAME).toString());
            this.writer = new OutputStreamWriter(new FileOutputStream(this.config.get(FILE_FIELD_NAME).toString() + UUID.randomUUID()), "UTF-8");
        } catch(IOException e) {
            LOG.error("Failed to create file ", e);
        }
    }

    public void validateConfig(Map<String, Object> config) throws ConfigException {
        LOG.debug("Validating config ");
        if (!config.containsKey(FILE_FIELD_NAME)) {
            throw new ConfigException("Missing config field: " + FILE_FIELD_NAME);
        }
        LOG.debug("Config valid ");
    }

    public List<StreamlineEvent> process(StreamlineEvent streamlineEvent) throws ProcessingException {
        try {
            LOG.debug("Processing {}", streamlineEvent);
            JSONObject json = new JSONObject();
            json.putAll(streamlineEvent);
            writer.write(json.toJSONString());
            writer.flush();
        } catch(Exception e) {
            LOG.error("Failed to process event", e);
            throw new ProcessingException(e);
        }
        return null;
    }

    public void cleanup() {
        LOG.debug("Cleaning up");
        try {
            writer.flush();
            writer.close();
        } catch(Exception e) {
            LOG.error("failed to close", e);
        }
    }
}
