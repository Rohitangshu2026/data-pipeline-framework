package org.example.datapipeline.parser;

import jakarta.xml.bind.*;

import org.example.datapipeline.config.Job;

import java.io.File;

/**
 * Parser responsible for converting a pipeline XML configuration
 * into a Job object graph using JAXB.
 *
 * This parser reads the XML pipeline definition and maps it to
 * the corresponding configuration classes defined in the config
 * package. The resulting Job object represents the pipeline
 * structure and can be further validated and normalized before
 * execution.
 */
public class JAXBPipelineParser {

    /**
     * Parses the pipeline XML file and returns the corresponding Job object.
     *
     * @param xmlPath path to the pipeline XML configuration file
     * @return Job object representing the parsed pipeline configuration
     * @throws Exception if the XML cannot be parsed or mapped correctly
     */
    public Job parse(String xmlPath) throws Exception {

        JAXBContext context = JAXBContext.newInstance(Job.class);

        Unmarshaller unmarshaller = context.createUnmarshaller();

        return (Job) unmarshaller.unmarshal(new File(xmlPath));
    }
}