package org.example.datapipeline.parser;

import org.example.datapipeline.exception.PipelineValidationException;
import jakarta.xml.bind.*;
import org.example.datapipeline.config.Job;
import java.io.File;
import javax.xml.XMLConstants;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

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

    private String simplifyMessage(String raw) {

        if (raw.contains("One of '{output}' is expected")) {
            return "Missing required element <output> inside <task>";
        }

        // fallback
        return raw;
    }
    /**
     * Parses the pipeline XML file and returns the corresponding Job object.
     *
     * @param xmlPath path to the pipeline XML configuration file
     * @return Job object representing the parsed pipeline configuration
     * @throws Exception if the XML cannot be parsed or mapped correctly
     */
    public Job parse(String xmlPath) throws Exception {
        try {
            JAXBContext context = JAXBContext.newInstance(Job.class);
            Unmarshaller unmarshaller = context.createUnmarshaller();

            SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            Schema schema = sf.newSchema(new File("src/main/resources/schema/job.xsd"));

            unmarshaller.setSchema(schema);

            return (Job) unmarshaller.unmarshal(new File(xmlPath));

        } catch (UnmarshalException e) {
            throw new PipelineValidationException(formatError(e, xmlPath));
        }
    }

    private String formatError(UnmarshalException e, String xmlPath) {

        Throwable linked = e.getLinkedException();

        if (linked instanceof org.xml.sax.SAXParseException sax) {
            return "Pipeline validation failed\n" +
                    "File: " + xmlPath + "\n" +
                    "Line: " + sax.getLineNumber() +
                    ", Column: " + sax.getColumnNumber() + "\n" +
                    "Issue: " + simplifyMessage(sax.getMessage());
        }

        return "XML parsing failed: " + e.getMessage();
    }
}



