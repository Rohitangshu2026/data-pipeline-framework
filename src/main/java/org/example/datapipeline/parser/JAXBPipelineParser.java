package org.example.datapipeline.parser;

import org.example.datapipeline.exception.PipelineValidationException;
import jakarta.xml.bind.*;
import org.example.datapipeline.config.Job;
import java.io.File;
import javax.xml.XMLConstants;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

/**
 * Deserialises a pipeline XML file into a {@link Job} object graph using JAXB.
 *
 * <p>This parser is the first step in the pipeline lifecycle. It reads the XML file,
 * validates it against the bundled W3C XML Schema ({@code src/main/resources/schema/job.xsd}),
 * and unmarshals the validated document into the annotated config POJOs
 * ({@link Job}, {@link org.example.datapipeline.config.Stage},
 * {@link org.example.datapipeline.config.Task}, etc.) using JAXB's
 * {@link jakarta.xml.bind.Unmarshaller}.
 *
 * <h2>Schema Validation</h2>
 * <p>The XSD schema is loaded once per parse call and attached to the unmarshaller via
 * {@link jakarta.xml.bind.Unmarshaller#setSchema(javax.xml.validation.Schema)}. Any
 * structural XML violation (missing required element, wrong attribute type, unknown element)
 * is caught as a {@link jakarta.xml.bind.UnmarshalException} and converted into a
 * {@link org.example.datapipeline.exception.PipelineValidationException} with a
 * human-readable error message that includes the file name, line number, and column number.
 *
 * <h2>Error Messages</h2>
 * <p>Some JAXB error messages are verbose and technical. The {@link #simplifyMessage} method
 * maps known patterns to friendlier alternatives (e.g. "One of '{output}' is expected" →
 * "Missing required element {@literal <output>} inside {@literal <task>}").
 *
 * <p>After parsing, the caller is responsible for calling
 * {@link Job#resolveDatasources()}, {@link org.example.datapipeline.validator.SemanticValidator#validate(Job)},
 * and {@link org.example.datapipeline.util.ConfigNormalizer#normalize(Job)} before
 * executing the pipeline.
 */
public class JAXBPipelineParser {

    /**
     * Maps known verbose JAXB/SAX error messages to shorter, user-facing alternatives.
     *
     * @param raw the raw error message from the SAX parser
     * @return a simplified message if a known pattern matches, otherwise the original message
     */
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

    /**
     * Formats a {@link jakarta.xml.bind.UnmarshalException} into a human-readable error
     * string that includes file path, line, column, and a simplified issue description.
     *
     * @param e       the exception thrown by the JAXB unmarshaller
     * @param xmlPath the path of the XML file being parsed (for the error message)
     * @return a multi-line formatted error string
     */
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



