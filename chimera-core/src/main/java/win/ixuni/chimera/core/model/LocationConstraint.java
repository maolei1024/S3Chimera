package win.ixuni.chimera.core.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlText;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Bucket region location response
 * <p>
 * Used for GET /{bucket}?location response
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JacksonXmlRootElement(localName = "LocationConstraint")
public class LocationConstraint {

    /**
     * Region name
     * <p>
     * If us-east-1 (AWS standard region), S3 may return null
     */
    @JacksonXmlText
    private String location;
}
