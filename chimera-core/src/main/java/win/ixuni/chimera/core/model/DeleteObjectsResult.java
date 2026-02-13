package win.ixuni.chimera.core.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.Builder;
import lombok.Data;

import java.util.List;

/**
 * 批量删除对象结果
 */
@Data
@Builder
@JacksonXmlRootElement(localName = "DeleteResult")
public class DeleteObjectsResult {

    /**
     * 成功删除的对象
     */
    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "Deleted")
    private List<DeletedObject> deleted;

    /**
     * 删除失败的对象
     */
    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "Error")
    private List<DeleteError> errors;

    @Data
    @Builder
    public static class DeletedObject {
        @JacksonXmlProperty(localName = "Key")
        private String key;
        
        @JacksonXmlProperty(localName = "VersionId")
        private String versionId;
        
        @JacksonXmlProperty(localName = "DeleteMarker")
        private Boolean deleteMarker;
    }

    @Data
    @Builder
    public static class DeleteError {
        @JacksonXmlProperty(localName = "Key")
        private String key;
        
        @JacksonXmlProperty(localName = "VersionId")
        private String versionId;
        
        @JacksonXmlProperty(localName = "Code")
        private String code;
        
        @JacksonXmlProperty(localName = "Message")
        private String message;
    }
}
