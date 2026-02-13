package win.ixuni.chimera.core.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * S3 Access Control Policy
 * 
 * S3 ACL response format:
 * <pre>
 * &lt;AccessControlPolicy&gt;
 *     &lt;Owner&gt;
 *         &lt;ID&gt;owner-id&lt;/ID&gt;
 *         &lt;DisplayName&gt;owner-name&lt;/DisplayName&gt;
 *     &lt;/Owner&gt;
 *     &lt;AccessControlList&gt;
 *         &lt;Grant&gt;
 *             &lt;Grantee xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="CanonicalUser"&gt;
 *                 &lt;ID&gt;user-id&lt;/ID&gt;
 *                 &lt;DisplayName&gt;user-name&lt;/DisplayName&gt;
 *             &lt;/Grantee&gt;
 *             &lt;Permission&gt;FULL_CONTROL&lt;/Permission&gt;
 *         &lt;/Grant&gt;
 *     &lt;/AccessControlList&gt;
 * &lt;/AccessControlPolicy&gt;
 * </pre>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JacksonXmlRootElement(localName = "AccessControlPolicy")
public class AccessControlPolicy {

    @JacksonXmlProperty(localName = "Owner")
    private Owner owner;

    @JacksonXmlProperty(localName = "AccessControlList")
    private AccessControlList accessControlList;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Owner {
        @JacksonXmlProperty(localName = "ID")
        private String id;

        @JacksonXmlProperty(localName = "DisplayName")
        private String displayName;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AccessControlList {
        @JacksonXmlElementWrapper(useWrapping = false)
        @JacksonXmlProperty(localName = "Grant")
        private List<Grant> grants;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Grant {
        @JacksonXmlProperty(localName = "Grantee")
        private Grantee grantee;

        @JacksonXmlProperty(localName = "Permission")
        private String permission;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Grantee {
        @JacksonXmlProperty(isAttribute = true, localName = "type", namespace = "http://www.w3.org/2001/XMLSchema-instance")
        private String type;

        @JacksonXmlProperty(localName = "ID")
        private String id;

        @JacksonXmlProperty(localName = "DisplayName")
        private String displayName;

        @JacksonXmlProperty(localName = "URI")
        private String uri;
    }

    /**
     * 创建默认的完全控制 ACL
     */
    public static AccessControlPolicy createDefaultFullControl(String ownerId, String ownerDisplayName) {
        return AccessControlPolicy.builder()
                .owner(Owner.builder()
                        .id(ownerId)
                        .displayName(ownerDisplayName)
                        .build())
                .accessControlList(AccessControlList.builder()
                        .grants(List.of(Grant.builder()
                                .grantee(Grantee.builder()
                                        .type("CanonicalUser")
                                        .id(ownerId)
                                        .displayName(ownerDisplayName)
                                        .build())
                                .permission("FULL_CONTROL")
                                .build()))
                        .build())
                .build();
    }

    /**
     * 权限常量
     */
    public static class Permission {
        public static final String FULL_CONTROL = "FULL_CONTROL";
        public static final String READ = "READ";
        public static final String WRITE = "WRITE";
        public static final String READ_ACP = "READ_ACP";
        public static final String WRITE_ACP = "WRITE_ACP";
    }
}
