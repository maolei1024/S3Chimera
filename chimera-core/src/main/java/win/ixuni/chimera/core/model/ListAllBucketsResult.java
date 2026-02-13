package win.ixuni.chimera.core.model;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import java.util.List;

/**
 * ListBuckets response result
 */
@JacksonXmlRootElement(localName = "ListAllMyBucketsResult", namespace = "http://s3.amazonaws.com/doc/2006-03-01/")
public class ListAllBucketsResult {

    @JacksonXmlProperty(localName = "Owner")
    private Owner owner;

    @JacksonXmlElementWrapper(localName = "Buckets")
    @JacksonXmlProperty(localName = "Bucket")
    private List<S3Bucket> buckets;

    public ListAllBucketsResult() {
    }

    public ListAllBucketsResult(Owner owner, List<S3Bucket> buckets) {
        this.owner = owner;
        this.buckets = buckets;
    }

    public Owner getOwner() {
        return owner;
    }

    public void setOwner(Owner owner) {
        this.owner = owner;
    }

    public List<S3Bucket> getBuckets() {
        return buckets;
    }

    public void setBuckets(List<S3Bucket> buckets) {
        this.buckets = buckets;
    }

    public static class Owner {
        @JacksonXmlProperty(localName = "ID")
        private String id;

        @JacksonXmlProperty(localName = "DisplayName")
        private String displayName;

        public Owner() {
        }

        public Owner(String id, String displayName) {
            this.id = id;
            this.displayName = displayName;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getDisplayName() {
            return displayName;
        }

        public void setDisplayName(String displayName) {
            this.displayName = displayName;
        }
    }

    public static ListAllBucketsResult of(List<S3Bucket> buckets) {
        Owner owner = new Owner("chimera", "S3Chimera");
        return new ListAllBucketsResult(owner, buckets);
    }
}
