package storm.starter.model;

public class DataModel {

    private String network;
    private String site;
    private String tag;

    public DataModel() {
    }

    public DataModel(String network, String site, String tag) {
        this.network = network;
        this.site = site;
        this.tag = tag;
    }

    public String getNetwork() {
        return network;
    }

    public void setNetwork(String network) {
        this.network = network;
    }

    public String getSite() {
        return site;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String get(DataFields field) {
        return field.get(this);
    }
}
