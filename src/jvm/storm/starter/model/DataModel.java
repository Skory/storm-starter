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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataModel dataModel = (DataModel) o;

        if (!network.equals(dataModel.network)) return false;
        if (!site.equals(dataModel.site)) return false;
        if (!tag.equals(dataModel.tag)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = network.hashCode();
        result = 31 * result + site.hashCode();
        result = 31 * result + tag.hashCode();
        return result;
    }
}
