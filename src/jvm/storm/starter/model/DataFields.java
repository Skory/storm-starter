package storm.starter.model;

public enum DataFields {
    network {
        @Override
        public String get(DataModel model) {
            return model.getNetwork();
        }
    }, site {
        @Override
        public String get(DataModel model) {
            return model.getSite();
        }
    }, tag {
        @Override
        public String get(DataModel model) {
            return model.getTag();
        }
    };

    public abstract String get(DataModel model);
}
