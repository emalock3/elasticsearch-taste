package org.codelibs.elasticsearch.taste.similarity;

import java.util.Map;

import org.codelibs.elasticsearch.taste.model.DataModel;
import org.codelibs.elasticsearch.taste.util.SettingsUtils;

public abstract class AbstractUserSimilarityFactory<T> implements
        SimilarityFactory<T> {

    protected DataModel dataModel;

    @Override
    public void init(final Map<String, Object> settings) {
        dataModel = SettingsUtils.get(settings, "dataModel");
    }

}