package com.snapscore.pipeline.textsearch;

import java.util.List;

public record TestSynonymsEntry(String identifier,
                                String primaryName,
                                List<String> synonyms) implements SynonymsEntry {

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getPrimaryName() {
        return primaryName;
    }

    @Override
    public List<String> getSynonyms() {
        return synonyms;
    }

}
