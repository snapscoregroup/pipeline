package com.snapscore.pipeline.textsearch;

import reactor.util.annotation.Nullable;

import java.util.List;

public interface SynonymsEntry {

    /**
     * Identifies an entity (e.g. team, stage ...) for whose names the synonyms are for
     * @return can return null but in that case non-null PrimaryName should be provided
     */
    @Nullable
    String getIdentifier();

    List<String> getSynonyms();

}
