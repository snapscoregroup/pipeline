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

    /**
     * "Primary" name of the entity (e.g. team, stage ...) that can be searched. The synonyms are tied to this name.
     * @return can return null but in that case non-null identifier should be provided
     */
    @Nullable
    String getPrimaryName();

    List<String> getSynonyms();

}
