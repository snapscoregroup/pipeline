package com.snapscore.pipeline.textsearch;

import java.util.regex.Pattern;

public class StringUtils {

    private static final Pattern DUPLICATED_SPACE_PATTERN = Pattern.compile("\\s{2,}");

    static String trim(String str) {
        if (str != null) {
            return DUPLICATED_SPACE_PATTERN.matcher(str).replaceAll(" ").trim();
        } else {
            return str;
        }
    }

}
