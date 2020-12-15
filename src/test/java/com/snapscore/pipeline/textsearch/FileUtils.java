package com.snapscore.pipeline.textsearch;

import com.snapscore.pipeline.logging.Logger;
import java.util.*;


public class FileUtils {

    private static final Logger log = Logger.setup(FileUtils.class);

    public static final String ILLEGAL_FILE_CHARACTERS = "[\\\\/:*?\"<>|]";


    public static List<String> readFileLinesFromResourcesDir(String fileName) {
        try {
            ClassLoader classLoader = FileUtils.class.getClassLoader();
            Scanner scanner = new Scanner((classLoader.getResourceAsStream(fileName)));
            List<String> lines = new ArrayList<>();
            while (scanner.hasNextLine()) {
                String word = scanner.nextLine();
                if (word != null) {
                    lines.add(word);
                }
            }
            return lines;
        } catch (Exception e) {
            log.error("Error reading data from resource file {}!", fileName, e);
            return Collections.EMPTY_LIST;
        }
    }

}
