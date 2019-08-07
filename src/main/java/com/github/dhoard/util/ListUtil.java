package com.github.dhoard.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Random;

public class ListUtil {

    private static final Random random = new Random();

    public static String randomElement(ArrayList<String> list) {
        return list.get(random.nextInt(list.size()));
    }

    public static ArrayList<String> buildList(Reader reader) throws IOException {
        ArrayList<String> stringList = new ArrayList<>();
        BufferedReader bufferedReader = new BufferedReader(reader);

        while (true) {
            String line = bufferedReader.readLine();
            if (null == line) {
                break;
            }

            if (!line.startsWith("#")) {
                stringList.add(line.trim());
            }
        }

        return stringList;
    }
}
