package de.tuberlin.dima.mschwarzer.lucene.tests;

import de.tuberlin.dima.mschwarzer.lucene.morelikethis.ResultCollector;
import org.junit.Test;

import java.io.IOException;

public class MoreLikeThisTest {
    @Test
    public void LocalTest() throws IOException {

        String csv = getClass().getClassLoader().getResources("articlenames.txt").nextElement().getPath();
        String out = getClass().getClassLoader().getResources("test.txt").nextElement().getPath();

        new ResultCollector()
                .setClusterName(null)
                .setHost("localhost")
                .setIndex("wikipedia")
                .setType("article")
                .execute(csv, out);
    }
}
