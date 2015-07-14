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
                .setClusterName("dima-power-cluster")
                .setHost("localhost")
                .setIndex("wikipedia")
                .setType("article")
                .setOutputFilename(out)
                        .setScrollEnd(2)
                                .setScrollStart(1)
//                .setInputFilename(csv)
                .execute();
    }

    @Test
    public void LocalMainTest() throws IOException {

        String csv = getClass().getClassLoader().getResources("articlenames.txt").nextElement().getPath();
        String out = getClass().getClassLoader().getResources("test.txt").nextElement().getPath();

        ResultCollector.main(new String[]{
                "dima-power-cluster",
                "localhost",
                "wikipedia",
                "article",
                out,
                "nofilter",
                "0",
                "1"});
    }
}
