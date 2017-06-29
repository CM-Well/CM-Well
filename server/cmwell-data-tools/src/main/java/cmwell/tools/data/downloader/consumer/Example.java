package cmwell.tools.data.downloader.consumer;

import cmwell.tools.data.downloader.consumer.japi.DownloaderUtils;
import scala.collection.Iterator;

/**
 * Example of using stream-based {@link cmwell.tools.data.downloader.streams.Downloader Downloader} code from Java API
 *
 * @see cmwell.tools.data.downloader.streams.Downloader
 * @see DownloaderUtils
 */
public class Example {
    public static void main(String[] args) {
        Iterator<String> iterator = DownloaderUtils.createIterator(
                "localhost:9000",
                "/example.org",
                null,
                null,
                true,
                "ntriples",
                50,
                30 * 1000,
                null
        );

        while (iterator.hasNext()) {
            System.out.println("current token =" + iterator.toString() +
                    " current data =" + iterator.next());
        }
    }
}
