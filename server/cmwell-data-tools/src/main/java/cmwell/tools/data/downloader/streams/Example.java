package cmwell.tools.data.downloader.streams;

import cmwell.tools.data.downloader.streams.japi.DownloaderUtils;
import scala.Option;

/**
 * Example of using calling Java API of
 * the {@link Downloader Downloader} code
 *
 * @see Downloader
 * @see DownloaderUtils
 */
public class Example {
    public static void main(String[] args) {
        DownloaderUtils.fromQuery(
                "localhost:9000",
                "/example.org",
                "",
                "",
                "ntriples",
                "stream",
                Option.apply(new Integer(50)),
                true
        );
    }
}
