package cmwell.tools.data.ingester;

import cmwell.tools.data.ingester.japi.IngesterUtils;

import java.io.ByteArrayInputStream;

/**
 * Example of using calling Java API of the {@link cmwell.tools.data.ingester.Ingester Ingester} code
 *
 * @see cmwell.tools.data.ingester.Ingester
 * @see IngesterUtils
 */
public class Example {
    public static void main(String[] args) throws Exception {
        String data =
                "<http://example.org/subject-1> <http://example.org/predicate#p1> \"1\"^^<http://www.w3.org/2001/XMLSchema#int> .\n" +
                "<http://example.org/subject-1> <http://example.org/predicate#p2> \"2\"^^<http://www.w3.org/2001/XMLSchema#int> .\n" +
                "<http://example.org/subject-1> <http://example.org/predicate#p3> \"3\"^^<http://www.w3.org/2001/XMLSchema#int> .\n" +
                "<http://example.org/subject-1> <http://example.org/predicate#p4> \"4\"^^<http://www.w3.org/2001/XMLSchema#int> .\n" +
                "<http://example.org/subject-1> <http://example.org/predicate#p5> \"5\"^^<http://www.w3.org/2001/XMLSchema#int> .\n" +
                "<http://example.org/subject-1> <http://example.org/predicate#p6> \"6\"^^<http://www.w3.org/2001/XMLSchema#int> .\n" +
                "<http://example.org/subject-1> <http://example.org/predicate#p7> \"7\"^^<http://www.w3.org/2001/XMLSchema#int> .\n" +
                "<http://example.org/subject-1> <http://example.org/predicate#p8> \"8\"^^<http://www.w3.org/2001/XMLSchema#int> .\n" +
                "<http://example.org/subject-1> <http://example.org/predicate#p9> \"9\"^^<http://www.w3.org/2001/XMLSchema#int> .\n" +
                "<http://example.org/subject-1> <http://example.org/predicate#p10> \"10\"^^<http://www.w3.org/2001/XMLSchema#int> .\n";

        IngesterUtils.fromInputStream("localhost:9000",
                "ntriples",
                null,
                new ByteArrayInputStream(data.getBytes("utf-8")),
                () -> { System.out.println("done!"); }
        );
    }
}
