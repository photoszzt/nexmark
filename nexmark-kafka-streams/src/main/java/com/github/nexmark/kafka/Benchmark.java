package com.github.nexmark.kafka;

import org.apache.commons.cli.*;

import java.io.File;
import java.nio.file.Path;
import java.util.*;

public class Benchmark {
    // TODO: remove this once q6 is supported
    private static final Set<String> UNSUPPORTED_QUERIES = new HashSet<>(Arrays.asList("q6", "q9", "q10"));

    private static final Option LOCATION = new Option("l", "location", true,
            "nexmark directory.");
    private static final Option QUERIES = new Option("q", "queries", true,
            "Query to run. If the value is 'all', all queries will be run.");

    public static void main(String[] args) throws ParseException {
        if (args == null || args.length == 0) {
            throw new RuntimeException("Usage: --queries q1,q3 --location /path/to/nexmark");
        }
        Options options = getOptions();
        DefaultParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args, true);
        Path location = new File(line.getOptionValue(LOCATION.getOpt())).toPath();
        Path queryLocation = location.resolve("queries");
        List<String> queries = getQueries(queryLocation, line.getOptionValue(QUERIES.getOpt()));
        System.out.println("Benchmark Queries: " + queries);
    }

    private static Options getOptions() {
        Options options = new Options();
        options.addOption(QUERIES);
        options.addOption(LOCATION);
        return options;
    }

    private static void runQueries(List<String> queries, Path location) {

    }

    /**
     * Returns the mapping from query name to query file path.
     */
    private static List<String> getQueries(Path queryLocation, String queries) {
        List<String> queryList = new ArrayList<>();
        if (queries.equals("all")) {
            File queriesDir = queryLocation.toFile();
            if (!queriesDir.exists()) {
                throw new IllegalArgumentException(
                        String.format("The queries dir \"%s\" does not exist.", queryLocation));
            }
            for (int i = 0; i < 12; i++) {
                String queryName = "q" + i;
                if (UNSUPPORTED_QUERIES.contains(queryName)) {
                    continue;
                }
                File queryFile = new File(queryLocation.toFile(), queryName + ".sql");
                if (queryFile.exists()) {
                    queryList.add(queryName);
                }
            }
        } else {
            for (String queryName : queries.split(",")) {
                if (UNSUPPORTED_QUERIES.contains(queryName)) {
                    continue;
                }
                File queryFile = new File(queryLocation.toFile(), queryName + ".sql");
                if (!queryFile.exists()) {
                    throw new IllegalArgumentException(
                            String.format("The query path \"%s\" does not exist.", queryFile.getAbsolutePath()));
                }
                queryList.add(queryName);
            }
        }
        return queryList;
    }
}
