package src;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.*;

import gen.CommonLexerRules;
import gen.TeraSql;

import java.nio.file.Paths;
import java.nio.file.Files;
import java.io.IOException;

import java.util.ArrayList;
import java.util.*;
import java.nio.file.*;
import java.util.concurrent.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class runtime_measure {

    // Inner class to represent tree nodes for JSON serialization
    public static class TreeNode {
        public String nodeType;
        public String text;
        public List<TreeNode> children;
        public int startIndex;
        public int stopIndex;
        
        public TreeNode(String nodeType, String text, int startIndex, int stopIndex) {
            this.nodeType = nodeType;
            this.text = text;
            this.children = new ArrayList<>();
            this.startIndex = startIndex;
            this.stopIndex = stopIndex;
        }
    }

    // Inner class to represent token information for JSON serialization
    public static class TokenInfo {
        public int index;
        public int type;
        public String typeName;
        public String text;
        public int startIndex;
        public int stopIndex;
        public int line;
        public int column;
        public int channel;
        
        public TokenInfo(int index, int type, String typeName, String text, 
                        int startIndex, int stopIndex, int line, int column, int channel) {
            this.index = index;
            this.type = type;
            this.typeName = typeName;
            this.text = text;
            this.startIndex = startIndex;
            this.stopIndex = stopIndex;
            this.line = line;
            this.column = column;
            this.channel = channel;
        }
    }

    // Inner class to represent metadata
    public static class Metadata {
        public String filename;
        public int totalTokens;
        public String processingTime;
        public String status; // "SUCCESS", "TIMEOUT", "PARTIAL"
        public List<String> failedQueries; // For partial processing
        
        public Metadata(String filename, int totalTokens, String processingTime, String status) {
            this.filename = filename;
            this.totalTokens = totalTokens;
            this.processingTime = processingTime;
            this.status = status;
            this.failedQueries = new ArrayList<>();
        }
    }

    // Root class to hold complete JSON structure
    public static class ParseResult {
        // public Metadata metadata;
        // public List<TokenInfo> tokenStream;
        public TreeNode parseTree;
        
        // For internal tracking (not serialized to JSON)
        public transient Metadata metadata;
        public transient List<TokenInfo> tokenStream;
        
        public ParseResult(Metadata metadata, List<TokenInfo> tokenStream, TreeNode parseTree) {
            this.metadata = metadata;
            this.tokenStream = tokenStream;
            this.parseTree = parseTree;
        }
    }

    // Timeout constants
    private static final int FILE_TIMEOUT_SECONDS = 180;
    private static final int QUERY_TIMEOUT_SECONDS = 30;
    
    // Logging lists
    private static List<String> timeoutFiles = new ArrayList<>();
    private static List<String> timeoutQueries = new ArrayList<>();

    public static void main(String[] args) throws IOException {

        long startTime = System.nanoTime();

        Path folderPath = Paths.get("C:\\Project_Work\\Lineage\\work\\dbql_extraction\\insert_t");
        Path csvOutput = Paths.get("C:\\Project_Work\\Lineage\\work\\dbql_extraction\\insert_te");
        Path jsonOutputFolder = Paths.get("C:\\Project_Work\\Lineage\\work\\dbql_extraction\\json_trees");
        Path logOutput = Paths.get("C:\\Project_Work\\Lineage\\work\\dbql_extraction\\timeout_log.txt");
        
        // Create JSON output directory if it doesn't exist
        if (!Files.exists(jsonOutputFolder)) {
            Files.createDirectories(jsonOutputFolder);
        }

        List<String> lines = new ArrayList<>();
        lines.add("FileName,DurationSeconds,JsonFile,Status");

        // Create Gson instance with pretty printing
        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(folderPath, "*.txt")) {

            for (Path file : stream) {

                String filename = file.getFileName().toString();
                System.out.println("Processing: " + filename);

                long fileStartTime = System.nanoTime();
                String input = Files.readString(file);

                // Try to process the entire file with timeout
                ParseResult result = processFileWithTimeout(input, filename, gson);
                
                long endTime = System.nanoTime();
                long durationInSec = (endTime - fileStartTime) / 1_000_000_000;

                if (result != null) {
                    // File processed successfully
                    result.metadata.processingTime = durationInSec + "s";
                    
                    // Save JSON to file
                    String jsonFilename = filename.replace(".txt", ".json");
                    Path jsonFilePath = jsonOutputFolder.resolve(jsonFilename);
                    String jsonString = gson.toJson(result);
                    Files.writeString(jsonFilePath, jsonString);

                    System.out.println("JSON saved to: " + jsonFilePath);
                    System.out.println("Total tokens: " + result.tokenStream.size());
                    System.out.println(filename + " took " + durationInSec + "s - Status: " + result.metadata.status);

                    lines.add(filename + "," + durationInSec + "," + jsonFilename + "," + result.metadata.status);
                } else {
                    // File timed out, try query-by-query processing
                    System.out.println(filename + " timed out after " + FILE_TIMEOUT_SECONDS + "s. Attempting query-by-query processing...");
                    timeoutFiles.add(filename);
                    
                    ParseResult partialResult = processFileByQueries(input, filename, gson);
                    
                    endTime = System.nanoTime();
                    durationInSec = (endTime - fileStartTime) / 1_000_000_000;
                    
                    if (partialResult != null) {
                        partialResult.metadata.processingTime = durationInSec + "s";
                        
                        // Save partial JSON to file
                        String jsonFilename = filename.replace(".txt", "_partial.json");
                        Path jsonFilePath = jsonOutputFolder.resolve(jsonFilename);
                        String jsonString = gson.toJson(partialResult);
                        Files.writeString(jsonFilePath, jsonString);

                        System.out.println("Partial JSON saved to: " + jsonFilePath);
                        System.out.println(filename + " partial processing took " + durationInSec + "s - Status: " + partialResult.metadata.status);

                        lines.add(filename + "," + durationInSec + "," + jsonFilename + "," + partialResult.metadata.status);
                    } else {
                        System.out.println(filename + " completely failed processing");
                        lines.add(filename + "," + durationInSec + ",FAILED,COMPLETE_FAILURE");
                    }
                }
            }
        }

        // Write CSV results
        Files.write(csvOutput, lines);
        
        // Write timeout log
        writeTimeoutLog(logOutput);
        
        long totalTime = System.nanoTime() - startTime;
        System.out.println("Total processing time: " + (totalTime / 1_000_000_000) + "s");
        System.out.println("Timeout files: " + timeoutFiles.size());
        System.out.println("Timeout queries: " + timeoutQueries.size());
    }

    /**
     * Process file with timeout
     */
    private static ParseResult processFileWithTimeout(String input, String filename, Gson gson) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        
        try {
            Future<ParseResult> future = executor.submit(() -> {
                try {
                    return processFullFile(input, filename);
                } catch (Exception e) {
                    System.err.println("Error processing file " + filename + ": " + e.getMessage());
                    return null;
                }
            });
            
            return future.get(FILE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            
        } catch (TimeoutException e) {
            System.out.println("File " + filename + " timed out after " + FILE_TIMEOUT_SECONDS + " seconds");
            return null;
        } catch (Exception e) {
            System.err.println("Error processing file " + filename + ": " + e.getMessage());
            return null;
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Process file by splitting into individual queries
     */
    private static ParseResult processFileByQueries(String input, String filename, Gson gson) {
        // Split input by semicolon
        String[] queries = input.split(";");
        
        List<TokenInfo> allTokens = new ArrayList<>();
        List<TreeNode> successfulTrees = new ArrayList<>();
        List<String> failedQueries = new ArrayList<>();
        
        int totalQueries = queries.length;
        int successfulQueries = 0;
        
        System.out.println("Processing " + totalQueries + " queries from " + filename);
        
        for (int i = 0; i < queries.length; i++) {
            String query = queries[i].trim();
            if (query.isEmpty()) continue;
            
            String queryIdentifier = filename + "_query_" + (i + 1);
            System.out.println("Processing query " + (i + 1) + "/" + totalQueries);
            
            ParseResult queryResult = processQueryWithTimeout(query, queryIdentifier);
            
            if (queryResult != null && queryResult.metadata.status.equals("SUCCESS")) {
                // Add tokens and tree from successful query
                allTokens.addAll(queryResult.tokenStream);
                if (queryResult.parseTree != null) {
                    successfulTrees.add(queryResult.parseTree);
                }
                successfulQueries++;
            } else {
                // Log failed query
                String failedQuery = queryIdentifier + ": " + query.substring(0, Math.min(query.length(), 100)) + "...";
                failedQueries.add(failedQuery);
                timeoutQueries.add(failedQuery);
                System.out.println("Query " + (i + 1) + " failed or timed out");
            }
        }
        
        if (successfulQueries > 0) {
            // Create a combined tree node for all successful queries
            TreeNode combinedTree = new TreeNode("CombinedQueries", "Multiple queries", 0, input.length());
            combinedTree.children.addAll(successfulTrees);
            
            Metadata metadata = new Metadata(filename, allTokens.size(), "0s", "PARTIAL");
            metadata.failedQueries = failedQueries;
            
            System.out.println("Successfully processed " + successfulQueries + "/" + totalQueries + " queries");
            
            return new ParseResult(metadata, allTokens, combinedTree);
        }
        
        return null;
    }

    /**
     * Process a single query with timeout
     */
    private static ParseResult processQueryWithTimeout(String query, String queryIdentifier) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        
        try {
            Future<ParseResult> future = executor.submit(() -> {
                try {
                    return processFullFile(query, queryIdentifier);
                } catch (Exception e) {
                    System.err.println("Error processing query " + queryIdentifier + ": " + e.getMessage());
                    return null;
                }
            });
            
            return future.get(QUERY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            
        } catch (TimeoutException e) {
            System.out.println("Query " + queryIdentifier + " timed out after " + QUERY_TIMEOUT_SECONDS + " seconds");
            return null;
        } catch (Exception e) {
            System.err.println("Error processing query " + queryIdentifier + ": " + e.getMessage());
            return null;
        } finally {
            executor.shutdownNow();
        }
    }

    /**
     * Process full file or query (original processing logic)
     */
    private static ParseResult processFullFile(String input, String identifier) {
        // Create lexer and token stream
        CommonLexerRules lexer = new CommonLexerRules(CharStreams.fromString(input));
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        
        // Fill the token stream - this ensures all tokens are loaded
        tokens.fill();
        
        // Extract all tokens before parsing
        List<TokenInfo> tokenStream = extractTokenStream(tokens, lexer);
        
        // Create parser and parse
        TeraSql parser = new TeraSql(tokens);
        ParseTree tree = parser.parse();

        // Convert parse tree to JSON
        TreeNode jsonTree = convertParseTreeToJson(tree, parser);
        
        // Create metadata
        Metadata metadata = new Metadata(identifier, tokenStream.size(), "0s", "SUCCESS");
        
        // Create complete parse result
        return new ParseResult(metadata, tokenStream, jsonTree);
    }

    /**
     * Write timeout log to file
     */
    private static void writeTimeoutLog(Path logOutput) throws IOException {
        List<String> logLines = new ArrayList<>();
        
        logLines.add("=== TIMEOUT LOG ===");
        logLines.add("Generated: " + new Date());
        logLines.add("");
        
        logLines.add("FILES THAT TIMED OUT (" + timeoutFiles.size() + "):");
        for (String file : timeoutFiles) {
            logLines.add("- " + file);
        }
        logLines.add("");
        
        logLines.add("QUERIES THAT TIMED OUT (" + timeoutQueries.size() + "):");
        for (String query : timeoutQueries) {
            logLines.add("- " + query);
        }
        
        Files.write(logOutput, logLines);
        System.out.println("Timeout log written to: " + logOutput);
    }

    /**
     * Extracts all tokens from CommonTokenStream and converts them to TokenInfo objects
     */
    private static List<TokenInfo> extractTokenStream(CommonTokenStream tokens, Lexer lexer) {
        List<TokenInfo> tokenStream = new ArrayList<>();
        
        // Get all tokens from the stream
        List<Token> allTokens = tokens.getTokens();
        
        for (int i = 0; i < allTokens.size(); i++) {
            Token token = allTokens.get(i);
            
            // Get token type name from vocabulary
            String typeName = lexer.getVocabulary().getDisplayName(token.getType());
            
            // Handle special cases for token type names
            if (typeName.startsWith("'") && typeName.endsWith("'")) {
                // Remove quotes from literal tokens like 'SELECT' -> SELECT
                typeName = typeName.substring(1, typeName.length() - 1);
            }
            
            TokenInfo tokenInfo = new TokenInfo(
                i,                                    // token index in stream
                token.getType(),                      // token type ID
                typeName,                            // token type name
                token.getText() != null ? token.getText() : "", // token text
                token.getStartIndex(),               // start character position
                token.getStopIndex(),                // stop character position
                token.getLine(),                     // line number
                token.getCharPositionInLine(),       // column position
                token.getChannel()                   // token channel (usually 0 for main channel)
            );
            
            tokenStream.add(tokenInfo);
        }
        
        return tokenStream;
    }

    /**
     * Converts ANTLR ParseTree to a custom TreeNode structure for JSON serialization
     */
    private static TreeNode convertParseTreeToJson(ParseTree tree, Parser parser) {
        if (tree instanceof ParserRuleContext) {
            ParserRuleContext ruleContext = (ParserRuleContext) tree;
            String ruleName = parser.getRuleNames()[ruleContext.getRuleIndex()];
            
            TreeNode node = new TreeNode(
                "RuleContext: " + ruleName,
                tree.getText(),
                ruleContext.getStart() != null ? ruleContext.getStart().getStartIndex() : -1,
                ruleContext.getStop() != null ? ruleContext.getStop().getStopIndex() : -1
            );

            // Add children recursively
            for (int i = 0; i < tree.getChildCount(); i++) {
                TreeNode childNode = convertParseTreeToJson(tree.getChild(i), parser);
                node.children.add(childNode);
            }

            return node;
            
        } else if (tree instanceof TerminalNode) {
            TerminalNode terminalNode = (TerminalNode) tree;
            Token token = terminalNode.getSymbol();
            
            return new TreeNode(
                "Terminal: " + parser.getVocabulary().getDisplayName(token.getType()),
                tree.getText(),
                token.getStartIndex(),
                token.getStopIndex()
            );
        } else {
            // Handle other node types
            TreeNode node = new TreeNode(
                "Unknown: " + tree.getClass().getSimpleName(),
                tree.getText(),
                -1,
                -1
            );

            for (int i = 0; i < tree.getChildCount(); i++) {
                TreeNode childNode = convertParseTreeToJson(tree.getChild(i), parser);
                node.children.add(childNode);
            }

            return node;
        }
    }
}
