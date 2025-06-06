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
        
        public Metadata(String filename, int totalTokens, String processingTime) {
            this.filename = filename;
            this.totalTokens = totalTokens;
            this.processingTime = processingTime;
        }
    }

    // Root class to hold complete JSON structure
    public static class ParseResult {
        public Metadata metadata;
        public List<TokenInfo> tokenStream;
        public TreeNode parseTree;
        
        public ParseResult(Metadata metadata, List<TokenInfo> tokenStream, TreeNode parseTree) {
            this.metadata = metadata;
            this.tokenStream = tokenStream;
            this.parseTree = parseTree;
        }
    }

    public static void main(String[] args) throws IOException {

        long startTime = System.nanoTime();

        Path folderPath = Paths.get("C:\\Project_Work\\Lineage\\work\\dbql_extraction\\insert_t");
        Path csvOutput = Paths.get("C:\\Project_Work\\Lineage\\work\\dbql_extraction\\insert_te");
        Path jsonOutputFolder = Paths.get("C:\\Project_Work\\Lineage\\work\\dbql_extraction\\json_trees");
        
        // Create JSON output directory if it doesn't exist
        if (!Files.exists(jsonOutputFolder)) {
            Files.createDirectories(jsonOutputFolder);
        }

        List<String> lines = new ArrayList<>();
        lines.add("FileName,DurationSeconds,JsonFile");

        // Create Gson instance with pretty printing
        Gson gson = new GsonBuilder().setPrettyPrinting().create();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(folderPath, "*.txt")) {

            for (Path file : stream) {

                String filename = file.getFileName().toString();
                System.out.println("Processing: " + filename);

                // Reset start time for each file
                long fileStartTime = System.nanoTime();

                String input = Files.readString(file);

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

                long endTime = System.nanoTime();
                long durationInSec = (endTime - fileStartTime) / 1_000_000_000;

                System.out.println(filename + " took " + durationInSec + "s");

                // Convert parse tree to JSON
                TreeNode jsonTree = convertParseTreeToJson(tree, parser);
                
                // Create metadata
                Metadata metadata = new Metadata(filename, tokenStream.size(), durationInSec + "s");
                
                // Create complete parse result
                ParseResult parseResult = new ParseResult(metadata, tokenStream, jsonTree);
                
                // Convert to JSON string
                String jsonString = gson.toJson(parseResult);

                // Save JSON to file
                String jsonFilename = filename.replace(".txt", ".json");
                Path jsonFilePath = jsonOutputFolder.resolve(jsonFilename);
                Files.writeString(jsonFilePath, jsonString);

                System.out.println("JSON saved to: " + jsonFilePath);
                System.out.println("Total tokens: " + tokenStream.size());

                lines.add(filename + "," + durationInSec + "," + jsonFilename);
            }
        }

        Files.write(csvOutput, lines);
        
        long totalTime = System.nanoTime() - startTime;
        System.out.println("Total processing time: " + (totalTime / 1_000_000_000) + "s");
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
