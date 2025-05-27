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

                CommonLexerRules lexer = new CommonLexerRules(CharStreams.fromString(input));
                CommonTokenStream tokens = new CommonTokenStream(lexer);
                TeraSql parser = new TeraSql(tokens);
                ParseTree tree = parser.parse();

                long endTime = System.nanoTime();
                long durationInSec = (endTime - fileStartTime) / 1_000_000_000;

                System.out.println(filename + " took " + durationInSec + "s");

                // Convert parse tree to JSON
                TreeNode jsonTree = convertParseTreeToJson(tree, parser);
                String jsonString = gson.toJson(jsonTree);

                // Save JSON to file
                String jsonFilename = filename.replace(".txt", ".json");
                Path jsonFilePath = jsonOutputFolder.resolve(jsonFilename);
                Files.writeString(jsonFilePath, jsonString);

                System.out.println("JSON saved to: " + jsonFilePath);

                lines.add(filename + "," + durationInSec + "," + jsonFilename);
            }
        }

        Files.write(csvOutput, lines);
        
        long totalTime = System.nanoTime() - startTime;
        System.out.println("Total processing time: " + (totalTime / 1_000_000_000) + "s");
    }

    /**
     * Converts ANTLR ParseTree to a custom TreeNode structure for JSON serialization
     */
    private static TreeNode convertParseTreeToJson(ParseTree tree, Parser parser) {
        if (tree instanceof RuleContext) {
            RuleContext ruleContext = (RuleContext) tree;
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

    /**
     * Alternative method that creates a more detailed JSON structure
     * including token information and parse tree metadata
     */
    private static Map<String, Object> convertParseTreeToDetailedJson(ParseTree tree, Parser parser) {
        Map<String, Object> node = new HashMap<>();
        
        if (tree instanceof RuleContext) {
            RuleContext ruleContext = (RuleContext) tree;
            String ruleName = parser.getRuleNames()[ruleContext.getRuleIndex()];
            
            node.put("type", "rule");
            node.put("ruleName", ruleName);
            node.put("ruleIndex", ruleContext.getRuleIndex());
            node.put("text", tree.getText());
            
            if (ruleContext.getStart() != null) {
                node.put("startToken", tokenToMap(ruleContext.getStart()));
            }
            if (ruleContext.getStop() != null) {
                node.put("stopToken", tokenToMap(ruleContext.getStop()));
            }
            
            List<Map<String, Object>> children = new ArrayList<>();
            for (int i = 0; i < tree.getChildCount(); i++) {
                children.add(convertParseTreeToDetailedJson(tree.getChild(i), parser));
            }
            node.put("children", children);
            
        } else if (tree instanceof TerminalNode) {
            TerminalNode terminalNode = (TerminalNode) tree;
            Token token = terminalNode.getSymbol();
            
            node.put("type", "terminal");
            node.put("tokenType", parser.getVocabulary().getDisplayName(token.getType()));
            node.put("tokenIndex", token.getTokenIndex());
            node.put("text", tree.getText());
            node.put("token", tokenToMap(token));
        }
        
        return node;
    }

    /**
     * Helper method to convert Token to Map for JSON serialization
     */
    private static Map<String, Object> tokenToMap(Token token) {
        Map<String, Object> tokenMap = new HashMap<>();
        tokenMap.put("type", token.getType());
        tokenMap.put("text", token.getText());
        tokenMap.put("line", token.getLine());
        tokenMap.put("column", token.getCharPositionInLine());
        tokenMap.put("startIndex", token.getStartIndex());
        tokenMap.put("stopIndex", token.getStopIndex());
        tokenMap.put("tokenIndex", token.getTokenIndex());
        return tokenMap;
    }
              }
