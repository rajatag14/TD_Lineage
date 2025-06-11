import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.*;
import java.io.*;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

// Custom Error Listener Class
class SQLErrorListener extends BaseErrorListener {
    private boolean hasErrors = false;
    private List<String> errorMessages = new ArrayList<>();
    
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                           int line, int charPositionInLine, String msg, RecognitionException e) {
        hasErrors = true;
        errorMessages.add(String.format("Line %d:%d - %s", line, charPositionInLine, msg));
    }
    
    public boolean hasErrors() {
        return hasErrors;
    }
    
    public List<String> getErrorMessages() {
        return errorMessages;
    }
    
    public void reset() {
        hasErrors = false;
        errorMessages.clear();
    }
}

// Query Validation Result Class
class ValidationResult {
    private boolean isValid;
    private List<String> errorMessages;
    
    public ValidationResult(boolean isValid, List<String> errorMessages) {
        this.isValid = isValid;
        this.errorMessages = errorMessages;
    }
    
    public boolean isValid() { return isValid; }
    public List<String> getErrorMessages() { return errorMessages; }
}

// File Processing Statistics Class
class FileStats {
    private String fileName;
    private int numberOfQueries;
    private int queriesWithError;
    private int corruptedQueries;
    private double percentageOfErrors;
    private long timeTaken;
    
    public FileStats(String fileName, int numberOfQueries, int queriesWithError, 
                    int corruptedQueries, double percentageOfErrors, long timeTaken) {
        this.fileName = fileName;
        this.numberOfQueries = numberOfQueries;
        this.queriesWithError = queriesWithError;
        this.corruptedQueries = corruptedQueries;
        this.percentageOfErrors = percentageOfErrors;
        this.timeTaken = timeTaken;
    }
    
    // Getters
    public String getFileName() { return fileName; }
    public int getNumberOfQueries() { return numberOfQueries; }
    public int getQueriesWithError() { return queriesWithError; }
    public int getCorruptedQueries() { return corruptedQueries; }
    public double getPercentageOfErrors() { return percentageOfErrors; }
    public long getTimeTaken() { return timeTaken; }
    
    public String toCsvRow() {
        return String.format("%s,%d,%d,%d,%.2f,%d", 
            fileName, numberOfQueries, queriesWithError, corruptedQueries, percentageOfErrors, timeTaken);
    }
}

// Failed Query Information Class
class FailedQuery {
    private String query;
    private List<String> errors;
    
    public FailedQuery(String query, List<String> errors) {
        this.query = query;
        this.errors = errors;
    }
    
    public String getQuery() { return query; }
    public List<String> getErrors() { return errors; }
}

public class SQLQueryValidator {
    
    /**
     * Validates a single SQL query using ANTLR grammar
     * Note: This is a placeholder for the actual ANTLR parser integration
     * You would need to replace this with your actual CommonLexerRules and TeraSql classes
     */
    public static ValidationResult validateQuery(String query, String startRule) {
        SQLErrorListener errorListener = new SQLErrorListener();
        
        try {
            // Create ANTLR input stream
            ANTLRInputStream input = new ANTLRInputStream(query);
            
            // Note: Replace 'CommonLexerRules' and 'TeraSql' with your actual generated classes
            // CommonLexerRules lexer = new CommonLexerRules(input);
            // CommonTokenStream tokens = new CommonTokenStream(lexer);
            // TeraSql parser = new TeraSql(tokens);
            // parser.addErrorListener(errorListener);
            
            // For demonstration, using a simple parser (replace with your actual parser)
            // parser.parse(); // Replace with getattr(parser, startRule)() equivalent
            
            // Placeholder validation logic - replace with actual ANTLR parsing
            boolean isValid = !query.trim().isEmpty() && query.contains("SELECT") || query.contains("INSERT") 
                            || query.contains("UPDATE") || query.contains("CREATE") || query.contains("DELETE");
            
            if (!isValid) {
                errorListener.syntaxError(null, null, 1, 0, "Invalid SQL syntax", null);
            }
            
        } catch (Exception e) {
            errorListener.syntaxError(null, null, 1, 0, e.getMessage(), null);
        }
        
        return new ValidationResult(!errorListener.hasErrors(), errorListener.getErrorMessages());
    }
    
    /**
     * Simple query cleaning function (placeholder for actual cleaning logic)
     */
    public static String queryCleanig(String content) {
        // Placeholder for query cleaning logic
        return content.replaceAll("\\r\\n", "\n")
                     .replaceAll("\\r", "\n")
                     .trim();
    }
    
    /**
     * Process a single SQL file
     */
    public static FileStats processSqlFile(String filePath, String startRule, String outputDir, String csvFilePath) {
        long startTime = System.currentTimeMillis();
        
        try {
            // Extract file name without extension
            Path path = Paths.get(filePath);
            String baseName = path.getFileName().toString();
            String fileNameNoExt = baseName.substring(0, baseName.lastIndexOf('.'));
            String errorFilePath = Paths.get(outputDir, fileNameNoExt + "_errors.txt").toString();
            
            // Read file content
            String content = Files.readString(path);
            content = queryCleanig(content);
            
            // Split queries by semicolon
            List<String> queries = Arrays.stream(content.split(";"))
                                        .map(String::trim)
                                        .filter(q -> !q.isEmpty())
                                        .collect(Collectors.toList());
            
            int successfulQueries = 0;
            int failedQueries = 0;
            int corruptedQueries = 0;
            List<FailedQuery> failedQueryList = new ArrayList<>();
            
            // Validate each query
            for (int i = 0; i < queries.size(); i++) {
                try {
                    String queryWithNewline = queries.get(i) + "\n";
                    ValidationResult result = validateQuery(queryWithNewline, startRule);
                    
                    if (result.isValid()) {
                        successfulQueries++;
                    } else {
                        failedQueries++;
                        failedQueryList.add(new FailedQuery(queryWithNewline, result.getErrorMessages()));
                    }
                } catch (Exception e) {
                    corruptedQueries++;
                }
            }
            
            // Write failed queries to error file
            if (!failedQueryList.isEmpty()) {
                try (PrintWriter writer = new PrintWriter(new FileWriter(errorFilePath))) {
                    for (FailedQuery failedQuery : failedQueryList) {
                        try {
                            writer.println("--- INVALID QUERY ---");
                            writer.println(failedQuery.getQuery());
                            writer.println();
                            writer.println("+ - ".repeat(50));
                            writer.println();
                        } catch (Exception e) {
                            System.out.println("Could not write query: " + failedQuery.getQuery());
                        }
                    }
                }
            }
            
            // Write errors to CSV
            List<String> errorMsgList = failedQueryList.stream()
                                                     .map(fq -> fq.getErrors().isEmpty() ? "" : 
                                                          fq.getErrors().get(0).split("-")[0].trim())
                                                     .collect(Collectors.toList());
            
            if (!errorMsgList.isEmpty()) {
                String errorsFilePath = Paths.get(outputDir, "errors.csv").toString();
                try (PrintWriter writer = new PrintWriter(new FileWriter(errorsFilePath, true))) {
                    for (String error : errorMsgList) {
                        writer.println(error + "," + fileNameNoExt);
                    }
                }
            }
            
            // Calculate statistics
            int totalQueries = queries.size();
            double errorPercentage = totalQueries > 0 ? (double) failedQueries / totalQueries * 100 : 0;
            long timeTaken = (System.currentTimeMillis() - startTime) / 1000; // Convert to seconds
            
            FileStats stats = new FileStats(baseName, totalQueries, failedQueries, 
                                          corruptedQueries, Math.round(errorPercentage * 100.0) / 100.0, timeTaken);
            
            System.out.printf("Queries: %d%n", stats.getNumberOfQueries());
            System.out.printf("Errors: %d (%.2f%%)%n", stats.getQueriesWithError(), stats.getPercentageOfErrors());
            
            // Append to CSV file
            try (PrintWriter writer = new PrintWriter(new FileWriter(csvFilePath, true))) {
                writer.println(stats.toCsvRow());
            }
            
            return stats;
            
        } catch (IOException e) {
            System.err.println("Error processing file " + filePath + ": " + e.getMessage());
            return new FileStats(Paths.get(filePath).getFileName().toString(), 0, 0, 1, 0, 
                               (System.currentTimeMillis() - startTime) / 1000); // Convert to seconds
        }
    }
    
    /**
     * Process all files in a directory sequentially
     */
    public static void processAllFiles(String inputDir, String outputDir, String csvFilePath) {
        try {
            // Create output directory
            Files.createDirectories(Paths.get(outputDir));
            
            String startRule = "parse";
            
            // Get all txt files
            List<String> txtFiles = Files.list(Paths.get(inputDir))
                                       .filter(path -> path.toString().endsWith(".txt"))
                                       .map(path -> path.getFileName().toString())
                                       .collect(Collectors.toList());
            
            System.out.println("Files: " + txtFiles);
            
            // Create CSV headers
            try (PrintWriter writer = new PrintWriter(new FileWriter(csvFilePath))) {
                writer.println("file_name,number_of_queries,queries_with_error,corrupted_queries,percentage_of_errors,time_taken_seconds");
            }
            
            // Create errors CSV
            String errorsFilePath = Paths.get(outputDir, "errors.csv").toString();
            try (PrintWriter writer = new PrintWriter(new FileWriter(errorsFilePath))) {
                writer.println("error,code");
            }
            
            // Process files sequentially
            for (String fileName : txtFiles) {
                String filePath = Paths.get(inputDir, fileName).toString();
                System.out.println("Processing file: " + fileName);
                processSqlFile(filePath, startRule, outputDir, csvFilePath);
            }
            
        } catch (IOException e) {
            System.err.println("Error setting up processing: " + e.getMessage());
        }
    }
    
    public static void main(String[] args) {
        // Configuration - set your paths here
        String outputFolder = "output_folder";
        
        // Input directory paths (uncomment the one you want to use)
        String inputDirectory = "C:\\Users\\";
        inputDirectory = "C:Users\\";
        String outputDirectory = "C:\\Users\\ + outputFolder + "\\";
        String csvFilePath = outputDirectory + "error_stats.csv";
        
        System.out.println("Starting SQL Query Validation...");
        System.out.println("Input Directory: " + inputDirectory);
        System.out.println("Output Directory: " + outputDirectory);
        System.out.println("Timestamp: " + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        
        // Process all files
        processAllFiles(inputDirectory, outputDirectory, csvFilePath);
        
        System.out.println("Processing completed!");
    }
}
