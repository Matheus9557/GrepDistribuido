import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class GrepMapReduceController {
    private final ExecutorService executor;
    private final String pattern;

    public GrepMapReduceController(int numThreads, String pattern) {
        this.executor = Executors.newFixedThreadPool(numThreads);
        this.pattern = pattern;
    }

    // Função Map que lê um arquivo e busca as linhas que correspondem ao padrão
    public Callable<List<String>> mapTask(String fileName) {
        return () -> {
            List<String> matchingLines = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
                String line;
                // Usando a expressão regular .*padrão.* para encontrar o padrão em qualquer lugar da linha
                while ((line = reader.readLine()) != null) {
                    if (line.matches(".*" + pattern + ".*")) {
                        matchingLines.add(line);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return matchingLines;
        };
    }

    // Coleta resultados de todas as threads após o Map
    public List<String> collectResults(List<Future<List<String>>> futures) throws Exception {
        List<String> allMatchingLines = new ArrayList<>();
        for (Future<List<String>> future : futures) {
            allMatchingLines.addAll(future.get());
        }
        return allMatchingLines;
    }

    // Executa o MapReduce
    public void execute(String[] files) throws Exception {
        List<Future<List<String>>> futures = new ArrayList<>();
        for (String file : files) {
            futures.add(executor.submit(mapTask(file)));
        }

        List<String> finalResults = collectResults(futures);
        executor.shutdown();

        // Exibe as linhas que correspondem ao padrão
        for (String line : finalResults) {
            System.out.println(line);
        }
    }
}
