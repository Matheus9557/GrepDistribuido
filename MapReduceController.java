import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

public class MapReduceController {
    private final ExecutorService executor;
    private final List<Map<String, List<String>>> intermediateResults;
    private final Pattern pattern;

    public MapReduceController(int numThreads, String regexPattern) {
        this.executor = Executors.newFixedThreadPool(numThreads);
        this.intermediateResults = new ArrayList<>();
        // Compilação da expressão regular
        this.pattern = Pattern.compile(regexPattern);
    }

    // Função Map
    public Callable<Map<String, List<String>>> mapTask(String fileName) {
        return () -> {
            Map<String, List<String>> wordCounts = new HashMap<>();
            try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    // Verificando se a linha contém o padrão "foo"
                    if (pattern.matcher(line).find()) {
                        wordCounts.computeIfAbsent(line, k -> new ArrayList<>()).add("1");
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return wordCounts;
        };
    }

    // Coleta os resultados
    public Map<String, List<String>> collectResults(List<Future<Map<String, List<String>>>> futures) throws Exception {
        Map<String, List<String>> aggregatedResults = new HashMap<>();
        for (Future<Map<String, List<String>>> future : futures) {
            Map<String, List<String>> result = future.get();
            for (Map.Entry<String, List<String>> entry : result.entrySet()) {
                aggregatedResults.merge(entry.getKey(), entry.getValue(), (v1, v2) -> {
                    v1.addAll(v2);
                    return v1;
                });
            }
        }
        return aggregatedResults;
    }

    // Função Reduce
    public void reduce(String word, List<String> occurrences) {
        System.out.println(word); // Exibe a linha que contém o padrão
    }

    // Função principal para executar o MapReduce
    public void execute(String[] files) throws Exception {
        List<Future<Map<String, List<String>>>> futures = new ArrayList<>();
        for (String file : files) {
            futures.add(executor.submit(mapTask(file)));
        }

        Map<String, List<String>> finalResults = collectResults(futures);
        executor.shutdown();

        // Processamento final na função Reduce
        for (Map.Entry<String, List<String>> entry : finalResults.entrySet()) {
            reduce(entry.getKey(), entry.getValue());
        }
    }

    public static void main(String[] args) throws Exception {
        // Recebe o padrão passado pelo usuário
        String regexPattern = args[0];

        // Arquivos a serem processados
        String[] files = {"file0.txt", "file1.txt"}; // Substitua com os nomes dos arquivos reais

        // Instancia o MapReduce com 4 threads e o padrão informado
        MapReduceController mapReduceController = new MapReduceController(4, regexPattern);
        mapReduceController.execute(files);
    }
}
