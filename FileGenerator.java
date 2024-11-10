import java.io.*;
import java.util.*;

public class FileGenerator {
    private int split;
    private int numWords;
    private char[] alphabet;
    private int minSize;
    private int maxSize;

    public FileGenerator(int split, int numWords, char[] alphabet, int minSize, int maxSize) {
        this.split = split;
        this.numWords = numWords;
        this.alphabet = alphabet;
        this.minSize = minSize;
        this.maxSize = maxSize;
    }

    // Gera uma palavra aleatória com base nos parâmetros fornecidos
    private String generateRandomWord() {
        Random random = new Random();
        int wordSize = random.nextInt((maxSize - minSize) + 1) + minSize;
        StringBuilder word = new StringBuilder(wordSize);
        for (int i = 0; i < wordSize; i++) {
            word.append(alphabet[random.nextInt(alphabet.length)]);
        }
        return word.toString();
    }

    // Divide o texto gerado em vários arquivos
    public void generateFiles() throws IOException {
        int wordsPerFile = numWords / split;
        for (int i = 0; i < split; i++) {
            BufferedWriter writer = new BufferedWriter(new FileWriter("file_" + i + ".txt"));
            for (int j = 0; j < wordsPerFile; j++) {
                writer.write(generateRandomWord() + " ");
            }
            writer.close();
        }
    }
}
