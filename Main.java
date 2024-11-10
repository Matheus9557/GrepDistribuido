public class Main {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Por favor, informe um padrão de busca.");
            System.exit(1);
        }

        // Padrão passado pelo usuário como argumento
        String pattern = args[0];

        try {
            // Gera arquivos de teste
            FileGenerator generator = new FileGenerator(2, 50, "abcdefghijklmnopqrstuvwxyz".toCharArray(), 3, 8);
            generator.generateFiles();

            // Executa o Grep MapReduce com 3 threads e o padrão fornecido
            GrepMapReduceController controller = new GrepMapReduceController(3, pattern);
            controller.execute(new String[]{"file_0.txt", "file_1.txt"});
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
