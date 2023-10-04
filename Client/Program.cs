namespace Client
{

    class Program
    { /* Input: "Name tmurl1 tmurl2 ...*/
        private static ClientLogic args_prep(string[] args)
        {
            ClientLogic clientLogic;
            if (args.Length == 0 || args.Length == 1)
            {
                Console.WriteLine("ERROR: Invalid number of args");
                Console.WriteLine("Press any key to close");
                Console.ReadKey();
                Environment.Exit(0);
            }
            clientLogic = new ClientLogic(args[0]);

            for (int i = 1; i < args.Length; i++) clientLogic.AddServer(args[i]);

            return clientLogic;
        }

        public static void Main(string[] args)
        {
            ClientLogic clientLogic = args_prep(args);

            while (true)
            {
                char key = Console.ReadKey().KeyChar;
                switch (key)
                {
                    case 'T':
                        clientLogic.Transaction();
                        break;
                    case 'W':
                        clientLogic.Sleep(); // verificar read dentro da funcao 
                        break;
                    case 'S':
                        clientLogic.Status(); // vai buscar o estado de cada server
                        /* Perguntar se temos de meter o estado te todos os clients,tm e lm ou se chega apenas dos tm
                         * Possivelmente podemos meter cada tm a dizer o que pensa dos outros tm e lm (de quem suspeita)
                         */
                        break;
                    case '#':
                        while (Console.ReadKey().KeyChar != '\n') ; // Reads and ignores line
                        break;
                    case ' ':
                        break; // It will read the next char
                }
            }
        }
    }
}