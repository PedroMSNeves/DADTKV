namespace Client
{

    class Program
    { /* expected input: config_file name tmurl1 tmurl2 ... */
        private static void exitOnError(string message)
        {
            Console.WriteLine($"ERROR: {message}");
            Console.WriteLine("Press any key to close.");
            Console.ReadKey();
            Environment.Exit(-1);
        }

        private static ClientLogic argsPrep(string[] args)
        {
            if (args.Length < 3) exitOnError("Invalid number of arguments");
            ClientLogic clientLogic = new ClientLogic(args[1]);
            for (int i = 2; i < args.Length; i++) clientLogic.AddServer(args[i]);
            return clientLogic;
        }

        public static void Main(string[] args)
        {
                ClientLogic clientLogic = argsPrep(args);

            string[] lines = System.IO.File.ReadAllLines(args[0]);
            foreach (string line in lines)
            {
                string[] words = line.Split(' ');
                if (words.Length == 0) break; //cheganis ai fim
                if (words[0].Length != 1) exitOnError("Invalid command");
                
                char c = words[0][0];
                switch (c)
                {
                    case 'S':
                        clientLogic.Status(); // vai buscar o estado de cada server
                        /* Perguntar se temos de meter o estado te todos os clients,tm e lm ou se chega apenas dos tm
                         * Possivelmente podemos meter cada tm a dizer o que pensa dos outros tm e lm (de quem suspeita)
                         */
                        break;
                    case 'T':
                        if (words.Length < 3) exitOnError("Invalid transaction");

                        List<string> reads = new List<string>();
                        Dictionary<string, int> writes = new Dictionary<string, int>();

                        int i = 0;
                        string restOfLine = line.Substring(2);
                        restOfLine = Parse_Lib.Parse.ParseStringNoSpaces(restOfLine);
                        if (!Parse_Lib.Parse.ParseReads(restOfLine, ref i, out reads)) exitOnError("Bad read transaction format");
                        if (!Parse_Lib.Parse.ParseWrites(restOfLine, ref i, out writes)) exitOnError("Bad write transaction format");
                        clientLogic.Transaction(reads, writes);
                        break;
                    case 'W':
                        int miliseconds = Parse_Lib.Parse.ParseInt(words[1]);
                        if (miliseconds < 0) exitOnError("Invalid number of miliseconds");
                        clientLogic.Sleep(miliseconds);
                        break;
                    case '#':
                        continue;
                }
            }
            Console.WriteLine("Client ended");
            Console.WriteLine("Press any key to close.");
            Console.ReadKey();
        }
    }
}