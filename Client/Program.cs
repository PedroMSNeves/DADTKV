
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Parse_Lib;

namespace Client
{

    class Program
    {
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
                    case '#':
                        while (Console.ReadKey().KeyChar != '\n') ; //ignora a linha
                        break;
                    case ' ':
                        break; // it will read the next char
                    //maybe add a comand that lists all DADINTs stored in the DADTKV lib of the client
                }
            }
        }
    }
}