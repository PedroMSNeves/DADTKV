
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
        public static void Main(string[] args)
        {
            ClientLogic clientLogic = new ClientLogic(args[1]);

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