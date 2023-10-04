using Grpc.Core;
using Grpc.Net.Client;
using System;


namespace DADTKV_TM
{

    class Program
    {
        // Receives input like "Tm1 myurl othertmurl1 othertmurl2 LM lmurl1 lmurl2" (name,hisurl,otherurl...,LM(delimiter),lmurl...)
        private static void getUrls(string[] args,ref List<string> tm_urls, ref List<string> lm_urls)
        {
            if (args.Length < 4) // Minimum is name, his own url the LM delimiter and 1 Lm url
            {
                Console.WriteLine("ERROR: Invalid number of args");
                Console.WriteLine("Press any key to close");
                Console.ReadKey();
                Environment.Exit(0);
            }
            bool lm = false;
            for (int i = 2; i < args.Length; i++)
            {
                if (args[i].Equals("LM")) lm = true;
                else if(!lm) tm_urls.Add(args[i]);
                else lm_urls.Add(args[i]);
            }
            if(!lm) 
            {
                Console.WriteLine("ERROR: Invalid number of args. No LeaseManagers provided");
                Console.WriteLine("Press any key to close");
                Console.ReadKey();
                Environment.Exit(0);
            }
            
        }


        public static void Main(string[] args)
        {
            List<string> tm_urls = new List<string>();
            List<string> lm_urls = new List<string>();
            Uri url;
            getUrls(args, ref tm_urls, ref lm_urls); // gets all url servers minus his
            try
            {
                url = new Uri(args[1]); // gets his url 
            }
            catch (System.UriFormatException ex)
            {
                Console.WriteLine("ERROR: Invalid url (self)");
                Console.WriteLine("Press any key to close");
                Console.ReadKey();
                return;
            }
            ServerPort serverPort = new ServerPort(url.Host, url.Port, ServerCredentials.Insecure);

            // Store is shared by the various services
            Store st = new Store(args[0]); 
            Server server = new Server
            {
                Services = { TmService.BindService(new ServerService(st, args[0], tm_urls, lm_urls)),
                            BroadCastService.BindService(new BroadService(st)),
                            LeaseService.BindService(new LService(st)) },
                Ports = { serverPort }
            };

            server.Start();

            Console.WriteLine("Insecure server listening on host " + url.Host + " port " + url.Port);
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            // Creates the cycle for the main thread, the thread that executes the transactions
            MainThread mt = new MainThread(st);
            mt.cycle();
        }
    }
}