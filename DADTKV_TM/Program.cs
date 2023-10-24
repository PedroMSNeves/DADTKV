using DADTKV_TM.Impls;
using Grpc.Core;
using System;

namespace DADTKV_TM
{
    class Program
    {
        public static void exitOnError(string message)
        {
            Console.WriteLine($"Error: {message}.\nPress any key to close.");
            Console.ReadKey();
            Environment.Exit(0);
        }

        // Expected input: name, my_url, crash_ts, other_url, ..., LM (delimiter), lm_url, ..., SP (delimiter), sp_ts, sp_id, ...
        public static void Main(string[] args)
        {
            List<string> tm_urls = new List<string>();
            List<string> lm_urls = new List<string>();
            Dictionary<int, List<string>> suspected_processes = new Dictionary<int, List<string>>(); // <timeslot, process_ids>

            //Console.WriteLine("TM");
            //foreach (string s in args) Console.Write(s + " ");

            // Minimum is name, his own url, when to crash, the LM delimiter and 1 Lm url
            if (args.Length < 5) exitOnError("Invalid number of arguments");

            Uri my_url = new Uri(args[1]);;
            ServerPort serverPort = new ServerPort(my_url.Host, my_url.Port, ServerCredentials.Insecure);

            int crash_ts = Int32.Parse(args[2]);

            bool lm = false;
            bool sp = false;
            int currentTimeslot = -1;
            for (int i = 3; i < args.Length; i++)
            {
                if (args[i].Equals("LM"))
                {
                    lm = true;                   
                }
                else if (args[i].Equals("SP"))
                {
                    lm = false;
                    sp = true;
                }
                else if (!lm && !sp)
                {
                    tm_urls.Add(args[i]);
                }
                else if (lm && !sp)
                {
                    lm_urls.Add(args[i]);
                }
                else if (sp) {
                    if (args[i].All(char.IsDigit))
                    {
                        currentTimeslot = int.Parse(args[i]);
                        if (!suspected_processes.ContainsKey(currentTimeslot))
                        {
                            suspected_processes[currentTimeslot] = new List<string>();
                        }
                    }
                    else if (currentTimeslot != -1)
                    {
                        suspected_processes[currentTimeslot].Add(args[i]);
                    }
                }
            }

            Console.WriteLine("TM");
            foreach (string url in tm_urls) { Console.WriteLine(url); }
            Console.WriteLine("LM");
            foreach (string url in lm_urls) { Console.WriteLine(url); }

            if (!lm) exitOnError("No LeaseManagers provided");

            // Store is shared by the various services
            Store st = new Store(args[0], tm_urls, lm_urls);
            Server server = new Server
            {
                Services = { TmService.BindService(new ServerService(st, args[0])),
                            BroadCastService.BindService(new BroadService(st)),
                            LeaseService.BindService(new LService(st, lm_urls.Count)) },
                Ports = { serverPort }
            };

            server.Start();

            Console.WriteLine("Insecure server listening on host " + my_url.Host + " port " + my_url.Port);
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            // Creates the cycle for the main thread, the thread that executes the transactions
            MainThread mt = new MainThread(st);
            mt.cycle();
        }
    }
}