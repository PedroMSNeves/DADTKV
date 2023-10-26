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

        // Expected input: name, my_url, crash_ts, timeSlotDuration, other_id, other_url, ..., LM (delimiter), lm_id, lm_url, ..., SP (delimiter), sp_ts, sp_id, ..., CP (delimeter), cp_ts, cp_id, ...
        public static void Main(string[] args)
        {
            Dictionary<string, string> tm_urls = new Dictionary<string, string>();
            Dictionary<string, string> lm_urls = new Dictionary<string, string>();
            Dictionary<int, List<string>> suspected_processes = new Dictionary<int, List<string>>(); // <timeslot, process_ids>
            Dictionary<int, List<string>> crashed_processes = new Dictionary<int, List<string>>(); // <timeslot, process_ids>

            //Console.WriteLine("TM");
            //foreach (string s in args) Console.Write(s + " ");
            
            // Minimum is name, his own url, when to crash, the LM delimiter and 1 Lm url
            if (args.Length < 7) exitOnError("Invalid number of arguments");

            Uri my_url = new Uri(args[1]);
            ServerPort serverPort = new ServerPort(my_url.Host, my_url.Port, ServerCredentials.Insecure);

            int crash_ts = Int32.Parse(args[2]);

            bool lm = false;
            bool sp = false;
            bool cp = false;
            int currentTimeslot = -1;
            for (int i = 4; i < args.Length; i++)
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
                else if (args[i].Equals("CP"))
                {
                    sp = false;
                    cp = true;
                    currentTimeslot = -1;
                }
                else if (!lm && !sp && !cp)
                {
                    tm_urls.Add(args[i], args[i + 1]);
                    i++;
                }
                else if (lm && !sp && !cp)
                {
                    lm_urls.Add(args[i], args[i + 1]);
                    i++;
                }
                else if (sp && !cp) {
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
                else if (cp)
                {
                    if (args[i].All(char.IsDigit))
                    {
                        currentTimeslot = int.Parse(args[i]);
                        if (!crashed_processes.ContainsKey(currentTimeslot))
                        {
                            crashed_processes[currentTimeslot] = new List<string>();
                        }
                    }
                    else if (currentTimeslot != -1)
                    {
                        crashed_processes[currentTimeslot].Add(args[i]);
                    }
                }
            }
            Console.WriteLine("TM NAME: " + args[0] + " " + suspected_processes.Count);
            foreach (int epoch in suspected_processes.Keys)
            {
                Console.Write("EPOCH: " + epoch + " My suspicions: ");
                foreach (string name in suspected_processes[epoch])
                {
                    Console.Write(name + " ");
                }
                Console.WriteLine();
            }

            Console.WriteLine("TM");
            foreach (KeyValuePair<string, string> url in tm_urls) { Console.WriteLine(url.Value); }
            Console.WriteLine("LM");
            foreach (KeyValuePair<string, string> url in lm_urls) { Console.WriteLine(url.Value); }


            Console.WriteLine("CRASHES");
            foreach (int epoch in crashed_processes.Keys)
            {
                Console.Write(epoch + ": ");
                foreach (string name in crashed_processes[epoch]) Console.Write(name + " ");
                Console.WriteLine();
            }
            //Console.ReadKey();

            if (!lm && !sp && !cp) exitOnError("No LeaseManagers provided");

            // Store is shared by the various services
            Store st = new Store(args[0], int.Parse(args[3]) ,tm_urls.Values.ToList(), lm_urls.Values.ToList(), tm_urls.Keys.ToList(), lm_urls.Keys.ToList());
            Server server = new Server
            {
                Services = { TmService.BindService(new ServerService(st, args[0])),
                            BroadCastService.BindService(new BroadService(st)),
                            LeaseService.BindService(new LService(st, lm_urls.Values.ToList().Count)) },
                Ports = { serverPort }
            };

            server.Start();

            Console.WriteLine("Insecure server listening on host " + my_url.Host + " port " + my_url.Port);
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            // Creates the cycle for the main thread, the thread that executes the transactions
            MainThread mt = new MainThread(st, crash_ts, crashed_processes, suspected_processes);
            mt.cycle();
        }
    }
}