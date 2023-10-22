using Grpc.Core;
using System;
using DADTKV_LM.Impls;

namespace DADTKV_LM
{
    class Program
    {
        // recieves input like "Lm1 myurl id numTimeSlots startingTime timeSlotDuration otherlmurl1 otherlmurl2 TM tmurl1 tmurl2" (name,hisurl,otherurl...,TM(delimiter),tmurl...)

        private static void getUrls(string[] args, ref List<string> tm_urls, ref List<string> lm_urls)
        {
            if (args.Length < 8) //minimum is name, his own url, id, numTimeSlots, startingTime, timeSlotDuration, the TM delimiter and 1 Tm url
            {
                Console.WriteLine("ERROR: Invalid number of args");
                Console.WriteLine("Press any key to close");
                Console.ReadKey();
                Environment.Exit(0);
            }
            bool tm = false;
            for (int i = 6; i < args.Length; i++)
            {
                // Console.WriteLine(tm + ": " + args[i]);
                if (args[i].Equals("TM")) tm = true;
                else if (!tm) lm_urls.Add(args[i]);
                else tm_urls.Add(args[i]);
            }
            if (!tm)
            {
                Console.WriteLine("LM Error: Invalid number of args. No TransactionManagers provided.");
                Console.WriteLine("Press any key to close");
                Console.ReadKey();
                Environment.Exit(0);
            }
        }

        public static void Main(string[] args)
        {
            List<string> tm_urls = new List<string>();
            List<string> lm_urls = new List<string>();
            Console.WriteLine("LM");
            foreach (string s in args) Console.Write(s + " ");
            getUrls(args, ref tm_urls, ref lm_urls); // gets all server urls except his own
            Uri url = new Uri(args[1]); // gets his url 

            ServerPort serverPort = new ServerPort(url.Host, url.Port, ServerCredentials.Insecure);

            if (!Int32.TryParse(args[2], out int id))
            {
                Console.WriteLine("LM Error: invalid ID.");
                return;
            }

            LeaseData dt;
            if (id == 0) dt = new LeaseData(true);
            else dt = new LeaseData(false);

            Server server = new Server
            {
                Services = { LeaseService.BindService(new LeageManager(args[0], dt, tm_urls, lm_urls)) ,
                            PaxosService.BindService(new Paxos(args[0], dt, tm_urls, lm_urls))},
                Ports = { serverPort }
            };

            server.Start();
            
            int numTimeSlots = Int32.Parse(args[3]);
            int timeSlotDuration = Int32.Parse(args[5]);

            PaxosLeader pl = new PaxosLeader(args[0], dt, id, tm_urls, lm_urls);

            Console.WriteLine("Insecure server listening on port " + url.Port);
            //Configuring HTTP for client connections in Register method
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
            
            // Wait until wall clock time
            DateTime startingTime = DateTime.Parse(args[4]);
            TimeSpan timeToWait = startingTime - DateTime.Now;
            if (timeToWait > TimeSpan.Zero)
            {
                Console.WriteLine("Waiting " + timeToWait.TotalMilliseconds + "ms to start");
                Thread.Sleep(timeToWait);
            }
            //Thread.Sleep(5000);
            pl.cycle(timeSlotDuration, numTimeSlots);
        }
    }
}