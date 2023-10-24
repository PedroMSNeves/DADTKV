using Grpc.Core;
using System;
using DADTKV_LM.Impls;

namespace DADTKV_LM
{
    class Program
    {

        public static void exitOnError(string message)
        {
            Console.WriteLine($"Error: {message}.\nPress any key to close.");
            Console.ReadKey();
            Environment.Exit(0);
        }

        // Expected input: name, my_url, id, numTimeSlots, startingTime, timeSlotDuration, crash_ts, other_lm_url, ..., TM (delimiter), tm_url, ..., SP (delimiter), sp_ts, sp_id, ...
        public static void Main(string[] args)
        {
            Console.WriteLine("LM");
            foreach (string s in args) Console.Write(s + " ");
            
            List<string> tm_urls = new List<string>();
            List<string> lm_urls = new List<string>();
            Dictionary<int, List<string>> suspected_processes = new Dictionary<int, List<string>>(); // <timeslot, process_ids>
            
            // Minimum is name, his own url, id, numTimeSlots, startingTime, timeSlotDuration, crash_ts, the TM delimiter and 1 TM url
            if (args.Length < 9) exitOnError("Invalid number of arguments");
            
            Uri my_url = new Uri(args[1]);
            ServerPort serverPort = new ServerPort(my_url.Host, my_url.Port, ServerCredentials.Insecure);
            
            if (!Int32.TryParse(args[2], out int id)) exitOnError("Invalid ID format");
            
            int crash_ts = Int32.Parse(args[6]);
            
            bool tm = false;
            bool sp = false;
            int currentTimeslot = -1;
            for (int i = 7; i < args.Length; i++)
            {
                // Console.WriteLine(tm + ": " + args[i]);
                if (args[i].Equals("TM")) {
                    tm = true;                   
                }
                else if (args[i].Equals("SP")) {
                    tm = false;
                    sp = true;
                }
                else if (!tm && !sp)
                {
                    lm_urls.Add(args[i]);
                }
                else if (tm && !sp)
                {
                    tm_urls.Add(args[i]);
                }
                else if (sp) {
                    if (args[i].All(char.IsDigit)) {
                        currentTimeslot = int.Parse(args[i]);
                        if (!suspected_processes.ContainsKey(currentTimeslot))
                        {
                            suspected_processes.Add(currentTimeslot, new List<string>());
                        }
                    }
                    else if (currentTimeslot != -1)
                    {
                        suspected_processes[currentTimeslot].Add(args[i]);
                    }
                }
            }

            if (!tm) exitOnError("No TransactionManagers provided.");

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

            Console.WriteLine("Insecure server listening on port " + my_url.Port);
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
            Thread.Sleep(5000);
            pl.cycle(timeSlotDuration, numTimeSlots);
        }
    }
}