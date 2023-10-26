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

        // Expected input: name, my_url, id, numTimeSlots, startingTime, timeSlotDuration, crash_ts, other_lm_id, other_lm_url, ..., TM (delimiter), tm_id, tm_url, ..., SP (delimiter), sp_ts, sp_id, ..., CP (delimeter), cp_ts, cp_id, ...
        public static void Main(string[] args)
        {
            Console.WriteLine("LM");
            foreach (string s in args) Console.Write(s + " ");

            Dictionary<string, string> tm_urls = new Dictionary<string, string>();
            Dictionary<string, string> lm_urls = new Dictionary<string, string>();
            Dictionary<int, List<string>> suspected_processes = new Dictionary<int, List<string>>(); // <timeslot, process_ids>
            Dictionary<int, List<string>> crashed_processes = new Dictionary<int, List<string>>(); // <timeslot, process_ids>

            // Minimum is name, his own url, id, numTimeSlots, startingTime, timeSlotDuration, crash_ts, the TM delimiter and 1 TM url
            if (args.Length < 10) exitOnError("Invalid number of arguments");

            Uri my_url = new Uri(args[1]);
            ServerPort serverPort = new ServerPort(my_url.Host, my_url.Port, ServerCredentials.Insecure);

            if (!Int32.TryParse(args[2], out int id)) exitOnError("Invalid ID format");

            int crash_ts = Int32.Parse(args[6]);

            bool tm = false;
            bool sp = false;
            bool cp = false;
            int currentTimeslot = -1;
            for (int i = 7; i < args.Length; i++)
            {
                // Console.WriteLine(tm + ": " + args[i]);
                if (args[i].Equals("TM"))
                {
                    tm = true;
                }
                else if (args[i].Equals("SP"))
                {
                    tm = false;
                    sp = true;
                }
                else if (args[i].Equals("CP"))
                {
                    sp = false;
                    cp = true;
                    currentTimeslot = -1;
                }
                else if (!tm && !sp && !cp)
                {
                    lm_urls.Add(args[i], args[i + 1]);
                    i++;
                }
                else if (tm && !sp && !cp)
                {
                    tm_urls.Add(args[i], args[i + 1]);
                    i++;
                }
                else if (sp && !cp)
                {
                    if (args[i].All(char.IsDigit))
                    {
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
                else if (cp)
                {
                    if (args[i].All(char.IsDigit))
                    {
                        currentTimeslot = int.Parse(args[i]);
                        if (!crashed_processes.ContainsKey(currentTimeslot + 1))
                        {
                            crashed_processes.Add(currentTimeslot + 1, new List<string>());
                        }
                    }
                    else if (currentTimeslot != -1)
                    {
                        crashed_processes[currentTimeslot + 1].Add(args[i]);
                    }
                }
            }
            Console.WriteLine("CRASHES");
            foreach (int epoch in crashed_processes.Keys)
            {
                Console.Write(epoch + ": ");
                foreach (string name in crashed_processes[epoch]) Console.Write(name + " ");
                Console.WriteLine();
            }

            //Console.ReadKey();

            if (!tm && !sp && !cp) exitOnError("No TransactionManagers provided.");

            LeaseData dt;
            if (id == 0) dt = new LeaseData(true);
            else dt = new LeaseData(false);

            Server server = new Server
            {
                Services = { LeaseService.BindService(new LeageManager(args[0], dt, tm_urls.Values.ToList(), lm_urls.Values.ToList(), tm_urls.Keys.ToList(), lm_urls.Keys.ToList())) ,
                            PaxosService.BindService(new Paxos(args[0], dt, tm_urls.Values.ToList(), lm_urls.Values.ToList(), tm_urls.Keys.ToList(), lm_urls.Keys.ToList()))},
                Ports = { serverPort }
            };

            server.Start();

            int numTimeSlots = Int32.Parse(args[3]);
            int timeSlotDuration = Int32.Parse(args[5]);

            PaxosLeader pl = new PaxosLeader(args[0], dt, id, crash_ts, tm_urls.Values.ToList(), lm_urls.Values.ToList(), tm_urls.Keys.ToList(), lm_urls.Keys.ToList(), crashed_processes);

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
            //Thread.Sleep(5000);
            pl.cycle(timeSlotDuration, numTimeSlots);
        }
    }
}