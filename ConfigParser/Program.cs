using System.Diagnostics;

namespace ConfigParser
{
    class Program
    {
        public static Process createProcess(string project, string arguments)
        {
            Process process = new Process();
            process.StartInfo.FileName = "alacritty";
            process.StartInfo.Arguments = $"-e dotnet run --project /Users/cjv/Documents/dad/DADTKV/{project} {arguments}";
            process.StartInfo.UseShellExecute = true;
            process.StartInfo.CreateNoWindow = false;
            return process;
        }

        public static void launchClient(string id, string clientScript, List<string> transactionManagers)
        {
            // Pass all Transaction Manager URLs to the client
            string args = $"/Users/cjv/Documents/dad/DADTKV/{clientScript} {id}";
            foreach (string transactionManager in transactionManagers)
            {
                args += $" {transactionManager}";
            }

            Console.WriteLine($"Launching client {id} with script {clientScript}");
            Process client = createProcess("Client", args);
            client.Start();
        }

        public static void launchTransactionManager(string id, string url, int crash_ts, List<string> transactionManagers, List<string> leaseManagers, Dictionary<int, List<string>> suspectedProcesses)
        {
            string args = $"{id} {url} {crash_ts}";
            // Pass all Transaction Manager URLs to the Transaction Manager
            foreach (string transactionManager in transactionManagers)
            {
                if (transactionManager != url) args += $" {transactionManager}";
            }

            args += " LM";

            // Pass all Lease Manager URLs to the Transaction Manager
            foreach (string leaseManager in leaseManagers)
            {
                args += $" {leaseManager}";
            }
            
            // Pass all suspected processes to the Transaction Manager
            args += " SP";
            foreach (KeyValuePair<int, List<string>> suspectedProcessesPair in suspectedProcesses)
            {
                args += $" {suspectedProcessesPair.Key}";
                foreach (string suspectedProcess in suspectedProcessesPair.Value)
                {
                    args += $" {suspectedProcess}";
                }
            }

            Console.WriteLine($"Launching Transaction Manager {id} with args: {args}");
            Process server = createProcess("DADTKV_TM", $"{args}");
            server.Start();
        }

        public static void launchLeaseManager(string id, int paxos_id, string url, int numTimeSlots, string startingTime, int timeSlotDuration, int crash_ts, List<string> transactionManagers, List<string> leaseManagers, Dictionary<int, List<string>> suspectedProcesses)
        {
            string args = $"{id} {url} {paxos_id} {numTimeSlots} {startingTime} {timeSlotDuration} {crash_ts}";
            // Pass all Lease Manager URLs to the Lease Manager
            foreach (string leaseManager in leaseManagers)
            {
                if (leaseManager != url) args += $" {leaseManager}";
            }

            args += " TM";

            // Pass all Transaction Manager URLs to the Lease Manager
            foreach (string transactionManager in transactionManagers)
            {
                args += $" {transactionManager}";
            }
            
            // Pass all suspected processes to the Lease Manager
            args += " SP";
            foreach (KeyValuePair<int, List<string>> suspectedProcessesPair in suspectedProcesses)
            {
                args += $" {suspectedProcessesPair.Key}";
                foreach (string suspectedProcess in suspectedProcessesPair.Value)
                {
                    args += $" {suspectedProcess}";
                }
            }

            Console.WriteLine($"Launching Lease Manager {id} with args: {args}");
            Process server = createProcess("DADTKV_LM", $"{args}");
            server.Start();
        }
        public static void Main(string[] args)
        {
            int numTimeSlots = 0;
            string startingTime = "00:00:00";
            int timeSlotDuration = 0;
            
            int count = 0;
            Dictionary<int, string> serverProcesses = new Dictionary<int, string>();
            Dictionary<string, int> serverCrashTimeslots = new Dictionary<string, int>();
            Dictionary<string, Dictionary<int, List<string>>> suspectedProcesses = new Dictionary<string, Dictionary<int, List<string>>>();

            // Save server IDs and URLs to pass onto the clients
            Dictionary<string, string> transactionManagers = new Dictionary<string, string>();
            Dictionary<string, string> leaseManagers = new Dictionary<string, string>();

            // Save client IDs and scripts to launch them
            Dictionary<string, string> clients = new Dictionary<string, string>();

            string[] lines = File.ReadAllLines(args[0]);
            foreach (string line in lines)
            {
                string[] words = line.Split(' ');

                // Skip comments
                if (words[0] == "#")
                {
                    continue;
                }

                // Process command
                if (words[0] == "P")
                {
                    string id = words[1];

                    // Transaction Manager
                    if (words[2] == "T")
                    {
                        string url = words[3];
                        transactionManagers.Add(id, url);
                        serverProcesses.Add(count++, id);
                    }

                    // Lease Manager
                    else if (words[2] == "L")
                    {
                        string url = words[3];
                        leaseManagers.Add(id, url);
                        serverProcesses.Add(count++, id);
                    }

                    // Client
                    else if (words[2] == "C")
                    {
                        string clientScript = words[3];
                        clients.Add(id, clientScript);
                    }
                }

                // Number of time slots
                else if (words[0] == "S")
                {
                    numTimeSlots = Int32.Parse(words[1]);
                }

                // Starting physical wall time
                else if (words[0] == "T")
                {
                    startingTime = words[1];
                }

                // Time slot duration
                else if (words[0] == "D")
                {
                    timeSlotDuration = Int32.Parse(words[1]);
                }

                // Server state
                else if (words[0] == "F")
                {
                    int timeSlot = Int32.Parse(words[1]);

                    for (int i = 2; i < 2 + serverProcesses.Count; i++)
                    {
                        if (words[i] == "N")
                        {
                            // Do nothing
                        }
                        else if (words[i] == "C")
                        {
                            string serverId = serverProcesses[i - 2];
                            serverCrashTimeslots.Add(serverId, timeSlot);
                        }
                    }
                    
                    for (int i = 2 + serverProcesses.Count; i < words.Length; i++)
                    {
                        string[] suspectedProcessesPair = words[i].Split(',');
                        string suspectingProcess = suspectedProcessesPair[0].Substring(1);
                        string suspectedProcess = suspectedProcessesPair[1].Substring(0, suspectedProcessesPair[1].Length - 1);
                        if (!suspectedProcesses.ContainsKey(suspectingProcess))
                        {
                            suspectedProcesses.Add(suspectingProcess, new Dictionary<int, List<string>>());
                        }
                        if (!suspectedProcesses[suspectingProcess].ContainsKey(timeSlot))
                        {
                            suspectedProcesses[suspectedProcess].Add(timeSlot, new List<string>());
                        }
                        suspectedProcesses[suspectingProcess][timeSlot].Add(suspectedProcess);
                    }
                }
            }

            // Launch all servers
            int paxos_id = 0;
            foreach (KeyValuePair<string, string> leaseManager in leaseManagers)
            {
                int crash_ts = serverCrashTimeslots.ContainsKey(leaseManager.Key) ? serverCrashTimeslots[leaseManager.Key] : -1;
                Dictionary<int, List<string>> suspectedProcessesForLM = suspectedProcesses.ContainsKey(leaseManager.Key) ? suspectedProcesses[leaseManager.Key] : new Dictionary<int, List<string>>();
                launchLeaseManager(leaseManager.Key, paxos_id++, leaseManager.Value, numTimeSlots, startingTime, timeSlotDuration, crash_ts, transactionManagers.Values.ToList(), leaseManagers.Values.ToList(), suspectedProcessesForLM);
            }

            foreach (KeyValuePair<string, string> transactionManager in transactionManagers)
            {
                int crash_ts = serverCrashTimeslots.ContainsKey(transactionManager.Key) ? serverCrashTimeslots[transactionManager.Key] : -1;
                Dictionary<int, List<string>> suspectedProcessesForTM = suspectedProcesses.ContainsKey(transactionManager.Key) ? suspectedProcesses[transactionManager.Key] : new Dictionary<int, List<string>>();
                launchTransactionManager(transactionManager.Key, transactionManager.Value, crash_ts, transactionManagers.Values.ToList(), leaseManagers.Values.ToList(), suspectedProcessesForTM);
            }

            // Launch all clients
            foreach (KeyValuePair<string, string> client in clients)
            {
                launchClient(client.Key, client.Value, transactionManagers.Values.ToList());
            }
        }
    }
}