using System.Diagnostics;

namespace ConfigParser
{
    class Program
    {
        public static Process createProcess(string project, string arguments)
        {
            Process process = new Process();
            process.StartInfo.FileName = "cmd.exe";
            process.StartInfo.Arguments = $"/c dotnet run --project {project} {arguments}";
            process.StartInfo.UseShellExecute = true;
            process.StartInfo.CreateNoWindow = false;
            return process;
        }

        public static void launchClient(string id, string clientScript, List<string> transactionManagers)
        {
            // Pass all Transaction Manager URLs to the client
            string args = $"{id}";
            foreach (string transactionManager in transactionManagers)
            {
                args += $" {transactionManager}";
            }

            Process client = createProcess("Client", args);

            Console.WriteLine($"Launching client {id} with script {clientScript}");
            client.Start();

            string input = File.ReadAllText(clientScript);
            client.StandardInput.Write(input);
        }

        public static void launchServer(string id, string url, string args)
        {
            Process server = createProcess("Server", $"{id} {url} {args}");
            server.Start();
        }

        public static void launchTransactionManager(string id, string url, List<string> transactionManagers, List<string> leaseManagers)
        {
            string args = "";
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

            Console.WriteLine($"Launching Transaction Manager {id} with url {url}");
            launchServer(id, url, args);
        }

        public static void launchLeaseManager(string id, int paxos_id, string url, List<string> transactionManagers, List<string> leaseManagers)
        {
            string args = $"{paxos_id}";
            // Pass all Lease Manager URLs to the Lease Manager
            foreach (string leaseManager in leaseManagers)
            {
                if (leaseManager != url)  args += $" {leaseManager}";
            }

            args += " TM";

            // Pass all Transaction Manager URLs to the Lease Manager
            foreach (string transactionManager in transactionManagers)
            {
                args += $" {transactionManager}";
            }

            Console.WriteLine($"Launching Lease Manager {id} with url {url}");
            launchServer(id, url, args);
        }
        public static void Main(string[] args)
        {
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
                    }

                    // Lease Manager
                    else if (words[2] == "L")
                    {
                        string url = words[3];
                        leaseManagers.Add(id, url);
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
                    int numTimeSlots = Int32.Parse(words[1]);
                }

                // Starting physical wall time
                else if (words[0] == "T")
                {
                    string startingTime = words[1];
                }

                // Time slot duration
                else if (words[0] == "D")
                {
                    int timeSlotDuration = Int32.Parse(words[1]);
                }

                // Server state
                else if (words[0] == "F")
                {
                    int timeSlot = Int32.Parse(words[1]);
                    // TODO
                }
            }

            // Launch all servers
            foreach (KeyValuePair<string, string> transactionManager in transactionManagers)
            {
                launchTransactionManager(transactionManager.Key, transactionManager.Value, transactionManagers.Values.ToList(), leaseManagers.Values.ToList());
            }

            int paxos_id = 0;
            foreach (KeyValuePair<string, string> leaseManager in leaseManagers)
            {
                launchLeaseManager(leaseManager.Key, paxos_id++, leaseManager.Value, transactionManagers.Values.ToList(), leaseManagers.Values.ToList());
            }

            // Launch all clients
            foreach (KeyValuePair<string, string> client in clients)
            {
                launchClient(client.Key, client.Value, transactionManagers.Values.ToList());
            }
        }
    }
}