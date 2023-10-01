using System.Diagnostics;

namespace ConfigParser
{
    class Program
    {
        public static void launchClient(string id, string clientScript)
        {
            Process client = new Process();
            client.StartInfo.FileName = "dotnet";
            client.StartInfo.Arguments = $"run --project Client {id}"; // TODO pass all TMs
            client.StartInfo.UseShellExecute = false;
            client.StartInfo.RedirectStandardInput = true;

            Console.WriteLine($"Launching client {id} with script {clientScript}");
            client.Start();

            string input = File.ReadAllText(clientScript);
            client.StandardInput.Write(input);
        }

        public static void launchTransactionManager(string id, string url)
        {
            Process tm = new Process();
            tm.StartInfo.FileName = "dotnet";
            tm.StartInfo.Arguments = $"run --project DADTKV_TM {id} {url}";
            tm.StartInfo.UseShellExecute = false;

            Console.WriteLine($"Launching Transaction Manager {id} with url {url}");
            tm.Start();
        }

        public static void launchLeaseManager(string id, string url)
        {
            Process lm = new Process();
            lm.StartInfo.FileName = "dotnet";
            lm.StartInfo.Arguments = $"run --project DADTKV_LM {id} {url}";
            lm.StartInfo.UseShellExecute = false;

            Console.WriteLine($"Launching Lease Manager {id} with url {url}");
            lm.Start();
        }
        public static void Main(string[] args)
        {
            // Read configuration file as argument
            string input = File.ReadAllText(args[0]);

            // Split configuration file into lines
            string[] lines = input.Split('\n');

            // Parse each line
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
                        launchTransactionManager(id, url);
                    }

                    // Lease Manager
                    else if (words[2] == "L")
                    {
                        string url = words[3];
                        launchLeaseManager(id, url);
                    }

                    // Client
                    else if (words[2] == "C")
                    {
                        string clientScript = words[3];
                        launchClient(id, clientScript);
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
        }
    }
}