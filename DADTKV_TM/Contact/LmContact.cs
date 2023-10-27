using Grpc.Core;
using Grpc.Net.Client;
using System;

namespace DADTKV_TM.Contact
{
    /// <summary>
    /// Conctact with Lease Manager, to request new leases
    /// </summary>
    public class LmContact
    {
        private string _name;
        List<string> lm_names;
        List<LeaseService.LeaseServiceClient> lm_stubs = null;
        bool[] bitmap;
        List<GrpcChannel> lm_channels = new List<GrpcChannel>();

        public LmContact(string name, List<string> lm_urls, List<string> l_names)
        {
            _name = name;
            foreach (string url in lm_urls)
            {
                try
                {
                    lm_channels.Add(GrpcChannel.ForAddress(url));
                }
                catch (System.UriFormatException)
                {
                    Console.WriteLine("ERROR: Invalid Lm server url");
                }
            }
            lm_names = l_names;
            bitmap = new bool[lm_channels.Count];
            for (int i = 0; i < bitmap.Length; i++) bitmap[i] = true;
        }

        /// <summary>
        /// Puts the server down
        /// </summary>
        /// <param name="name"></param>
        public void CrashedServer(string name)
        {
            for (int i = 0; i < lm_names.Count; i++)
            {
                if (lm_names[i] == name)
                {
                    bitmap[i] = false;
                    break;
                }
            }
        }

        /// <summary>
        /// Pings 
        /// </summary>
        /// <param name="name"></param>
        public bool ContactSuspect(string name, Store st)
        {
            for (int i = 0; i < lm_names.Count; i++)
            {
                if (lm_names[i] == name)
                {
                    if (bitmap[i])
                    {
                        //return PingSuspect(i, st);
                    }
                }
            }
            return false;
        }
        public bool PingSuspect(int index, Store st)
        {
            Grpc.Core.AsyncUnaryCall<PingLm> ping = lm_stubs[index].PingSuspectAsync(new PingLm(), new CallOptions(deadline: DateTime.UtcNow.AddSeconds(10)));
            Random rd = new Random();

            while (true)
            {
                Monitor.Wait(st, rd.Next(100, 150));
                if (ping.ResponseAsync.IsCompleted)
                {
                    if (ping.ResponseAsync.IsCompletedSuccessfully) return true;
                    return false;
                }
            }
        }
        public bool KillSuspect(string name, Store st)
        {
            List<Grpc.Core.AsyncUnaryCall<LeaseReply>> replies = new List<Grpc.Core.AsyncUnaryCall<LeaseReply>>();
            KillRequestLm request = new KillRequestLm { TmName = name };
            int acks = 0;
            int responses = 0;
            for (int i = 0; i < lm_names.Count; i++)
            {

                if (lm_names[i] != name && bitmap[i])
                {
                    replies.Add(lm_stubs[i].KillSuspectAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(10))));
                }
                else
                {
                    replies.Add(null);
                    responses++;
                }
            }

            Random rd = new Random();
            while (responses < lm_stubs.Count)
            {
                Monitor.Wait(st, rd.Next(100, 150));
                for (int i = 0; i < replies.Count; i++)
                {
                    if (replies[i] != null)
                    {
                        if (replies[i].ResponseAsync.IsCompleted)
                        {
                            if (replies[i].ResponseAsync.IsFaulted)
                            {
                                bitmap[i] = false;
                            }
                            else if (replies[i].ResponseAsync.Result.Ack == true) acks++;
                            responses++;
                            replies[i] = null;
                        }
                    }
                }
            }
            return acks == LmAlive();
        }
        public bool RequestLease(List<string> keys, int leaseId, ref bool killMe)
        {
            List<Grpc.Core.AsyncUnaryCall<LeaseReply>> replies = new List<Grpc.Core.AsyncUnaryCall<LeaseReply>>();
            LeaseRequest request = new LeaseRequest { Id = _name, LeaseId = leaseId }; //cria request
            request.Keys.AddRange(keys);
            Console.WriteLine("REQUEST LEASE: " + request.ToString());
            int acks = 0;
            int responses = 0;
            // Initializes lm_stubs
            if (lm_stubs == null)
            {
                lm_stubs = new List<LeaseService.LeaseServiceClient>();
                foreach(GrpcChannel channel in lm_channels)
                {
                    lm_stubs.Add(new LeaseService.LeaseServiceClient(channel));
                }
            }
            // If true, we will never be able to reach consensus
            if(LmAlive() <= Majority()) 
            { 
                killMe = true;
                return false; 
            }
            // Sends request to all the Alive Lm's
            for (int i = 0; i < lm_stubs.Count; i++)
            {
                if (bitmap[i])
                {
                    replies.Add(lm_stubs[i].LeaseAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(10))));
                }
                else 
                { 
                    replies.Add(null);
                    responses++;
                }
            }
            // Has to contact all the Alive Lm's
            while (responses < lm_stubs.Count)
            {
                for (int i = 0; i < replies.Count; i++)
                {
                    if (replies[i] != null)
                    {
                        if (replies[i].ResponseAsync.IsCompleted)
                        {
                            if (replies[i].ResponseAsync.IsFaulted)
                            {
                                bitmap[i] = false;
                            }
                            else if (replies[i].ResponseAsync.Result.Ack == true)
                            {
                                acks++;
                            }
                            responses++;
                            replies[i] = null;
                        }
                    } 
                }
            }

            // Confirmation(_name, leaseId, acks > Majority());
            Console.Write("LEASE REQUEST CHEGOU AOS LMs? ");
            Console.WriteLine(acks);
            return acks == LmAlive();
        }

        private int Majority()
        {            
            return (int) Math.Floor((decimal)((lm_stubs.Count)/2)); // perguntar se é dos vivos ou do total
        }
        private int LmAlive()
        {
            int count = 0;
            foreach (bool b in bitmap) if (b) count++;
            return count;
        }
        public List<string> GetDeadNames()
        {
            List<string> names = new List<string>();
            for (int i = 0; i < lm_names.Count; i++)
            {
                if (!bitmap[i])
                {
                    names.Add(lm_names[i]);
                }
            }
            return names;
        }
    }
}
