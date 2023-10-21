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
        List<LeaseService.LeaseServiceClient> lm_stubs = null;
        List<GrpcChannel> lm_channels = new List<GrpcChannel>();

        public LmContact(string name, List<string> lm_urls)
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

        }
        public bool RequestLease(List<string> keys, int leaseId, Store st)
        {
            List<Grpc.Core.AsyncUnaryCall<LeaseReply>> replies = new List<Grpc.Core.AsyncUnaryCall<LeaseReply>>();
            LeaseRequest request = new LeaseRequest { Id = _name, LeaseId = leaseId }; //cria request
            request.Keys.AddRange(keys);
            Console.WriteLine("REQUEST LEASE: " + request.ToString());
            int acks = 0;
            if (lm_stubs == null)
            {
                lm_stubs = new List<LeaseService.LeaseServiceClient>();
                foreach(GrpcChannel channel in lm_channels)
                {
                    lm_stubs.Add(new LeaseService.LeaseServiceClient(channel));
                }
            }
            foreach (LeaseService.LeaseServiceClient stub in lm_stubs)
            {
                // Perguntar se basta receber ack de apenas 1 Lm, se precisamos de todos os ack ou uma maioria
                replies.Add(stub.LeaseAsync(request));
            }
            Random rd = new Random();
            while (acks < lm_stubs.Count)
            {
                Monitor.Wait(st, rd.Next(100, 150));
                for (int i = 0; i < replies.Count; i++)
                {
                    if (replies[i].ResponseAsync.IsCompleted)
                    {
                        if (replies[i].ResponseAsync.Result.Ack == true) acks++;
                        replies.Remove(replies[i]);
                        i--;
                        if (acks == lm_stubs.Count) break;
                    }
                }
                if (replies.Count == 0) break; //error
            }
            Console.Write("LEASE REQUEST CHEGOU AOS LMs? ");
            Console.WriteLine(acks);
            return true;
        }
    }
}
