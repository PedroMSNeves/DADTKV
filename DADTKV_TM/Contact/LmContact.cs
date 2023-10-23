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
        bool[] bitmap;
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
            bitmap = new bool[lm_channels.Count];
            for (int i = 0; i < bitmap.Length; i++) bitmap[i] = true;
        }
        public bool RequestLease(List<string> keys, int leaseId)
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
            if(LmAlive() <= Majority()) { return false; }
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
            int alive = LmAlive();
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
            return ConfirmRequest(acks == alive, leaseId);
        }
        private bool ConfirmRequest (bool ack, int leaseId)
        {
            ConfirmLeaseRequest request = new ConfirmLeaseRequest { Id = _name, LeaseId = leaseId, Ack = ack }; //cria request
            for (int i = 0; i < lm_stubs.Count; i++)
            {
                if (bitmap[i])
                {
                    lm_stubs[i].ConfirmLeaseAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(10)));
                }
            }
            return true;
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
    }
}
