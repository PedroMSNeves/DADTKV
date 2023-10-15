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
        private int _lease_id;
        private string _name;
        List<LeaseService.LeaseServiceClient> lm_stubs = null;
        List<GrpcChannel> lm_channels = new List<GrpcChannel>();

        public LmContact(string name, List<string> lm_urls)
        {
            _lease_id = 0;
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
        public bool RequestLease(List<string> keys)
        {
            LeaseReply reply;
            LeaseRequest request = new LeaseRequest { Id = _name }; //cria request
            request.Keys.AddRange(keys);
            if(lm_stubs == null)
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
                reply = stub.LeaseAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult();
            }
            incrementLeaseId();
            return true;
        }
        public void incrementLeaseId() { _lease_id++; }
    }
}
