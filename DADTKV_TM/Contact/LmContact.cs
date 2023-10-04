using Grpc.Core;
using Grpc.Net.Client;

namespace DADTKV_TM.Contact
{
    /// <summary>
    /// Conctact with Lease Manager, to request new leases
    /// </summary>
    public class LmContact
    {
        private int _lease_id;
        private string _name;
        List<LeaseService.LeaseServiceClient> lm_stubs = new List<LeaseService.LeaseServiceClient>();
        public LmContact(string name, List<string> lm_urls)
        {
            _lease_id = 0;
            _name = name;
            foreach (string url in lm_urls)
            {
                try
                {
                    lm_stubs.Add(new LeaseService.LeaseServiceClient(GrpcChannel.ForAddress(url)));
                }
                catch (System.UriFormatException ex)
                {
                    Console.WriteLine("ERROR: Invalid Lm server url");
                }
            }

        }
        public bool RequestLease(List<string> keys)
        {
            LeaseReply reply;
            LeaseRequest request = new LeaseRequest { Id = _name, LeaseRequestId = _lease_id }; //cria request
            request.Keys.AddRange(keys);

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
