using Grpc.Core;
using Grpc.Net.Client;
using DADTKV_LM.Structs;

namespace DADTKV_LM.Contact
{
    public class TmContact
    {
        List<LeaseService.LeaseServiceClient> tm_stubs = null;
        List<GrpcChannel> tm_channels = new List<GrpcChannel>();

        public TmContact(List<string> tm_urls)
        {
            foreach (string url in tm_urls) tm_channels.Add(GrpcChannel.ForAddress(url));
        }
        public void BroadLease(int epoch, List<Request> leases)
        {
            LeaseReply reply;
            LeaseBroadCastRequest request = new LeaseBroadCastRequest { Epoch = epoch }; //cria request
                                     //request.Leases.AddRange(leases);
            Console.WriteLine("sent");
            Thread.Sleep(50000);
            foreach (Request r in leases)
            {
                LeaseProto lp = new LeaseProto { Tm = r.Tm_name };
                foreach (string k in r.Keys) { lp.Keys.Add(k); }
                request.Leases.Add(lp);
            }

            if (tm_stubs == null)
            {
                tm_stubs = new List<LeaseService.LeaseServiceClient>();

                foreach (GrpcChannel channel in tm_channels)
                {
                    tm_stubs.Add(new LeaseService.LeaseServiceClient(channel));
                }
            }

            foreach (LeaseService.LeaseServiceClient stub in tm_stubs)
            {

                reply = stub.LeaseBroadCastAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult(); // tirar isto de syncrono
            }
        }
    }
}
