using Grpc.Core;
using Grpc.Net.Client;
using DADTKV_LM.Structs;
using System;

namespace DADTKV_LM.Contact
{
    public class TmContact
    {
        List<LeaseService.LeaseServiceClient> tm_stubs = null;
        List<GrpcChannel> tm_channels = new List<GrpcChannel>();

        public TmContact(List<string> tm_urls)
        {
            foreach (string url in tm_urls)
            {
                try
                {
                    tm_channels.Add(GrpcChannel.ForAddress(url));
                }
                catch (System.UriFormatException)
                {
                    Console.WriteLine("ERROR: Invalid Tm server url");
                }
            }
        }
        public bool BroadLease(int epoch, List<Request> leases)
        {
            List<Grpc.Core.AsyncUnaryCall<LeaseReply>> replies = new List<Grpc.Core.AsyncUnaryCall<LeaseReply>>();
            LeaseBroadCastRequest request = new LeaseBroadCastRequest { Epoch = epoch };

            int acks = 0;
            Console.WriteLine(leases.Count);
            Console.WriteLine("sent");

            foreach (Request r in leases)
            {
                LeaseProto lp = new LeaseProto { Tm = r.Tm_name, LeaseId = r.Lease_ID };
                foreach (string k in r.Keys) { lp.Keys.Add(k); }
                request.Leases.Add(lp);
            }

            if (tm_stubs == null)
            {
                tm_stubs = new List<LeaseService.LeaseServiceClient>();
                foreach (GrpcChannel channel in tm_channels)
                {
                    Console.WriteLine(channel.Target);
                    tm_stubs.Add(new LeaseService.LeaseServiceClient(channel));
                }
            }
            foreach (LeaseService.LeaseServiceClient stub in tm_stubs)
            {
                Console.WriteLine(request.Leases.Count);
                replies.Add(stub.LeaseBroadCastAsync(request));
                Console.WriteLine("DONE");
            }
            foreach (Grpc.Core.AsyncUnaryCall<LeaseReply> reply in replies)
            {
                try
                {
                    if (reply.ResponseAsync.Result.Ack) acks++;
                }
                catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded)
                {
                    Console.WriteLine("Could not contact TM");
                }
            }
            Console.Write("RESULTADO PAXOS CHEGOU AOS TMs? ");
            Console.WriteLine(acks);   
            return acks > (tm_stubs.Count / 2); //aqui é todos ou maioria?
        }
    }
}
