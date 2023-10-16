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
        public void BroadLease(int epoch, List<Request> leases)
        {
            List<Grpc.Core.AsyncUnaryCall<LeaseReply>> reply = new List<Grpc.Core.AsyncUnaryCall<LeaseReply>>();
            LeaseBroadCastRequest request = new LeaseBroadCastRequest { Epoch = epoch }; //cria request
                                                                                         //request.Leases.AddRange(leases);
            Console.WriteLine(leases.Count);
            Console.WriteLine("sent");
            //Thread.Sleep(50000);
            foreach (Request r in leases) //as leases vem vazias
            {
                LeaseProto lp = new LeaseProto { Tm = r.Tm_name };
                foreach (string k in r.Keys) { lp.Keys.Add(k); }
                request.Leases.Add(lp);
            }
            //LeaseProto xp = new LeaseProto { Tm = "tm1" };
            //xp.Keys.Add("name3");
            //request.Leases.Add( xp );

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
                reply.Add(stub.LeaseBroadCastAsync(request)); // tirar isto de syncrono
                Console.WriteLine("DONE");
            }
            //depois usar os valores do reply
        }
    }
}
