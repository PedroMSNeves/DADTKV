using Grpc.Core;
using Grpc.Net.Client;
using DADTKV_LM.Structs;
using System;

namespace DADTKV_LM.Contact
{
    public class TmContact
    {
        List<string> tm_names;
        List<LeaseService.LeaseServiceClient> tm_stubs = null;
        List<GrpcChannel> tm_channels = new List<GrpcChannel>();
        bool[] tm_bitmap;
        private string _name;

        public TmContact(string name, List<string> tm_urls, List<string> t_names)
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
            tm_names = t_names;
            tm_bitmap = new bool[tm_channels.Count];
            for (int i = 0; i < tm_bitmap.Length; i++) tm_bitmap[i] = true;
        }
        public void CrashedServer(string name)
        {
            for (int i = 0; i < tm_names.Count; i++)
            {
                if (tm_names[i] == name)
                {
                    tm_bitmap[i] = false;
                    break;
                }
            }
        }
        public bool Alive(string name)
        {
            for (int i = 0; i < tm_names.Count; i++)
            {
                if (tm_names[i] == name && tm_bitmap[i]) return true;
            }
            return false;
        }
        private int AliveTMs()
        {
            int count = 0;
            foreach (bool b in tm_bitmap) if (b) count++;
            return count;
        }
        public bool BroadLease(int epoch, List<Request> leases)
        {
            List<Grpc.Core.AsyncUnaryCall<LeaseReply>> replies = new List<Grpc.Core.AsyncUnaryCall<LeaseReply>>();
            LeaseBroadCastRequest request = new LeaseBroadCastRequest { Epoch = epoch, LmName = _name };

            int acks = 0;
            int responses = 0;
            Console.WriteLine(leases.Count);
            Console.WriteLine("sent");

            foreach (Request r in leases)
            {
                LeaseProto lp = new LeaseProto { Tm = r.Tm_name, LeaseId = r.Lease_ID };
                foreach (string k in r.Keys) { lp.Keys.Add(k); }
                request.Leases.Add(lp);
            }
            Console.WriteLine(request.ToString());

            if (tm_stubs == null)
            {
                tm_stubs = new List<LeaseService.LeaseServiceClient>();
                foreach (GrpcChannel channel in tm_channels)
                {
                    Console.WriteLine(channel.Target);
                    tm_stubs.Add(new LeaseService.LeaseServiceClient(channel));
                }
            }
            //if (AliveTMs() <= Majority()) return false;

            for (int i = 0; i < tm_stubs.Count; i++)
            {
                Console.WriteLine("TM "+i+ " é branco? "+ tm_bitmap[i]);
                if (tm_bitmap[i])
                {
                    Console.WriteLine(request.Leases.Count);
                    replies.Add(tm_stubs[i].LeaseBroadCastAsync(request));
                    Console.WriteLine("DONE");
                }
                else
                {
                    replies.Add(null);
                    responses++;
                }
            }
            while (responses < tm_stubs.Count)
            {
                for (int i = 0; i < replies.Count; i++)
                {
                    if (replies[i] != null)
                    {
                        if (replies[i].ResponseAsync.IsCompleted)
                        {
                            if (replies[i].ResponseAsync.IsFaulted)
                            {
                                tm_bitmap[i] = false;
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
            Console.Write("RESULTADO PAXOS CHEGOU AOS TMs? ");
            Console.WriteLine(acks);   
            return acks == AliveTMs();
        }
        private int Majority()
        {
            return (int)Math.Floor((decimal)((tm_stubs.Count) / 2));
        }
    }
}
