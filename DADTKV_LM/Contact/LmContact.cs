using DADTKV_LM.Structs;
using Grpc.Core;
using Grpc.Net.Client;

namespace DADTKV_LM.Contact
{
    public class LmContact
    {
        private string _name;
        public List<PaxosService.PaxosServiceClient> lm_stubs = null;
        List<GrpcChannel> lm_channels = new List<GrpcChannel>();

        public LmContact(string name, List<string> lm_urls)
        {
            _name = name;
            foreach (string url in lm_urls) lm_channels.Add(GrpcChannel.ForAddress(url));
        }

        public bool PrepareRequest(PrepareRequest request,int Write_TS, int Other_TS, List<Request> Others_value)
        {
            Promise reply;
            int promises = 1;
            foreach (PaxosService.PaxosServiceClient stub in lm_stubs)
            {
                reply = stub.PrepareAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult(); // tirar isto de syncrono
                if (reply.Ack) promises++;

                Other_TS = Write_TS;
                if (reply.WriteTs > Other_TS)
                {
                    foreach (LeasePaxos l in reply.Leases)
                    {
                        Others_value.Add(new Request(l.Tm, l.Keys.ToList()));
                    }
                    Other_TS = reply.WriteTs;
                }
            }
            return promises > (lm_stubs.Count + 1) / 2; //true se for maioria, removemos da lista se morrerem?
        }
        public bool BroadAccepted(AcceptRequest request)
        {
            List<int> values = new List<int>();
            values.Add(0);//ack counter
            values.Add(0);//nack counter


            if (lm_stubs == null)
            {
                lm_stubs = new List<PaxosService.PaxosServiceClient>();
                foreach (GrpcChannel channel in lm_channels)
                {
                    lm_stubs.Add(new PaxosService.PaxosServiceClient(channel));
                }
            }
            foreach (PaxosService.PaxosServiceClient stub in lm_stubs)
            {
                Thread t = new Thread(() => { sendAccepted(ref values, stub, request); });
                t.Start();
            }
            lock (values)
            {
                while (!(values[0] + 1 >= (lm_stubs.Count + 1) / 2 || values[1] >= (lm_stubs.Count + 1) / 2)) Monitor.Wait(this);
                if (values[0] + 1 >= (lm_stubs.Count + 1) / 2) return true;
                else return false;
            }
        }
        public bool AcceptRequest(AcceptRequest request)
        {
            AcceptReply reply;
            int acks = 1;
            foreach (PaxosService.PaxosServiceClient stub in lm_stubs)
            {
                reply = stub.AcceptAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult(); // tirar isto de syncrono
                if (reply.Ack) acks++;
            }
            return acks > (lm_stubs.Count + 1) / 2; //em vez do count vai ser com o bitmap dos lms que estao vivos
        }
        private void sendAccepted(ref List<int> values, PaxosService.PaxosServiceClient stub, AcceptRequest request)
        {
            AcceptReply reply = stub.AcceptedAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult();
            lock (values)
            {
                if (reply.Ack) values[0]++;
                else values[1]++;
                Monitor.Pulse(this);
            }
        }
        public bool getLeaderAck(int possible_leader)
        {
            return lm_stubs[possible_leader].GetLeaderAckAsync(new AckRequest(), new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult().Ack;
        }
    }
}