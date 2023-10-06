using Grpc.Core;
using Grpc.Net.Client;

namespace DADTKV_LM.Contact
{
    public class LmContact
    {
        private string _name;
        public List<PaxosService.PaxosServiceClient> lm_stubs = new List<PaxosService.PaxosServiceClient>();
        public LmContact(string name, List<string> lm_urls)
        {
            _name = name;
            foreach (string url in lm_urls) lm_stubs.Add(new PaxosService.PaxosServiceClient(GrpcChannel.ForAddress(url)));
        }
        public bool BroadAccepted(AcceptRequest request)
        {
            List<int> values = new List<int>();
            values.Add(0);//ack counter
            values.Add(0);//nack counter
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