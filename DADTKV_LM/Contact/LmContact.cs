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
        public int NumOfStubs() { return lm_channels.Count; }

        public Promise PrepareRequest(PrepareRequest request, int i)
        {
            Promise reply;
            if (lm_stubs == null)
            {
                lm_stubs = new List<PaxosService.PaxosServiceClient>();
                foreach (GrpcChannel channel in lm_channels)
                {
                    lm_stubs.Add(new PaxosService.PaxosServiceClient(channel));
                }
            }
            try
            {
                Console.WriteLine("i: "+i);
                Console.WriteLine(lm_stubs[i].ToString());
                reply = lm_stubs[i].PrepareAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult();
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded || ex.StatusCode == StatusCode.Unavailable)// || ex.StatusCode == StatusCode.Unknown)
            {
                reply = new Promise { Ack = false };
                Console.WriteLine("Erro ao contactar os outros LMs");
            }

            return reply;
        }
        public bool BroadAccepted(AcceptRequest request) //Used by the others to propagate accepted
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
            lock (this)
            {
                while (!(values[0] + 1 >= (lm_stubs.Count + 1) / 2 || values[1] >= (lm_stubs.Count + 1) / 2)) Monitor.Wait(this);
                if (values[0] + 1 >= (lm_stubs.Count + 1) / 2) return true;
                else return false;
            }
        }
        private void sendAccepted(ref List<int> values, PaxosService.PaxosServiceClient stub, AcceptRequest request)
        {
            Console.WriteLine("NEW THREAD TO SEND ACCEPTED TO OTHERS");
            AcceptReply reply = stub.AcceptedAsync(request).GetAwaiter().GetResult();
            lock (this)
            {
                if (reply.Ack) values[0]++;
                else values[1]++;
                Monitor.Pulse(this);
            }
            Console.WriteLine("ACCEPTED SENDED YES: " + values[0]);
            Console.WriteLine("ACCEPTED SENDED NO: " + values[1]);
        }
        public bool AcceptRequest(AcceptRequest request) //Used by the leaders to propagate accept 
        {
            Console.WriteLine("IN");
            List<Grpc.Core.AsyncUnaryCall<AcceptReply>> replies = new List<Grpc.Core.AsyncUnaryCall<AcceptReply>>();
            int acks = 1;
            foreach (PaxosService.PaxosServiceClient stub in lm_stubs)
            {
                replies.Add(stub.AcceptAsync(request)); // tirar isto de syncrono
            }
            foreach (Grpc.Core.AsyncUnaryCall<AcceptReply> reply in replies)
            {
                try
                {
                    if (reply.ResponseAsync.Result.Ack) acks++;
                }
                catch (RpcException ex) when (ex.StatusCode == StatusCode.Unavailable || ex.StatusCode == StatusCode.DeadlineExceeded)
                {
                    Console.WriteLine("Could not contact LM");
                }
            }
            Console.WriteLine("ACKS :" + acks);
            Console.WriteLine("OUT");
            return acks > (lm_stubs.Count + 1) / 2; //em vez do count vai ser com o bitmap dos lms que estao vivos
        }
       
        public bool getLeaderAck(int possible_leader)
        {
            if (lm_stubs == null) //É preciso?
            {
                lm_stubs = new List<PaxosService.PaxosServiceClient>();
                foreach (GrpcChannel channel in lm_channels)
                {
                    lm_stubs.Add(new PaxosService.PaxosServiceClient(channel));
                }
            }
            AcceptReply reply;
            try 
            {
                reply = lm_stubs[possible_leader].GetLeaderAckAsync(new AckRequest(), new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult();
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded || ex.StatusCode == StatusCode.Unavailable)
            {
                Console.WriteLine("Leader probably dead");
                reply = new AcceptReply { Ack = false };
            }
            return reply.Ack;
        }
    }
}