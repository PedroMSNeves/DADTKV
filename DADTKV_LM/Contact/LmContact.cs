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
        List<int> lm_bitmap = new List<int>();

        public LmContact(string name, List<string> lm_urls)
        {
            _name = name;
            foreach (string url in lm_urls) 
            { 
                lm_channels.Add(GrpcChannel.ForAddress(url));
                lm_bitmap.Add(1);
            }
        }
        public int CheckPossibleLeader(int possibleLeader)
        {
            for (int i = 0; i < lm_bitmap.Count; i++)
            {
                if (i < possibleLeader)
                {
                    lm_bitmap[i] = 0; //em vez de assumri que morreu logo podiamos primeiro contactar com ele
                    continue;
                }
                if (i == possibleLeader && lm_bitmap[i] == 1) return possibleLeader;
                if (lm_bitmap[i] == 1)
                {
                    return i;
                }
            }
            return -1;
        }
        public int AliveLMs() 
        {
            lock (lm_bitmap)
            {
                int n = 0;
                foreach (int lm in lm_bitmap)
                {
                    if (lm == 1) n++;
                }
                return n;
            }
        }
        public int TotalNumLMs() { return lm_channels.Count; }

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
                if (lm_bitmap[i] == 1) reply = lm_stubs[i].PrepareAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult();
                else reply = new Promise { Ack = false };
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded || ex.StatusCode == StatusCode.Unavailable)// || ex.StatusCode == StatusCode.Unknown)
            {
                reply = new Promise { Ack = false };
                Console.WriteLine("Erro ao contactar os outros LMs");
                lock (lm_bitmap)
                {
                    lm_bitmap[i] = 0;
                }
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
            for (int i = 0; i < lm_stubs.Count; i++)
            {
                Thread t = new Thread(() => { sendAccepted(ref values, i, request); });
                t.Start();
            }
            lock (this)
            {
                int alive = AliveLMs();
                while (!(values[0] + 1 >= (alive + 1) / 2 || values[1] >= (alive + 1) / 2)) Monitor.Wait(this);
                if (values[0] + 1 >= (alive + 1) / 2) return true;
                else return false;
            }
        }
        private void sendAccepted(ref List<int> values, int i, AcceptRequest request)
        {
            Console.WriteLine("NEW THREAD TO SEND ACCEPTED TO OTHERS");
            try
            {
                Console.WriteLine("tamanho: " + lm_bitmap.Count);
                Console.WriteLine("value: " + lm_bitmap[1]);
                if (lm_bitmap[i] == 1) //erro de index mas pq
                {
                    AcceptReply reply = lm_stubs[i].AcceptedAsync(request).GetAwaiter().GetResult();
                    lock (this)
                    {
                        if (reply.Ack) values[0]++;
                        else values[1]++;
                        Monitor.Pulse(this);
                    }

                    Console.WriteLine("ACCEPTED SENDED YES: " + values[0]);
                    Console.WriteLine("ACCEPTED SENDED NO: " + values[1]);
                }
                else { Console.WriteLine("I think the LM: "+ i + " is dead!"); }
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Unavailable || ex.StatusCode == StatusCode.DeadlineExceeded)
            {
                lm_bitmap[i] = 0;
            }
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
            //foreach (Grpc.Core.AsyncUnaryCall<AcceptReply> reply in replies)
            for (int i = 0; i < replies.Count; i++)
            {
                try
                {
                    if (lm_bitmap[i] == 1)
                    {
                        replies[i].ResponseAsync.Wait();
                        if (replies[i].ResponseAsync.Result.Ack) acks++;
                    }
                }
                catch (RpcException ex) when (ex.StatusCode == StatusCode.Unavailable || ex.StatusCode == StatusCode.DeadlineExceeded)
                {
                    Console.WriteLine("Could not contact LM");
                    lm_bitmap[i] = 0;
                }
            }
            Console.WriteLine("ACKS :" + acks);
            Console.WriteLine("OUT");
            return acks > (AliveLMs() + 1) / 2; //em vez do count vai ser com o bitmap dos lms que estao vivos
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
                lm_bitmap[possible_leader] = 0;
            }
            return reply.Ack;
        }
    }
}