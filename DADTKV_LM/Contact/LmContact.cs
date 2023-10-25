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
        bool[] lm_bitmap;

        public LmContact(string name, List<string> lm_urls)
        {
            _name = name;
            foreach (string url in lm_urls) 
            { 
                lm_channels.Add(GrpcChannel.ForAddress(url));
            }
            lm_bitmap = new bool[lm_urls.Count];
            for (int i = 0; i < lm_bitmap.Length; i++) lm_bitmap[i] = true;
        }
        public int CheckPossibleLeader(int possibleLeader)
        {
            for (int i = 0; i < lm_bitmap.Length; i++)
            {
                if (i < possibleLeader)
                {
                    lm_bitmap[i] = false; //em vez de assumri que morreu logo podiamos primeiro contactar com ele
                    continue;
                }
                if (i == possibleLeader && lm_bitmap[i]) return possibleLeader;
                if (lm_bitmap[i])
                {
                    return i;
                }
            }
            return -1;
        }
        private int Majority() //aqui contar os que estão alive
        {
            return (int)Math.Floor((decimal)((lm_stubs.Count + 1) / 2));
        }
        public int AliveLMs() 
        {
            lock (lm_bitmap)
            {
                int n = 0;
                foreach (bool lm in lm_bitmap)
                {
                    if (lm) n++;
                }
                return n;
            }
        }
        public int NumLMs() { return lm_channels.Count; }
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
            if (AliveLMs() <= Majority()) return new Promise { Ack = false };
            try
            {
                Console.WriteLine("i: "+i);
                if (lm_bitmap[i]) reply = lm_stubs[i].PrepareAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult();
                else reply = new Promise { Ack = false };
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded || ex.StatusCode == StatusCode.Unavailable)// || ex.StatusCode == StatusCode.Unknown)
            {
                reply = new Promise { Ack = false };
                Console.WriteLine("Erro ao contactar os outros LMs");
                lock (lm_bitmap)
                {
                    lm_bitmap[i] = false;
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

            if (AliveLMs() <= Majority()) return false;

            
            lock (this)
            {
                for (int i = 0; i < lm_bitmap.Length; i++)
                {
                    Console.WriteLine("Como assim: " + i);
                    int j = i;
                    Thread t = new Thread(() => { sendAccepted(ref values, j, request); });
                    t.Start();
                }
                while (!(values[0] + 1 >= (NumLMs() + 1) / 2 || values[1] >= (NumLMs() + 1) / 2)) Monitor.Wait(this);
                if (values[0] + 1 >= (NumLMs() + 1) / 2) return true;
                else return false;
            }
        }
        private void sendAccepted(ref List<int> values, int i, AcceptRequest request)
        {
            Console.WriteLine("NEW THREAD TO SEND ACCEPTED TO OTHERS");
            try
            {
                Console.WriteLine("tamanho: " + lm_bitmap.Length);
                Console.WriteLine("index: " + i);

                if (lm_bitmap[i]) //erro de index mas pq
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
                else 
                {
                    Console.WriteLine("I think the LM: "+ i + " is dead!");
                    values[1]++;
                }
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Unavailable || ex.StatusCode == StatusCode.DeadlineExceeded)
            {
                lm_bitmap[i] = false;
                values[1]++;
            }
        }
        public bool AcceptRequest(AcceptRequest request) //Used by the leaders to propagate accept 
        {
            Console.WriteLine("IN");
            List<Grpc.Core.AsyncUnaryCall<AcceptReply>> replies = new List<Grpc.Core.AsyncUnaryCall<AcceptReply>>();
            int acks = 1;
            int responses = 0;

            for (int i = 0; i < lm_stubs.Count; i++)
            {
                if (lm_bitmap[i])
                {
                    replies.Add(lm_stubs[i].AcceptAsync(request));
                }
                else replies.Add(null);
            }
            while (responses < lm_stubs.Count && acks <= Majority())
            {
                for (int i = 0; i < replies.Count; i++)
                {
                    if (replies[i] != null)
                    {
                        if (replies[i].ResponseAsync.IsCompleted)
                        {
                            if (replies[i].ResponseAsync.IsFaulted)
                            {
                                lm_bitmap[i] = false;
                            }
                            else if (replies[i].ResponseAsync.Result.Ack == true) acks++;
                            responses++;
                            replies[i] = null;
                            if (acks > Majority()) break;
                        }
                    }
                }
            }

            Console.WriteLine("ACKS :" + acks);
            Console.WriteLine("OUT");
            return acks > Majority(); //em vez do count vai ser com o bitmap dos lms que estao vivos
        }
       
        public bool getLeaderAck(int possible_leader)
        {
            if (lm_stubs == null)
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
                lm_bitmap[possible_leader] = false;
            }
            return reply.Ack;
        }
    }
}