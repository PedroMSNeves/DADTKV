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
        List<string> lm_names;
        bool[] lm_bitmap;

        public LmContact(string name, List<string> lm_urls, List<string> l_names)
        {
            _name = name;
            foreach (string url in lm_urls) 
            { 
                lm_channels.Add(GrpcChannel.ForAddress(url));
            }
            lm_bitmap = new bool[lm_urls.Count];
            for (int i = 0; i < lm_bitmap.Length; i++) lm_bitmap[i] = true;
            lm_names = l_names;
        }
        public void CrashedServer(string name)
        {
            for (int i = 0; i < lm_names.Count; i++)
            {
                if (lm_names[i] == name)
                {
                    lm_bitmap[i] = false;
                    break;
                }
            }
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
        private int Majority()
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
            //if (AliveLMs() <= Majority()) return new Promise { Ack = false };
            try
            {
                Console.WriteLine("i: "+i);
                if (lm_bitmap[i])
                {
                    Console.WriteLine("A enviar msg a um gajo vivo! Fingers crossed");
                    reply = lm_stubs[i].PrepareAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(15))).GetAwaiter().GetResult();
                }
                else reply = new Promise { Ack = false };
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded || ex.StatusCode == StatusCode.Unavailable)// || ex.StatusCode == StatusCode.Unknown)
            {
                reply = new Promise { Ack = false };
                Console.WriteLine("Erro ao contactar os outros LMs");
                lm_bitmap[i] = false;
            }
            return reply;
        }
        public bool BroadAccepted(AcceptRequest request) //Used by the others to propagate accepted
        {
            List<Grpc.Core.AsyncUnaryCall<AcceptReply>> replies = new List<Grpc.Core.AsyncUnaryCall<AcceptReply>>();
            List<int> values = new List<int>();
            int responses = 0;
            int acks = 1;
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

            //if (AliveLMs() <= Majority()) return false;
            lock (this)
            {
                for (int i = 0; i < lm_names.Count; i++)
                {

                    if (lm_bitmap[i])
                    {
                        replies.Add(lm_stubs[i].AcceptedAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(10))));
                    }
                    else
                    {
                        replies.Add(null);
                        responses++;
                    }
                }

                Random rd = new Random();
                while (responses < lm_stubs.Count)
                {
                    Monitor.Wait(this, rd.Next(100, 150));
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
                                if (acks > Majority()) return true;
                            }
                        }
                    }
                }
            }
            return acks > Majority();
        }
        public bool AcceptRequest(AcceptRequest request) //Used by the leaders to propagate accept 
        {
            Console.WriteLine("IN");
            List<Grpc.Core.AsyncUnaryCall<AcceptReply>> replies = new List<Grpc.Core.AsyncUnaryCall<AcceptReply>>();
            int acks = 1;
            int responses = 0;

            for (int i = 0; i < lm_bitmap.Length; i++)
            {
                if (lm_bitmap[i])
                {
                    replies.Add(lm_stubs[i].AcceptAsync(request));
                }
                else
                {
                    replies.Add(null);
                    responses++;
                }
            }
            while (responses < lm_stubs.Count)
            {
                //Monitor.Wait(st, rd.Next(100, 150));
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
                        }
                    }
                }
            }
            Console.WriteLine("ACKS :" + acks);
            Console.WriteLine("OUT");
            return acks > (AliveLMs() + 1) / 2; //em vez do count vai ser com o bitmap dos lms que estao vivos
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