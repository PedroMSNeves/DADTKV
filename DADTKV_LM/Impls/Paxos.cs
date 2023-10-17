using Grpc.Core;
using Grpc.Net.Client;
using DADTKV_LM.Contact;
using DADTKV_LM.Structs;


namespace DADTKV_LM.Impls
{
    public class Paxos : PaxosService.PaxosServiceBase
    {
        private LeaseData _data;
        LmContact _lmcontact;
        TmContact _tmContact;
        int epoch = -1;

        public Paxos(string name, LeaseData data, List<string> tm_urls, List<string> lm_urls)
        {
            Name = name;
            _data = data;
            //When paxos is called needs to select the requests that don´t conflict to define our value
            My_value = _data.GetMyValue();
            _tmContact = new TmContact(tm_urls);
            _lmcontact = new LmContact(name, lm_urls);
        }
        public string Name { get; }
        public List<Request> My_value { set; get; }


        public override Task<Promise> Prepare(PrepareRequest request, ServerCallContext context)
        {
            return Task.FromResult(Prep(request));
        }
        public Promise Prep(PrepareRequest request) //returns promise if roundId >  my readTS
        {
            Promise reply;
            Console.WriteLine(request.LeaderId);

            epoch = request.Epoch;

            lock (_data)
            {
                if (_data.IsLeader && request.RoundId > _data.RoundID)
                {
                    _data.IsLeader = false;
                }
                if (request.RoundId <= _data.Read_TS)
                {
                    reply = new Promise { Ack = false };
                    return reply;
                }
                _data.Read_TS = request.RoundId;
                _data.Possible_Leader = request.LeaderId;
                reply = new Promise { WriteTs = _data.Write_TS, Ack = true };
                foreach (Request r in My_value)
                {
                    LeasePaxos lp = new LeasePaxos { Tm = r.Tm_name, LeaseId = r.Lease_ID };
                    foreach (string k in r.Keys) { lp.Keys.Add(k); }
                    reply.Leases.Add(lp);
                }
            }
            return reply;
        }

        public override Task<AcceptReply> Accept(AcceptRequest request, ServerCallContext context)
        {
            return Task.FromResult(Accpt(request));
        }
        public AcceptReply Accpt(AcceptRequest request) //quandp recebe Accept e aceita, manda Accepted para todos os outros learners
        {
            AcceptReply reply = new AcceptReply();
            Console.WriteLine(request.LeaderId);
            foreach (LeasePaxos s in request.Leases)foreach(string key  in s.Keys) Console.WriteLine(key);
            if (request.WriteTs < _data.Read_TS) // precisa de lock
            {
                reply.Ack = false;
                return reply;
            }
            epoch = request.Epoch;

            //broadcast accept
            bool result = _lmcontact.BroadAccepted(request);
            //só aplica o valor recebido depois de receber uma maioria de accepted dos outros, falta essa msg no proto
            lock (this)
            {
                if (_data.IsLeader && request.WriteTs > _data.RoundID)
                {
                    _data.IsLeader = false;
                }
                if (!result) reply.Ack = false;
                else
                {
                    if (request.WriteTs == _data.Read_TS) // se isto nao acontecer e porque deu promise entretanto
                    {
                        reply.Ack = true;
                        foreach (LeasePaxos l in request.Leases)
                        {
                            My_value.Add(new Request(l.Tm, l.Keys.ToList(), l.LeaseId));
                        }
                        _data.Possible_Leader = request.LeaderId;
                        //Depois de alterar o seu valor envia resposta do Paxos para os TMs
                        _tmContact.BroadLease(epoch, My_value);
                    }
                    else reply.Ack = false;
                }

            }
            return reply;
        }
        public override Task<AcceptReply> Accepted(AcceptRequest request, ServerCallContext context)
        {
            return Task.FromResult(Accpted(request));
        }
        public AcceptReply Accpted(AcceptRequest request) //quandp recebe Accept e aceita, manda Accepted para todos os outros learners
        {
            AcceptReply reply = new AcceptReply();
            if (request.WriteTs != _data.Read_TS)
            {
                reply.Ack = false;
                return reply;
            }
            reply.Ack = true;
            return reply;
        }
        public override Task<AcceptReply> GetLeaderAck(AckRequest request, ServerCallContext context)
        {
            return Task.FromResult(LeaderAck());
        }
        public AcceptReply LeaderAck() //returns promise if roundId >  my readTS
        {
            AcceptReply reply = new AcceptReply();

            lock (_data)
            {
                if (_data.IsLeader ) reply.Ack = true;
                else reply.Ack = false;
            }
            return reply;
        }

    }
}
