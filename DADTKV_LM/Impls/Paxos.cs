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

        public Paxos(string name, LeaseData data, List<string> tm_urls, List<string> lm_urls, List<string> tm_names, List<string> lm_names)
        {
            Name = name;
            _data = data;
            //When paxos is called needs to select the requests that don´t conflict to define our value
            My_value = _data.GetMyValue(); //quando é que fazemos isto?
            _tmContact = new TmContact(tm_urls, tm_names);
            _lmcontact = new LmContact(name, lm_urls, lm_names);
        }
        public string Name { get; }
        public List<Request> My_value { set; get; }


        public override Task<Promise> Prepare(PrepareRequest request, ServerCallContext context)
        {
            return Task.FromResult(Prep(request));
        }
        /// <summary>
        /// Reply to a Prepare msg
        /// </summary>
        public Promise Prep(PrepareRequest request) 
        {
            Promise reply;
            Console.WriteLine("RECEBI PREPARE");
            Console.WriteLine("leader_id: "+request.LeaderId);
            Console.WriteLine("epoch: "+request.Epoch);

            lock (_data)
            {
                _data.Epoch = request.Epoch;
                int epoch = request.Epoch;
                Console.Write("Round ID leader: " +request.RoundId);
                Console.WriteLine(" ;my readTs: "+ _data.GetReadTS(epoch));

                if (_data.IsLeader && request.RoundId > _data.RoundID)
                {
                    _data.IsLeader = false;
                }
                if (request.RoundId <= _data.GetReadTS(epoch))
                {
                    reply = new Promise { Ack = false };
                    return reply;
                }

                _data.SetReadTS(epoch, request.RoundId);
                _data.Possible_Leader = request.LeaderId;

                reply = new Promise { WriteTs = _data.GetWriteTS(epoch), Ack = true, Epoch = epoch };

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
        /// <summary>
        /// Reply to a Accept msg
        /// Accepts the value if its the last it read and
        /// If the broadcast is also accepted by a mojority 
        /// </summary>
        public AcceptReply Accpt(AcceptRequest request)
        {
            AcceptReply reply = new AcceptReply { Epoch = request.Epoch };
            //_data.Epoch = request.Epoch;

            foreach (LeasePaxos s in request.Leases)foreach(string key  in s.Keys) Console.WriteLine(key);
            if (request.WriteTs < _data.GetReadTS(request.Epoch)) // precisa de lock
            {
                reply.Ack = false;
                return reply;
            }

            bool result = _lmcontact.BroadAccepted(request);
            Console.WriteLine("/////////////////////////////////////////// "+result);
            _data.Possible_Leader = _lmcontact.CheckPossibleLeader(_data.Possible_Leader);
            
            lock (this)
            {
                int epoch = request.Epoch;

                if (_data.IsLeader && request.WriteTs > _data.RoundID)
                {
                    _data.IsLeader = false;
                }
                if (!result) reply.Ack = false;
                else
                {
                    if (request.WriteTs >= _data.GetReadTS(epoch)) // se isto nao acontecer e porque deu promise entretanto
                    {
                        Console.WriteLine("Vou aceitar o pedido");
                        reply.Ack = true;
                        My_value.Clear();
                        foreach (LeasePaxos l in request.Leases)
                        {
                            My_value.Add(new Request(l.Tm, l.Keys.ToList(), l.LeaseId));
                        }
                        _data.Possible_Leader = request.LeaderId;
                        //Sends the Paxos result to all TMs
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
        /// <summary>
        /// Reply to a Accepted msg
        /// Returns ack with true if the value to be accepted is the last value we've seen 
        /// </summary>
        public AcceptReply Accpted(AcceptRequest request)
        {
            lock (this)
            {
                //_data.Epoch = request.Epoch;
                Console.WriteLine("RECEBI ACCEPTED");
                AcceptReply reply = new AcceptReply { Epoch = request.Epoch };
                Console.WriteLine("request writets: " + request.WriteTs);
                Console.WriteLine("ReadTs: " + _data.GetReadTS(request.Epoch));
                if (request.WriteTs != _data.GetReadTS(request.Epoch) && !_data.IsLeader)
                {
                    reply.Ack = false;
                    return reply;
                }
                reply.Ack = true;
                return reply;
            }
        }
        public override Task<AcceptReply> GetLeaderAck(AckRequest request, ServerCallContext context)
        {
            return Task.FromResult(LeaderAck(request));
        }
        /// <summary>
         /// Reply to a LeaderAck msg
         /// Returns ack with true if we are the leader
         /// </summary>
        public AcceptReply LeaderAck(AckRequest request)
        {
            AcceptReply reply = new AcceptReply { Epoch = request.Epoch };

            lock (_data)
            {
                if (_data.IsLeader ) reply.Ack = true;
                else reply.Ack = false;
            }
            return reply;
        }

    }
}
