using System;
using Grpc.Core;
using DADTKV_LM.Structs;
using DADTKV_LM.Contact;
using Grpc.Net.Client;

namespace DADTKV_LM.Impls
{
    public class PaxosLeader
    {
        private LeaseData _data;
        LmContact _lmcontact;
        TmContact _tmContact;
        Dictionary<int, List<Request>> my_value;
        Dictionary<int, List<Request>> other_value;
        Dictionary<int, int> other_TS;


        public PaxosLeader(string name, LeaseData data, int id, List<string> tm_urls, List<string> lm_urls)
        {
            Name = name;
            _data = data;
            Id = id;

            _tmContact = new TmContact(tm_urls);
            _lmcontact = new LmContact(name, lm_urls);

            my_value = new Dictionary<int, List<Request>>();
            other_value = new Dictionary<int, List<Request>>();
            other_TS = new Dictionary<int, int>();

        }
        public string Name { get; }
        public List<Request> GetMyValue(int epoch){ return my_value[epoch]; }
        public void SetMyValue(int epoch, List<Request> req)
        {
            lock (this) {
                if (my_value.ContainsKey(epoch))
                {
                    my_value[epoch].Clear();
                    my_value[epoch].AddRange(req);
                }
                else
                {
                    my_value.Add(epoch, req);
                }
            }
        }
        public List<Request> GetOtherValue(int epoch) 
        {
            List<Request> res;
            lock (this)
            {
                res = new List<Request>();
                foreach (Request req in other_value[epoch]) { res.Add(req); }
            }
            return res; 
        }
        public void SetOtherValue(int epoch, List<Request> req)
        {
            lock (this)
            {
                if (other_value.ContainsKey(epoch))
                {
                    other_value[epoch].Clear();
                    other_value[epoch].AddRange(req);
                }
                else
                {
                    other_value.Add(epoch, req);
                }
            }
        }
        public int GetOtherTS(int epoch)
        {
            lock (this)
            {
                if (other_TS.ContainsKey(epoch)) return other_TS[epoch];
                else return 0;
            }
        }
        public void SetOtherTS(int epoch, int val) { lock (this) { other_TS[epoch] = val; } }
        public int Id { get; set; }

        /// <summary>
        /// LMs wait until they are leaders
        /// </summary>
        public void cycle(int timeSlotDuration, int numTimeSlots)
        {
            _data.Epoch = 1;
            bool ack = true;
            int possible_leader = 0;

            while (true) //wait until you are the leader
            {
                //Console.WriteLine("Possible Leader: " + possible_leader);
                //Console.WriteLine("Id: " + Id);
                while (Id == possible_leader + 1 && ack) //if we are the next leader
                {
                    Thread.Sleep(5000); //dorme 5 sec e depois manda mensagem
                    //lock (_data)
                    //{
                        possible_leader = _data.Possible_Leader;
                        if (Id == possible_leader + 1) // depois ver tambem se é o seguinte no bitmap que esteja vivo
                        {
                            ack = _lmcontact.getLeaderAck(_data.Possible_Leader); //Contact the leader to see if he is alive

                            if (!ack)
                            {
                                possible_leader++;
                                _data.IsLeader = true;
                            }
                        }
                   // }
                }
                possible_leader = _data.Possible_Leader;
                if (_data.IsLeader) break;
            }
            for (int i = 0; i < numTimeSlots; i++)
            {
                Console.WriteLine("New Paxos Instance");
                int epoch = _data.Epoch;
                _data.Epoch++;
                Thread t = new Thread(() => RunPaxosInstance(epoch));
                t.Start();               
                Thread.Sleep(timeSlotDuration);
                //t.Join();
                _data.RoundID++;
            }
        }
        public void RunPaxosInstance(int epoch)
        {
            int round_id;

            SetMyValue(epoch, _data.GetMyValue());

            while (true) {

                while (GetMyValue(epoch).Count == 0) //nao ter este while e mandar o paxos vazio?
                {
                    Thread.Sleep(1000);
                    SetMyValue(epoch, _data.GetMyValue());
                    Console.WriteLine("QUERO MAIS PEDIDOS");
                }

                while (!PrepareRequest(epoch))
                {
                    _data.RoundID = GetOtherTS(epoch) + 1;//evitamos fazer muitas vezes inuteis
                }
                round_id = _data.RoundID;

                Console.WriteLine("PREPARE DONE");
                if (AcceptRequest(epoch, round_id))
                {
                    Console.WriteLine("ACCEPT DONE");
                    _data.SetWriteTS(epoch, round_id);

                    if (other_TS[epoch] > _data.GetWriteTS(epoch))
                    {
                        if (_tmContact.BroadLease(epoch, other_value[epoch])) 
                        {
                            Console.WriteLine("BROADCASTED");
                            _data.SetWriteTS(epoch, round_id);
                            break;  
                        }
                    }
                    else
                    {
                        if (_tmContact.BroadLease(epoch, my_value[epoch]))
                        {
                            Console.WriteLine("BROADCASTED");
                            _data.SetWriteTS(epoch, round_id);
                            break;
                        }
                    }
                }
            }
            //_data.RoundID++;
        }
        public bool PrepareRequest(int epoch)
        {
            PrepareRequest request = new PrepareRequest { RoundId = _data.RoundID, Epoch = epoch };
            return PrepareRequest(request, epoch);
        }
        public bool PrepareRequest(PrepareRequest request, int epoch)
        {
            bool res;
            //lock (this)
           //{
                Promise reply;
                int promises = 1;
                int numLMs = _lmcontact.NumLMs();
                Console.WriteLine("num Stubs: "+ numLMs);
                
                for (int i = 0; i < numLMs; i++)
                {
                    reply = _lmcontact.PrepareRequest(request, i);
                    if (reply.Ack) promises++;

                    SetOtherTS(epoch, _data.GetWriteTS(epoch));
                    if (reply.WriteTs > GetOtherTS(epoch))
                    {
                        if (other_value.ContainsKey(epoch)) other_value[epoch].Clear();
                        foreach (LeasePaxos l in reply.Leases)
                        {
                            other_value[epoch].Add(new Request(l.Tm, l.Keys.ToList(), l.LeaseId));
                        }
                        SetOtherTS(epoch, reply.WriteTs);
                    }
                }
                res = promises > (numLMs + 1) / 2;
            Console.WriteLine("lider consegue prepare: "+res);
           // }
            return res;
        }
        public bool AcceptRequest(int epoch, int round_id)
        {
            Console.WriteLine("ACCEPT REQUEST");
            Console.WriteLine("With: "+ round_id);
            AcceptRequest request = new AcceptRequest { WriteTs = round_id, Epoch = epoch };
            LeasePaxos lp;

            if (GetOtherTS(epoch) > _data.GetWriteTS(epoch))
            {
                foreach (Request r in GetOtherValue(epoch))
                {
                    lp = new LeasePaxos { Tm = r.Tm_name, LeaseId = r.Lease_ID };
                    foreach (string k in r.Keys) { lp.Keys.Add(k); }
                    request.Leases.Add(lp);
                }
            }
            else
            {
                foreach (Request r in GetMyValue(epoch))
                {
                    lp = new LeasePaxos { Tm = r.Tm_name, LeaseId = r.Lease_ID };
                    foreach (string k in r.Keys) { lp.Keys.Add(k); }
                    request.Leases.Add(lp);
                }
            }
            request.LeaderId = Id;
            return _lmcontact.AcceptRequest(request);

        }
    }
}
