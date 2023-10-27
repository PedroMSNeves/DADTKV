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
        private string _name;
        LmContact _lmcontact;
        TmContact _tmContact;
        Dictionary<int, List<Request>> my_value;
        Dictionary<int, List<Request>> other_value;
        Dictionary<int, int> other_TS;
        public Dictionary<int, List<string>> _crashed;
        int _myCrashEpoch;

        public PaxosLeader(string name, LeaseData data, int id, int crash_ts, TmContact tmContact, LmContact lmContact, Dictionary<int, List<string>> crashedP)
        {
            _name = name;
            _data = data;
            Id = id;
            _myCrashEpoch = crash_ts;

            _tmContact = tmContact;
            _lmcontact = lmContact;

            my_value = new Dictionary<int, List<Request>>();
            other_value = new Dictionary<int, List<Request>>();
            other_TS = new Dictionary<int, int>();
            _crashed = crashedP;
        }
        public List<Request> GetMyValue(int epoch){ return my_value[epoch]; }
        public void SetMyValue(int epoch, List<Request> req)
        {
            //lock (this) {
                if (my_value.ContainsKey(epoch))
                {
                    my_value[epoch].Clear();
                    my_value[epoch].AddRange(req);
                }
                else
                {
                    my_value.Add(epoch, req);
                }
           // }
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

        public void CrashedServer(string name)
        {
            _tmContact.CrashedServer(name);
            //_lmcontact.CrashedServer(name);
        }

        /// <summary>
        /// LMs wait until they are leaders
        /// </summary>
        public void cycle(int timeSlotDuration, int numTimeSlots)
        {
            _data.Epoch = 0;
            bool ack = true;
            int possible_leader = 0;
            int epoch;

            while (true) //wait until you are the leader
            {
                //Console.WriteLine("Possible Leader: " + possible_leader);
                //Console.WriteLine("Id: " + Id);
                
                if (Id == possible_leader + 1 && ack) //if we are the next leader
                {
                    Thread.Sleep(5000); //dorme 5 sec e depois manda mensagem
                    
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
                }
                possible_leader = _data.Possible_Leader;
                if (_data.IsLeader) break;

                epoch = _data.Epoch;
                for (int i = 1; i <= epoch; i++)
                {
                    if (_myCrashEpoch != -1 && i >= _myCrashEpoch) return;
                    if (_crashed.ContainsKey(i))
                    {
                        foreach (string name in _crashed[i])
                        {
                            CrashedServer(name);
                        }
                    }
                }
            }
            for (int i = 0; i < numTimeSlots; i++)
            {
                Console.WriteLine("New Paxos Instance");
                _data.Epoch++;

                epoch = _data.Epoch;          

                for (int j = 1; j <= epoch; j++)
                {
                    if (_myCrashEpoch != -1 && j >= _myCrashEpoch) return; 

                    if (_crashed.ContainsKey(j))
                    {
                        foreach (string name in _crashed[j])
                        {
                            CrashedServer(name);
                        }
                    }
                }
                Console.WriteLine("epoch: "+ epoch);
                Thread t = new Thread(() => RunPaxosInstance(epoch));
                t.Start();               
                Thread.Sleep(timeSlotDuration);
                _data.RoundID++;
            }
        }
        public void RunPaxosInstance(int epoch)
        {
            int round_id;

            lock (my_value)
            {
                SetMyValue(epoch, _data.GetMyValue());
                while (GetMyValue(epoch).Count == 0) //nao ter este while e mandar o paxos vazio?
                {
                    Thread.Sleep(1000);
                    SetMyValue(epoch, _data.GetMyValue());
                    Console.WriteLine("QUERO MAIS PEDIDOS");
                }
            }

            while (true) {
                
                while (!PrepareRequest(epoch))
                {
                    //_data.RoundID = GetOtherTS(epoch) + 1;//evitamos fazer muitas vezes inuteis
                    _data.RoundID = _data.RoundID + 10;
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
            Console.WriteLine("ROundID: "+_data.RoundID);
            _data.SetReadTS(epoch, _data.RoundID);
            PrepareRequest request = new PrepareRequest { RoundId = _data.RoundID, Epoch = epoch };
            return PrepareRequest(request, epoch);
        }
        public bool PrepareRequest(PrepareRequest request, int epoch)
        {
            bool res;

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
            res = promises > (_lmcontact.AliveLMs() + 1) / 2;
            Console.WriteLine("lider consegue prepare: "+res);
            return res;
        }
        public bool AcceptRequest(int epoch, int round_id)
        {
            Console.Write("ACCEPT REQUEST ");
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
