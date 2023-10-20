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

            Epoch = 0; //mudar este, provavelmente vai sair daqui
            _tmContact = new TmContact(tm_urls);
            _lmcontact = new LmContact(name, lm_urls);

            my_value = new Dictionary<int, List<Request>>();
            other_value = new Dictionary<int, List<Request>>();
            other_TS = new Dictionary<int, int>();

        }
        public string Name { get; }
        public List<Request> GetMyValue(int epoch)
        {
            return my_value[epoch];
        }
        public void SetMyValue(int epoch, List<Request> req)
        {
            my_value[epoch].Clear();
            my_value[epoch].AddRange(req);
        }
        public List<Request> GetOtherValue(int epoch)
        {
            return other_value[epoch];
        }
        public void SetOtherValue(int epoch, List<Request> req)
        {
            other_value[epoch].Clear();
            other_value[epoch].AddRange(req);
        }
        public int GetOtherTS(int epoch)
        {
            return other_TS[epoch];
        }
        public void SetOtherTS(int epoch, int val)
        {
            other_TS[epoch] = val;
        }
        public int Id { get; set; }
        public int Epoch { get; set; }

        public void cycle(int timeSlotDuration, int numTimeSlots)
        {
            int epoch = 1;
            bool ack = true;
            int possible_leader = -1;

            while (true)
            {
                while (Id != possible_leader + 1 && ack)
                {
                    Thread.Sleep(5000); //dorme 5 sec e depois manda mensagem
                    lock (_data)
                    {
                        possible_leader = _data.Possible_Leader;
                        if (Id == possible_leader + 1) // depois ver tambem se é o seguinte no bitmap que esteja vivo
                        {
                            ack = _lmcontact.getLeaderAck(_data.Possible_Leader);
                        }
                    }
                }

                for (int i = 0; i < numTimeSlots; i++)
                {
                    Thread t = new Thread(() => RunPaxosInstance(epoch));
                    t.Start();
                    Thread.Sleep(timeSlotDuration);
                    epoch++;
                }
            }
        }
        public void RunPaxosInstance(int epoch)
        {
            lock (this)
            {
                while (_data.IsLeader)
                {
                    SetMyValue(epoch, _data.GetMyValue());

                    while (GetMyValue(epoch).Count == 0)
                    {
                        Thread.Sleep(1000);
                        SetMyValue(epoch, _data.GetMyValue());
                    }
                    Epoch++;//implementar tempo, isto vai sair daqui
                    while (!PrepareRequest(epoch))
                    {
                        _data.RoundID = GetOtherTS(epoch) + 1;//evitamos fazer muitas vezes inuteis
                    }
                    if (AcceptRequest(epoch))
                    {
                        _data.SetWriteTS(epoch, _data.RoundID);
                        //_data.Write_TS = _data.RoundID;//evitamos fazer muitas vezes inuteis
                        //eliminar os requests

                        if (other_TS[epoch] > _data.GetWriteTS(epoch)) _tmContact.BroadLease(Epoch, other_value[epoch]); //ver se correu bem, se não correu bem não aumenta a epoch?
                        else _tmContact.BroadLease(epoch, my_value[epoch]);
                        _data.SetWriteTS(epoch, _data.RoundID);

                    }
                    //x em x tempo faz
                }
            }
        }
        public bool PrepareRequest(int epoch)
        {
            PrepareRequest request = new PrepareRequest { RoundId = _data.RoundID++, Epoch = Epoch };
            return PrepareRequest(request, epoch);
        }
        public bool PrepareRequest(PrepareRequest request, int epoch)
        {
            bool res;
            lock (this)
            {
                Promise reply;
                int promises = 1;
                for (int i = 0; i < _lmcontact.lm_stubs.Count; i++)
                {
                    reply = _lmcontact.PrepareRequest(request, i);
                    if (reply.Ack) promises++;

                    SetOtherTS(epoch, _data.GetWriteTS(epoch));
                    if (reply.WriteTs > GetOtherTS(epoch))
                    {
                        foreach (LeasePaxos l in reply.Leases)
                        {
                            other_value[epoch].Add(new Request(l.Tm, l.Keys.ToList(), l.LeaseId));
                        }
                        SetOtherTS(epoch, reply.WriteTs);
                    }
                }
                res = promises > (_lmcontact.lm_stubs.Count + 1) / 2;
            }
            return res;
        }

        public bool AcceptRequest(int epoch)
        {
            AcceptRequest request = new AcceptRequest { WriteTs = _data.RoundID };
            LeasePaxos lp;
            //comparar os TS e ver qual vamos aceitar
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
