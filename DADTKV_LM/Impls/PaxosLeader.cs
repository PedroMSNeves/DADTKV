using System;
using Grpc.Core;
using DADTKV_LM.Structs;
using DADTKV_LM.Contact;

namespace DADTKV_LM.Impls
{
    public class PaxosLeader
    {
        private LeaseData _data;
        LmContact _lmcontact;
        TmContact _tmContact;

        public PaxosLeader(string name, LeaseData data, int id, List<string> tm_urls, List<string> lm_urls)
        {
            Name = name;
            Others_value = new List<Request>();
            Other_TS = 0;
            _data = data;
            Id = id;
            Epoch = 0;
            My_value = _data.GetMyValue();
            _tmContact = new TmContact(tm_urls);
            _lmcontact = new LmContact(name, lm_urls);

        }
        public string Name { get; }
        public List<Request> My_value { set; get; }
        public List<Request> Others_value { set; get; }
        public int Other_TS { get; set; }
        public int Id { get; set; }
        public int Epoch { get; set; }

        public void cycle()
        {
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
                lock (this)
                {
                    while (_data.IsLeader)
                    {
                        My_value = _data.GetMyValue();
                        while (My_value.Count == 0)
                        {
                            Thread.Sleep(1000);
                            My_value = _data.GetMyValue();
                        }
                        Epoch = Epoch + 1; //implementar tempo
                        while (!PrepareRequest())
                        {
                            _data.RoundID = Other_TS + 1;//evitamos fazer muitas vezes inuteis
                        }
                        if (AcceptRequest())
                        {
                            _data.Write_TS = _data.RoundID;//evitamos fazer muitas vezes inuteis
                                                           //eliminar os requests

                            if (Other_TS > _data.Write_TS) _tmContact.BroadLease(Epoch, Others_value);
                            else _tmContact.BroadLease(Epoch, My_value);
                            _data.Write_TS = _data.RoundID;

                        }
                        //x em x tempo faz
                    }
                }
                possible_leader = Id;
            }
        }
        public bool PrepareRequest()
        {
            PrepareRequest request = new PrepareRequest { RoundId = _data.IncrementRoundID() };
            return _lmcontact.PrepareRequest(request, _data.Write_TS, Other_TS, Others_value);
        }

        public bool AcceptRequest()
        {
            AcceptRequest request = new AcceptRequest { WriteTs = _data.RoundID };
            LeasePaxos lp;
            //comparar os TS e ver qual vamos aceitar
            if (Other_TS > _data.Write_TS)
            {
                foreach (Request r in Others_value)
                {
                    lp = new LeasePaxos { Tm = r.Tm_name };
                    foreach (string k in r.Keys) { lp.Keys.Add(k); }
                    request.Leases.Add(lp);
                }
            }
            else
            {
                foreach (Request r in My_value)
                {
                    lp = new LeasePaxos { Tm = r.Tm_name };
                    foreach (string k in r.Keys) { lp.Keys.Add(k); }
                    request.Leases.Add(lp);
                }
            }
            request.LeaderId = Id;
            return _lmcontact.AcceptRequest(request);
           
        }
    }
}
