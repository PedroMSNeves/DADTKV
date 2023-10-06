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
            Promise reply;
            int promises = 0;
            foreach (PaxosService.PaxosServiceClient stub in _lmcontact.lm_stubs)
            {
                reply = stub.PrepareAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult(); // tirar isto de syncrono
                if (reply.Ack) promises++;

                Other_TS = _data.Write_TS;
                if (reply.WriteTs > Other_TS)
                {
                    foreach (LeasePaxos l in reply.Leases)
                    {
                        Others_value.Add(new Request(l.Tm, l.Keys.ToList()));
                    }
                    Other_TS = reply.WriteTs;
                }
            }
            return promises > (_lmcontact.lm_stubs.Count + 1) / 2; //true se for maioria, removemos da lista se morrerem?
        }

        public bool AcceptRequest()
        {
            AcceptReply reply;
            AcceptRequest request = new AcceptRequest { WriteTs = _data.RoundID };
            LeasePaxos lp;
            int acks = 0;
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
            foreach (PaxosService.PaxosServiceClient stub in _lmcontact.lm_stubs)
            {
                reply = stub.AcceptAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult(); // tirar isto de syncrono
                if (reply.Ack) acks++;
            }
            return acks > (_lmcontact.lm_stubs.Count + 1) / 2; //em vez do count vai ser com o bitmap dos lms que estao vivos
        }
    }
}
