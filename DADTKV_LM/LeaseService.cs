using Grpc.Core;
using System;
using System.Collections;
using System.Threading.Channels;
using Grpc.Net.Client;
using System.Globalization;
using System.Xml.Linq;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Google.Protobuf.Collections;

namespace DADKTV_LM
{
    public class Request //Lease basically
    {
        public Request(string tm_name, List<string> keys, int leaseId)
        {
            Tm_name = tm_name;
            Keys = keys;
            LeaseID = leaseId;

        }
        public string Tm_name { get; }
        public List<string> Keys { get; }
        public int LeaseID { get; }
    }


    public class LeaseData
    {   /* se um lm morrer podemos ter bitmap dos lm vivos */
        List<Request> _requests = new List<Request>(); //requests received from TMs

        public LeaseData(bool leader) 
        {
            RoundID = 0;
            Read_TS = 0;
            Write_TS = 0;
            Possible_Leader = 0;
            IsLeader = leader;
        }

        public int RoundID { set; get; }
        public int IncrementRoundID()
        {
            return ++RoundID;
        }
        public int Read_TS { get; set; }
        public int Write_TS { get; set; }
        public int Possible_Leader { get; set; }
        public bool IsLeader { get; set; } 
        public void AddRequest(Request request) //adds in the end
        {
            lock (this)
            {
                _requests.Add(request);
            }
        }
        public void AddRequestBeginning(Request request) //nao ness
        {
            lock (this)
            {
                _requests.Insert(0, request);
            }
        }
        public bool RemoveRequest() //removes from beginning //nao ness
        {
            lock (this)
            {
                if (_requests.Count == 0) return false;
                _requests.RemoveAt(0);
                return true;
            }
        }
        public bool Intersect(List<string> l1, List<string> l2)
        {
            return l1.Intersect(l2).Any();
        }
        public List<Request> GetMyValue()
        {
            lock (this)
            {
                List<string> keys = new List<string>();
                List<Request> myValue = new List<Request>();
                foreach(Request request in _requests)
                {
                    if (Intersect(keys, request.Keys)) continue;
                    myValue.Add(request);
                    keys.AddRange(request.Keys);
                }
                    
                return myValue;
            }
        }
    }

    public class LeageManager : LeaseService.LeaseServiceBase
    {
        private string _name;
        private LeaseData _data;
        LmContact _lmcontact;
        TmContact _tmContact;
        public LeageManager(string name, LeaseData data, List<string> tm_urls, List<string> lm_urls)
        {
            _name = name;
            _data = data;
            _tmContact = new TmContact(tm_urls);
            _lmcontact = new LmContact(name, lm_urls);
        }

        public override Task<LeaseReply> Lease(LeaseRequest request, ServerCallContext context)
        {
            return Task.FromResult(Ls(request));
        }
        public LeaseReply Ls(LeaseRequest request)
        {
            _data.AddRequest(new Request(request.Id, request.Keys.ToList(), request.LeaseRequestId));
            return new LeaseReply { Ack = true };
        }

        public void BroadcastLease(int epoch, List<Request> leases)
        {
            _tmContact.BroadLease(epoch, leases);
        }
    }


    public class Paxos : PaxosService.PaxosServiceBase
    {
        private LeaseData _data;
        LmContact _lmcontact;
        TmContact _tmContact;

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
                    LeasePaxos lp = new LeasePaxos { Tm = r.Tm_name, LeaseRequestId = r.LeaseID };
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
            if (request.WriteTs < _data.Read_TS) // precisa de lock
            {
                reply.Ack = false;
                return reply;
            }

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
                            My_value.Add(new Request(l.Tm, l.Keys.ToList(), l.LeaseRequestId));
                        }
                        _data.Possible_Leader = request.LeaderId;
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
    }

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
                            _tmContact.BroadLease(Epoch, _data.GetMyValue());
                                
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
                        Others_value.Add(new Request(l.Tm, l.Keys.ToList(), l.LeaseRequestId));
                    }
                    Other_TS = reply.WriteTs;
                }
            }
            return promises > (_lmcontact.lm_stubs.Count + 1) / 2; //true se for maioria, removemos da lista se morrerem?
        }

        public bool AcceptRequest()
        {
            AcceptReply reply;
            AcceptRequest request = new AcceptRequest { WriteTs = _data.RoundID};
            LeasePaxos lp;
            int acks = 0;
            //comparar os TS e ver qual vamos aceitar
            if (Other_TS > _data.Write_TS)
            {
                foreach (Request r in Others_value)
                {
                    lp = new LeasePaxos { Tm = r.Tm_name, LeaseRequestId = r.LeaseID };
                    foreach (string k in r.Keys) { lp.Keys.Add(k); }
                    request.Leases.Add(lp);
                }
            }
            else
            {
                foreach (Request r in My_value)
                {
                    lp = new LeasePaxos { Tm = r.Tm_name, LeaseRequestId = r.LeaseID };
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

    public class TmContact
    {
        List<LeaseService.LeaseServiceClient> tm_stubs = new List<LeaseService.LeaseServiceClient>();
        public TmContact(List<string> tm_urls)
        {
            foreach (string url in tm_urls) tm_stubs.Add(new LeaseService.LeaseServiceClient(GrpcChannel.ForAddress(url)));
        }
        public void BroadLease(int epoch, List<Request> leases)
        {
            LeaseReply reply;
            LeaseBroadCastRequest request = new LeaseBroadCastRequest { Epoch = epoch }; //cria request
            //request.Leases.AddRange(leases);
            foreach (Request r in leases)
            {
                LeaseProto lp = new LeaseProto { Tm = r.Tm_name };
                foreach (string k in r.Keys) { lp.Keys.Add(k); }
                request.Leases.Add(lp);
            }

            foreach (LeaseService.LeaseServiceClient stub in tm_stubs)
            {
                reply = stub.LeaseBroadCastAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult(); // tirar isto de syncrono
            }
        }
    }

    public class LmContact
    {
        private string _name;
        public List<PaxosService.PaxosServiceClient> lm_stubs = new List<PaxosService.PaxosServiceClient>();
        public LmContact(string name, List<string> lm_urls)
        {
            _name = name;
            foreach (string url in lm_urls) lm_stubs.Add(new PaxosService.PaxosServiceClient(GrpcChannel.ForAddress(url)));
        }
        public bool BroadAccepted(AcceptRequest request)
        {
            List<int> values = new List<int>();
            values.Add(0);//ack counter
            values.Add(0);//nack counter
            foreach (PaxosService.PaxosServiceClient stub in lm_stubs)
            {
                Thread t = new Thread(() => { sendAccepted(ref values, stub, request); });
                t.Start();
            }
            lock (values)
            {
                while (!(values[0] + 1 >= (lm_stubs.Count + 1) / 2 || values[1] >= (lm_stubs.Count + 1) / 2)) Monitor.Wait(this);
                if (values[0] + 1 >= (lm_stubs.Count + 1) / 2) return true;
                else return false;
            }
        }
        private void sendAccepted(ref List<int> values, PaxosService.PaxosServiceClient stub, AcceptRequest request)
        {
            AcceptReply reply = stub.AcceptedAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult();
            lock (values)
            {
                if (reply.Ack) values[0]++;
                else values[1]++;
                Monitor.Pulse(this);
            }
        }
        public bool getLeaderAck(int possible_leader)
        {
            return lm_stubs[possible_leader].GetLeaderAckAsync(new AckRequest(), new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult().Ack;
        }
    }
}