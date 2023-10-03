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
    {
        public List<LeaseService.LeaseServiceClient> tm_stubs = new List<LeaseService.LeaseServiceClient>(); //nao quero isto publico mas por agr fica assim
        public List<PaxosService.PaxosServiceClient> lm_stubs = new List<PaxosService.PaxosServiceClient>();
        List<Request> _requests = new List<Request>(); //requests received from TMs

        public LeaseData(List<string> lm_urls, List<string> tm_urls) 
        {
            foreach (string url in tm_urls) tm_stubs.Add(new LeaseService.LeaseServiceClient(GrpcChannel.ForAddress(url)));
            foreach (string url in lm_urls) lm_stubs.Add(new PaxosService.PaxosServiceClient(GrpcChannel.ForAddress(url)));
            RoundID = 0;
            Read_TS = 0;
            Write_TS = 0;
        }

        public int RoundID { set; get; }
        public int IncrementRoundID()
        {
            return ++RoundID;
        }
        public int Read_TS { get; set; }
        public int Write_TS { get; set; }

        public void AddRequest(Request request) //adds in the end
        {
            lock (this)
            {
                _requests.Add(request);
            }
        }
        public void AddRequestBeginning(Request request)
        {
            lock (this)
            {
                _requests.Insert(0, request);
            }
        }
        public bool RemoveRequest() //removes from beginning
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
                if (_requests.Count == 0) return myValue;
                while (true) {
                    if (Intersect(keys, _requests[0].Keys))
                    {
                        break;
                    }
                    myValue.Add(_requests[0]);
                    keys.AddRange(_requests[0].Keys);
                    RemoveRequest();
                }
                return myValue;
            }
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

    public class LeageManager : LeaseService.LeaseServiceBase
    {
        private string _name;
        private LeaseData _data;
        public LeageManager(string name, LeaseData data)
        {
            _name = name;
            _data = data;
        }

        public override Task<LeaseReply> Lease(LeaseRequest request, ServerCallContext context)
        {
            return Task.FromResult(Ls(request));
        }
        public LeaseReply Ls(LeaseRequest request)
        {
            //dar lock

            _data.AddRequest(new Request(request.Id, request.Keys.ToList(), request.LeaseRequestId));
            return new LeaseReply { Ack = true };
        }

        public void BroadcastLease(int epoch, List<Request> leases)
        {
            _data.BroadLease(epoch, leases);
        }
    }


    public class Paxos : PaxosService.PaxosServiceBase
    {
        private LeaseData _data;

        public Paxos(string name, LeaseData data)
        {
            Name = name;
            _data = data;
            //When paxos is called needs to select the requests that don´t conflict to define our value
            My_value = _data.GetMyValue();
        }
        public string Name { get; }
        public List<Request> My_value { set; get; }

        
        public override Task<Promise> Prepare(PrepareRequest request, ServerCallContext context)
        {
            return Task.FromResult(Prep(request));
        }
        public Promise Prep(PrepareRequest request) //returns promise if roundId >  my readTS
        {
            //dar lock
            
            Promise reply;
            if (request.RoundId <= _data.Read_TS)
            {
                reply = new Promise { Ack = false };
                return reply;
            }

            _data.Read_TS = request.RoundId;
            reply = new Promise { WriteTs = _data.Write_TS, Ack = true };
            foreach (Request r in My_value)
            {
                LeasePaxos lp = new LeasePaxos { Tm = r.Tm_name, LeaseRequestId = r.LeaseID };
                foreach (string k in r.Keys) { lp.Keys.Add(k); }
                reply.Leases.Add(lp);
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
            if (request.WriteTs < _data.Read_TS)
            {
                reply.Ack = false;
                return reply;
            }
            reply.Ack = true;
            //só aplica o valor recebido depois de receber uma maioria de accepted dos outros, falta essa msg no proto
            //devia guardar o valor a aplicar?
            return reply;
        }
    }

    public class PaxosLeader
    {
        private LeaseData _data;

        public PaxosLeader(string name, LeaseData data)
        {
            Name = name;
            Others_value = new List<Request>();
            Other_TS = 0;
            _data = data;
            My_value = _data.GetMyValue();

            if (PrepareRequest()) AcceptRequest();
        }
        public string Name { get; }
        public List<Request> My_value { set; get; }
        public List<Request> Others_value { set; get; }
        public int Other_TS { get; set; }

        public bool PrepareRequest()
        {
            PrepareRequest request = new PrepareRequest { RoundId = _data.IncrementRoundID() };
            Promise reply;
            int promises = 0;
            foreach (PaxosService.PaxosServiceClient stub in _data.lm_stubs)
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
            return promises > (_data.lm_stubs.Count + 1) / 2; //true se for maioria, removemos da lista se morrerem?
        }

        public void AcceptRequest()
        {
            AcceptReply reply;
            AcceptRequest request = new AcceptRequest { WriteTs = _data.RoundID};
            LeasePaxos lp;
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
            foreach (PaxosService.PaxosServiceClient stub in _data.lm_stubs)
            {
                reply = stub.AcceptAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult(); // tirar isto de syncrono
                //if (reply.Ack) promises++;
            }
            //return promises;
        }
    }
}