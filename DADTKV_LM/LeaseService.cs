using Grpc.Core;
using System;
using System.Collections;
using System.Threading.Channels;
using Grpc.Net.Client;
using System.Globalization;
using System.Xml.Linq;
using System.Collections.Generic;
using System.ComponentModel;

namespace DADKTV_LM
{
    public class Request //Lease basically
    {
        public Request(string tm_name, List<string> keys, int leaseId)
        {
            Tm_name = tm_name;
            Keys = keys;
            //LeaseID = leaseId;
        }
        public string Tm_name { get; }
        public List<string> Keys { get; }
        //public int LeaseID { get; }
    }
    public class LeaseData
    {
        public List<LeaseService.LeaseServiceClient> tm_stubs = new List<LeaseService.LeaseServiceClient>(); //nao quero isto publico mas por agr fica assim
        public List<PaxosService.PaxosServiceClient> lm_stubs = new List<PaxosService.PaxosServiceClient>();
        Queue<Request> _requests = new Queue<Request>(); //requests received from TMs //change this to List
        int _writeTS = 0;
        int _readTS = 0;
        List<Request> _my_value = new List<Request>();

        public LeaseData(List<string> lm_urls, List<string> tm_urls) 
        {
            foreach (string url in tm_urls) tm_stubs.Add(new LeaseService.LeaseServiceClient(GrpcChannel.ForAddress(url)));
            foreach (string url in lm_urls) lm_stubs.Add(new PaxosService.PaxosServiceClient(GrpcChannel.ForAddress(url)));
        }
        public int GetWriteTS() 
        {
            return _writeTS;
        }
        public int GetReadTS()
        {
            return _readTS;
        }
        public int IncrementWriteTS() 
        {
            return ++_writeTS;
        }
        public int SetReadTS(int n)
        {
            _readTS = n;
            return _readTS;
        }
        public List<Request> GetMyValue()
        {
            return _my_value;
        }
        public bool AddRequest(Request request)
        {
            _requests.Enqueue(request);
            return true;
        }
        public bool RemoveRequest()
        {
            return _requests.TryDequeue(out var result);
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

        public override Task<LeaseReply> Lease(LeaseRequest request, ServerCallContext context) // return task
        {
            return Task.FromResult(Ls(request));
        }
        public LeaseReply Ls(LeaseRequest request) //perform the request
        {
            //dar lock
            LeaseReply reply = new LeaseReply { Ack = _data.AddRequest(
                new Request(request.Id, request.Keys.ToList(), request.LeaseRequestId)) };
            return reply;
        }

        public void BroadcastLease(int epoch, List<Request> leases) //meter isto na outra class do paxos
        {
            _data.BroadLease(epoch, leases);
        }
    }

    public class Paxos : PaxosService.PaxosServiceBase
    {
        private string _name;
        private LeaseData _data;
        public Paxos(string name, LeaseData data)
        {
            _name = name;
            _data = data;
            //When paxos is called needs to select the requests that don´t conflict to define our value
        }

        public int PrepareRequest()
        {
            PrepareRequest request = new PrepareRequest { RoundId = _data.IncrementWriteTS() }; //rever afinal não é bem assim
            Promise reply;
            int promises = 0;
            foreach (PaxosService.PaxosServiceClient stub in _data.lm_stubs)
            {
                reply = stub.PrepareAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult(); // tirar isto de syncrono
                if (reply.Ack) promises++;
            }
            return promises; //mudar para bool, true se for maioria
        }
        public override Task<Promise> Prepare(PrepareRequest request, ServerCallContext context) // return task
        {
            return Task.FromResult(Prep(request));
        }
        public Promise Prep(PrepareRequest request) //returns promise if roundId >  my readTS
        {
            //dar lock
            
            Promise reply;
            if (request.RoundId < _data.GetReadTS())
            {
                reply = new Promise { Ack = false };
                return reply;
            }

            _data.SetReadTS(request.RoundId);
            reply = new Promise { WriteTs = _data.GetWriteTS(), Ack = true };
            foreach (Request r in _data.GetMyValue())
            {
                LeasePaxos lp = new LeasePaxos { Tm = r.Tm_name };
                foreach (string k in r.Keys) { lp.Keys.Add(k); }
                reply.Leases.Add(lp);
            }
            return reply;
        }
    }
}