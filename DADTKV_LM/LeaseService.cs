using Grpc.Core;
using System;
using System.Collections;
using System.Threading.Channels;
using Grpc.Net.Client;
using System.Globalization;

namespace DADKTV_LM
{
    public class Request
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
    public class Data
    {
        List<LeaseService.LeaseServiceClient> tm_stubs = new List<LeaseService.LeaseServiceClient>();
        Queue<Request> requests = new Queue<Request>();

        public Data(List<string> tm_urls) 
        {
            foreach (string url in tm_urls) tm_stubs.Add(new LeaseService.LeaseServiceClient(GrpcChannel.ForAddress(url)));
        }
        public bool AddRequest(Request request)
        {
            requests.Enqueue(request);
            return true;
        }
        public bool RemoveRequest()
        {
            Request result;
            return requests.TryDequeue(out result);
        }
    }

    public class ServerService : LeaseService.LeaseServiceBase
    {
        private string _name;
        private Data _data;
        public ServerService(string name, List<string> tm_urls, List<string> lm_urls)
        {
            _name = name;
            _data = new Data(tm_urls);
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
    }
}