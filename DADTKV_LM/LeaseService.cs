using Grpc.Core;
using System;
using System.Collections;
namespace DADKTV_LM
{
    public class ServerService : LeaseService.LeaseServiceBase
    {
        private string _name;
        public ServerService(string name, List<string> tm_urls, List<string> lm_urls)
        {
            _name = name;
        }

        public override Task<LeaseReply> Lease(LeaseRequest request, ServerCallContext context) // return task
        {
            return Task.FromResult(Ls(request));
        }
        public LeaseReply Ls(LeaseRequest request) //perform the request
        {
            LeaseReply reply = new LeaseReply { };
            return reply;
        }
    }
}