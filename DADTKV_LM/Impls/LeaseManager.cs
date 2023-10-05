using System;
using DADTKV_LM.Contact;
using DADTKV_LM.Structs;
using Grpc.Core;


namespace DADTKV_LM.Impls
{
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
            _data.AddRequest(new Request(request.Id, request.Keys.ToList()));
            return new LeaseReply { Ack = true };
        }

        public void BroadcastLease(int epoch, List<Request> leases)
        {
            _tmContact.BroadLease(epoch, leases);
        }
    }
}
