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
        List<Request> received;
        public LeageManager(string name, LeaseData data, List<string> tm_urls, List<string> lm_urls)
        {
            _name = name;
            _data = data;
            _tmContact = new TmContact(tm_urls);
            _lmcontact = new LmContact(name, lm_urls);
            received = new List<Request>();
        }

        public override Task<LeaseReply> Lease(LeaseRequest request, ServerCallContext context)
        {
            return Task.FromResult(Ls(request));
        }
        public LeaseReply Ls(LeaseRequest request)
        {
            Console.WriteLine("NEW REQUEST: " + request.ToString());
            lock(received)
            {
                received.Add(new Request(request.Id, request.Keys.ToList(), request.LeaseId));
            }
            //_data.AddRequest(new Request(request.Id, request.Keys.ToList(), request.LeaseId));
            return new LeaseReply { Ack = true };
        }

        public override Task<LeaseReply> ConfirmLease(ConfirmLeaseRequest request, ServerCallContext context)
        {
            return Task.FromResult(Confirm(request));
        }
        public LeaseReply Confirm(ConfirmLeaseRequest request)
        {
            Request req = null;

            lock(received)
            {
                foreach (Request r in received)
                {
                    if (r.Lease_ID == request.LeaseId && r.Tm_name == request.Id)
                    {
                        req = r;
                    }
                }
                if(req == null) return new LeaseReply { Ack = true };
                if (request.Ack) _data.AddRequest(req);
                received.Remove(req);
            }
            Console.WriteLine("CONFIRMATION TM : " + request.Id + " LEASEID: " + request.LeaseId + request.Ack);
            return new LeaseReply { Ack = true };

        }

        public void BroadcastLease(int epoch, List<Request> leases)
        {
            _tmContact.BroadLease(epoch, leases);
        }
    }
}
