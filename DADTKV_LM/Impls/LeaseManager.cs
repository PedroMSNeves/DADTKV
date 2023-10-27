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
        public LeageManager(string name, LeaseData data, TmContact tmContact, LmContact lmContact)
        {
            _name = name;
            _data = data;
            _tmContact = tmContact;
            _lmcontact = lmContact;
        }

        public override Task<LeaseReply> Lease(LeaseRequest request, ServerCallContext context)
        {
            return Task.FromResult(Ls(request));
        }
        public LeaseReply Ls(LeaseRequest request)
        {
            Console.WriteLine("NEW REQUEST: " + request.ToString());
            //if (_tmContact.Alive(request.Id)) return new LeaseReply { Ack = false };

            _data.AddRequest(new Request(request.Id, request.Keys.ToList(), request.LeaseId));
            return new LeaseReply { Ack = true };
        }

        public void BroadcastLease(int epoch, List<Request> leases)
        {
            _tmContact.BroadLease(epoch, leases);
        }
    }
}
