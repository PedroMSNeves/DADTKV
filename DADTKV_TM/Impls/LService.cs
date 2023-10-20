using Grpc.Core;
using DADTKV_TM.Structs;

namespace DADTKV_TM.Impls
{
    /// <summary>
    /// Receive new leases from the Lease Managers
    /// </summary>
    public class LService : LeaseService.LeaseServiceBase
    {
        private int _lm_count;
        private Store store;
        public List<WaitLeases> waitLeases;
        public LService(Store st, int lm_count)
        {
            _lm_count = lm_count;
            store = st;
            waitLeases = new List<WaitLeases>();
        }
        public override Task<LeaseReply> LeaseBroadCast(LeaseBroadCastRequest request, ServerCallContext context)
        {
            return Task.FromResult(LBCast(request));
        }
        public LeaseReply LBCast(LeaseBroadCastRequest request)
        {
            List<FullLease> leases = new List<FullLease>();
            bool ready = false;
            lock (waitLeases)
            {
                foreach (LeaseProto lp in request.Leases)
                {
                    Console.WriteLine(lp.ToString());
                    leases.Add(new FullLease(lp.Tm, request.Epoch, lp.Keys.ToList(), lp.LeaseId));
                }
                List<WaitLeases> remove = new List<WaitLeases>();
                foreach (WaitLeases wl in waitLeases)
                {
                    if (wl.Epoch == request.Epoch)
                    {
                        if (wl.Leases.Equals(leases))
                        {
                            wl.increaseAcks();
                            if (wl.Acks == _lm_count) ready = true;
                        }
                        remove.Add(wl);
                    }
                }
                if (ready)
                {
                    foreach (WaitLeases wl in remove)
                    {
                        waitLeases.Remove(wl);
                    }
                    store.WaitLeases(leases, request.Epoch);
                }

            }
            return new LeaseReply { Ack = true };
        }
    }
}
