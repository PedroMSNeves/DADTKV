using Grpc.Core;
using DADTKV_TM.Structs;

namespace DADTKV_TM.Impls
{
    /// <summary>
    /// Receive new leases from the Lease Managers
    /// </summary>
    public class LService : LeaseService.LeaseServiceBase
    {
        private Store store;

        public LService(Store st)
        {
            store = st;
        }
        public override Task<LeaseReply> LeaseBroadCast(LeaseBroadCastRequest request, ServerCallContext context)
        {
            return Task.FromResult(LBCast(request));
        }
        public LeaseReply LBCast(LeaseBroadCastRequest request)
        {
            List<FullLease> leases = new List<FullLease>();
            Console.WriteLine("newleases");
            foreach (LeaseProto lp in request.Leases)
            {
                Console.WriteLine(lp.ToString());
                leases.Add(new FullLease(lp.Tm,request.Epoch,lp.Keys.ToList()));
            }
            store.NewLeases(leases, request.Epoch);
            return new LeaseReply { Ack = true };
        }
    }
}
