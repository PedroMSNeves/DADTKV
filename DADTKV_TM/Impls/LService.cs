using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
            Dictionary<string, string> keyValuePairs = new Dictionary<string, string>();
            foreach (LeaseProto lp in request.Leases)
            {
                foreach (string key in lp.Keys)
                {
                    keyValuePairs[key] = lp.Tm;
                }
            }
            return new LeaseReply { Ack = store.NewLeases(keyValuePairs, request.Epoch) };
        }
    }
}
