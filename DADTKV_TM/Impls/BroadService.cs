using Grpc.Core;

namespace DADTKV_TM.Impls
{
    /// <summary>
    /// Receive changes from other Transaction Managers
    /// </summary>
    public class BroadService : BroadCastService.BroadCastServiceBase
    {
        private Store store;

        public BroadService(Store st)
        {
            store = st;
        }
        public override Task<BroadReply> BroadCast(BroadRequest request, ServerCallContext context)
        {
            return Task.FromResult(BCast(request));
        }
        public BroadReply BCast(BroadRequest request)
        {
            return new BroadReply { Ack = store.Write(request.Writes.ToList(), request.TmName, request.Epoch.ToList()) };
        }
    }
}
