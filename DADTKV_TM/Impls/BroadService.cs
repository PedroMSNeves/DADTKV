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
            Console.WriteLine("REceived new propagate");
            foreach(DadIntTmProto dadIntTmProto in request.Writes.ToList()) Console.WriteLine(dadIntTmProto.ToString());
            return new BroadReply { Ack = store.Write(request.Writes.ToList(), request.TmName) };
        }
        public override Task<BroadReply> ResidualDeletion(ResidualDeletionRequest request, ServerCallContext context)
        {
            return Task.FromResult(RDeletion(request));
        }
        public BroadReply RDeletion(ResidualDeletionRequest request)
        {
            return new BroadReply { Ack = store.DeleteResidual(request.FirstKeys.ToList(), request.TmName) } ;
        }
    }
}
