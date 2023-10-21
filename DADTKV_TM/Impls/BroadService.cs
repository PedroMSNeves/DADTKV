using Grpc.Core;

namespace DADTKV_TM.Impls
{
    /// <summary>
    /// Receive changes from other Transaction Managers
    /// </summary>
    public class BroadService : BroadCastService.BroadCastServiceBase
    {
        private Store store;
        Dictionary<string, List<LeaseProtoTm>> waitList = new Dictionary<string, List<LeaseProtoTm>>();
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
            foreach(DadIntTmProto dadIntTmProto in request.Writes.ToList()) Console.WriteLine(dadIntTmProto.ToString());
            return new BroadReply { Ack = store.Write(request.Writes.ToList(), request.TmName, request.Epoch) };
        }
        public override Task<ResidualReply> ResidualDeletion(ResidualDeletionRequest request, ServerCallContext context)
        {
            return Task.FromResult(RDeletion(request));
        }
        public ResidualReply RDeletion(ResidualDeletionRequest request)
        {
            ResidualReply reply = new ResidualReply();
            Console.WriteLine("REceived new residualDeletion");
            lock (waitList)
            {
                if (waitList.ContainsKey(request.ResidualLeases[0].Tm)) waitList[request.ResidualLeases[0].Tm].Clear();
                waitList[request.ResidualLeases[0].Tm] = request.ResidualLeases.ToList();
                bool[] acks = store.DeleteResidual(request.ResidualLeases.ToList(), request.Epoch);
                reply.Acks.AddRange(acks);
            }
            return reply;
        }
        public override Task<BroadReply> Confirm(ConfirmRequest request, ServerCallContext context)
        {
            return Task.FromResult(Conf(request));
        }
        public BroadReply Conf(ConfirmRequest request)
        {
            Console.WriteLine("REceived new residualDeletion Confirmation");
            lock(waitList)
            {
                for (int i = 0; i < request.Bools.Count; i++)
                {
                    if (request.Bools[i])
                    {
                        store.LeaseRemove(waitList[request.TmName][i], request.Epoch);
                    }
                }
                waitList[request.TmName].Clear();
            }
            return new BroadReply { Ack = true } ;
        }
    }
}
