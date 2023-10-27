using Grpc.Core;

namespace DADTKV_TM.Impls
{
    /// <summary>
    /// Receive changes from other Transaction Managers
    /// </summary>
    public class BroadService : BroadCastService.BroadCastServiceBase
    {
        private Store store;
        Dictionary<string, List<LeaseProtoTm>> waitListresidual = new Dictionary<string, List<LeaseProtoTm>>();

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
            // Verification to deny response to dead Tms
            foreach (string name in store.GetDeadNamesTm()) if(name == request.TmName) throw new RpcException(new Status(StatusCode.Aborted, "You are dead"));
            // Sees if it can write the request
            return new BroadReply { Ack = store.TestWrite(request.TmName, request.LeaseId, request.Epoch) };
        }
        public override Task<BroadReply> ConfirmBroadChanges(ConfirmBroadChangesRequest request, ServerCallContext context)
        {
            return Task.FromResult(Conf(request));
        }
        public BroadReply Conf(ConfirmBroadChangesRequest request)
        {
            Console.WriteLine("REceived new Write Confirmation");
            // Writes the request if given green light
            if (request.Ack)
            {
                store.Write(request.Writes.ToList());
            }
            return new BroadReply { Ack = true };
        }
        public override Task<ResidualReply> ResidualDeletion(ResidualDeletionRequest request, ServerCallContext context)
        {
            return Task.FromResult(RDeletion(request));
        }
        public ResidualReply RDeletion(ResidualDeletionRequest request)
        {
            // Verification to deny response to dead Tms
            foreach (string name in store.GetDeadNamesTm()) if (name == request.ResidualLeases[0].Tm) throw new RpcException(new Status(StatusCode.Aborted, "You are dead"));
            ResidualReply reply = new ResidualReply();
            Console.WriteLine("REceived new residualDeletion");

            // Sees if it can remove the requested leases
            lock (waitListresidual)
            {
                //if (waitListresidual.ContainsKey(request.ResidualLeases[0].Tm)) waitListresidual[request.ResidualLeases[0].Tm].Clear();
                waitListresidual[request.ResidualLeases[0].Tm] = request.ResidualLeases.ToList();
                bool[] acks = store.DeleteResidual(request.ResidualLeases.ToList(), request.Epoch);
                reply.Acks.AddRange(acks);
            }
            return reply;
        }

        public override Task<BroadReply> ConfirmResidualDeletion (ConfirmResidualDeletionRequest request, ServerCallContext context)
        {
            return Task.FromResult(Conf(request));
        }
        public BroadReply Conf(ConfirmResidualDeletionRequest request)
        {
            Console.WriteLine("REceived new residualDeletion Confirmation");
            // Removes requested leases 
            lock (waitListresidual)
            {
                for (int i = 0; i < request.Bools.Count; i++)
                {
                    if (request.Bools[i])
                    {
                        store.LeaseRemove(waitListresidual[request.TmName][i], request.Epoch);
                    }
                }
                waitListresidual[request.TmName].Clear();
            }
            return new BroadReply { Ack = true } ;
        }
        public override Task<PingTm> PingSuspect(PingTm request, ServerCallContext context)
        {
            return Task.FromResult(PingS(request));
        }
        public PingTm PingS(PingTm request)
        {
            return new PingTm(request);
        }
        public override Task<BroadReply> KillSuspect(KillRequestTm request, ServerCallContext context)
        {
            return Task.FromResult(KillS(request));
        }
        public BroadReply KillS(KillRequestTm request)
        {
            store.CrashedServer(request.TmName);
            return new BroadReply { Ack = true } ;
        }
    }
}
