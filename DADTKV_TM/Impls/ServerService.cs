using DADTKV_TM.Contact;
using DADTKV_TM.Structs;
using Grpc.Core;

namespace DADTKV_TM.Impls
{
    /// <summary>
    /// Receive Requests from the Clients
    /// </summary>
    public class ServerService : TmService.TmServiceBase
    {
        Store _store;

        public ServerService(Store store, string name)
        {
            _store = store;
        }
        public override Task<TxReply> TxSubmit(TxRequest request, ServerCallContext context) // Returns task
        {
            return Task.FromResult(TxSub(request));
        }
        public TxReply TxSub(TxRequest request)
        {
            ResultOfTransaction reply;
            List<string> reads = request.Reads.ToList();
            List<DadIntProto> writes = request.Writes.ToList();
            int tnum = _store.verifyAndInsertRequest(reads, writes);
            if (tnum == -1) { throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Could not get hold of Lm's")); }
            reply = _store.getResult(tnum);
            if (reply.Error_code == -2) { throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Could not broadcast the writes")); }

            TxReply tx = new TxReply();
            tx.Reads.AddRange(reply.Result);

            return tx;
        }
    }
}
