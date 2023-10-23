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
            // Tries to Insert Request
            int tnum = _store.VerifyAndInsertRequest(request.Reads.ToList(), request.Writes.ToList());
            if (tnum == -1) { throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Could not get hold of Lm's")); }
            // Gets the result of the request
            reply = _store.GetResult(tnum);
            if (reply.Error_code == -2) { throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Could not broadcast the writes")); }
            // Prepares reply
            TxReply tx = new TxReply();
            tx.Reads.AddRange(reply.Result);

            return tx;
        }
    }
}
