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
        LmContact _lmcontact;//é para tirar
        TmContact _tmContact;//é para tirar
        public ServerService(Store store, string name, List<string> tm_urls, List<string> lm_urls)
        {
            _store = store;
            _tmContact = new TmContact(tm_urls);
            _lmcontact = new LmContact(name, lm_urls);
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

            int tnum = _store.insertRequest(reads, writes);
            reply = _store.getResult(tnum);
            if (reply.Error_code == -1) { throw new RpcException(new Status(StatusCode.NotFound, "Read key not found")); }
            else if (reply.Error_code == -2) { throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Could not broadcast the writes")); }

            TxReply tx = new TxReply();
            tx.Reads.AddRange(reply.Result);

            return tx;
        }
        /*public bool LeaseRequest(ref List<string> reads,ref List<DadIntProto> writes)
        {
            List<string> keys = new List<string>();
            foreach (string key in reads) { keys.Add(key); }
            foreach (DadIntProto write in writes) { keys.Add(write.Key); }
            keys = keys.Distinct().ToList(); // retira duplicados
            _lmcontact.RequestLease(keys); //try catch

            return true;
        }*/
    }
}
