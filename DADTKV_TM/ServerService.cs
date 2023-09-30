using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Xml.Linq;
using Grpc.Core;
using Grpc.Net.Client;


namespace DADTKV_TM
{
    public struct Lease
    {
        public Lease(string tm_name, int epoch)
        {
            Tm_name = tm_name;
            Epoch = epoch;
        }
        public string Tm_name { get; }
        public int Epoch { get; }

        public override string ToString() => $"({Tm_name}, {Epoch})";
    }

    public class Store // guarda a info
    {
        private Dictionary<string, int> _store;
        private Dictionary<string, Queue<Lease>> _leases;
        public Store()
        {
            _store = new Dictionary<string, int>();
            _leases = new Dictionary<string, Queue<Lease>>(); // guarda as leases num fifo associadas a uma string
        }
        public int Request(List<string> reads, List<DadIntProto> writes, ref List<DadIntProto> reply, string name, TmContact tmContact)
        {
            lock (this)
            {
                foreach (string read in reads)
                {
                    if (_leases.TryGetValue(read, out var queue))
                    {
                        try
                        {
                            Lease lease = queue.Peek();
                            if (lease.Tm_name != name) return -1;
                        }
                        catch (InvalidOperationException ex) { return -1; }
                    }
                    else return -2; // sem entrada, nao pode criar

                }
                foreach (DadIntProto write in writes)
                {

                    if (_leases.TryGetValue(write.Key, out var queue))
                    {
                        try
                        {
                            Lease lease = queue.Peek();
                            if (lease.Tm_name != name) return -1;
                        }
                        catch (InvalidOperationException ex) { return -1; }
                    }
                    else return -1; // sem entrada mas cria
                }
                //asdasdasda
                if(!tmContact.BroadCastChanges(writes)) return -3;
                foreach(string key in reads) reply.Add(new DadIntProto{ Key = key , Value = _store[key] });
                foreach(DadIntProto write in writes) _store[write.Key] = write.Value; //testar se cria a key mesmo se nao tiver la 
            }
            
            return 0;
        }
    }
    public class ServerService : TmService.TmServiceBase
    {
        private string _name;
        Store _store;
        LmContact _lmcontact;
        TmContact _tmContact;
        public ServerService(Store store, string name, List<string> tm_urls, List<string> lm_urls)
        {
            _name = name;
            _store = store;
            _tmContact = new TmContact(tm_urls);
            _lmcontact = new LmContact(lm_urls);
            //foreach(string url in tm_urls)
        }
        public override Task<TxReply> TxSubmit(TxRequest request, ServerCallContext context) // devolve task
        {
            return Task.FromResult(TxSub(request));
        }
        public TxReply TxSub(TxRequest request) //cumpre o pedido
        {
            List<DadIntProto> reply = new List<DadIntProto>();
            List<string> reads = new List<string>();
            List<DadIntProto> writes = new List<DadIntProto>();
            bool requested = false; 

            foreach (string st in request.Reads) { reads.Add(st); }
            foreach (DadIntProto dad in request.Writes) { writes.Add(dad); }

            while (true)
            {
                int err = _store.Request(reads, writes, ref reply, _name, _tmContact);
                if (err == 0) break;
                else if(err == -2) { throw new RpcException(new Status(StatusCode.NotFound, "Read key not found")); }
                else if (err == -3) { throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Could not broadcast the writes")); }
                else if (err == -1 && !requested) 
                {
                    LeaseRequest(ref reads, ref writes);
                    requested = true;
                }
            }

            return new TxReply();
        }
        public bool LeaseRequest(ref List<string> reads,ref List<DadIntProto> writes)
        {
            List<string> keys = new List<string>();
            foreach (string key in reads) { keys.Add(key); }
            foreach (DadIntProto write in writes) { keys.Add(write.Key); }
            keys = keys.Distinct().ToList(); // retira duplicados
            _lmcontact.RequestLease(keys, _name); //try catch

            return true;
        }
    }

    public class LmContact
    {
        List<LeaseService.LeaseServiceClient> lm_stubs = new List<LeaseService.LeaseServiceClient>();
        public LmContact (List<string> lm_urls)
        {
            foreach (string url in lm_urls) lm_stubs.Add(new LeaseService.LeaseServiceClient(GrpcChannel.ForAddress(url)));
        }
        public void RequestLease(List<string> keys, string name)
        {
            LeaseReply reply;
            LeaseRequest request = new LeaseRequest { Id = name }; //cria request
            request.Keys.AddRange(keys);

            foreach (LeaseService.LeaseServiceClient stub in lm_stubs)
            {
                reply = stub.LeaseAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult(); // tirar isto de syncrono
            }
        }
    }
    public class TmContact
    {
        List<BroadCastService.BroadCastServiceClient> tm_stubs = new List<BroadCastService.BroadCastServiceClient>();
        public TmContact(List<string> tm_urls)
        {
            foreach (string url in tm_urls) tm_stubs.Add(new BroadCastService.BroadCastServiceClient(GrpcChannel.ForAddress(url)));
        }
        public bool BroadCastChanges(List<DadIntProto> writes)
        {
            BroadReply reply;
            BroadRequest request = new BroadRequest(); //cria request
            List<DadIntTmProto> writesTm = new List<DadIntTmProto>();
            foreach(DadIntProto tm in writes) writesTm.Add(new DadIntTmProto { Key = tm.Key , Value = tm.Value });
            request.Writes.AddRange(writesTm);

            foreach (BroadCastService.BroadCastServiceClient stub in tm_stubs)
            {
                reply = stub.BroadCastAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult(); // tirar isto de syncrono
                //nao faz nada com as reply, ainda
            }
            return true;
        }
    }
}
