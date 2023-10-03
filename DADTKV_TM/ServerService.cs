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
    /// <summary>
    /// Class lease for storing information about a certain lease
    /// </summary>
    public class Lease
    {
        public Lease(string tm_name, int epoch)
        {
            Tm_name = tm_name;
            Epoch = epoch;
            End = false;
        }
        public string Tm_name { get; }
        public int Epoch { get; }
        public bool End { set; get; }

        public override string ToString() => $"({Tm_name}, {Epoch}, {End})";
    }
    /// <summary>
    /// Where data is stored
    /// </summary>
    public class Store // guarda a info
    {
        private string _name;
        private Dictionary<string, int> _store;
        private Dictionary<string, Queue<Lease>> _leases;
        public Store(string name)
        {
            _name = name;
            _store = new Dictionary<string, int>();
            _leases = new Dictionary<string, Queue<Lease>>(); // guarda as leases num fifo associadas a uma string
        }
        public int Request(List<string> reads, List<DadIntProto> writes, ref List<DadIntProto> reply, TmContact tmContact)
        {
            List<int> epoch = new List<int>();
            lock (this)
            {
                foreach (string read in reads)
                {
                    if (_leases.TryGetValue(read, out var queue))
                    {
                        try
                        {
                            Lease lease = queue.Peek();
                            if (lease.Tm_name != _name) return -1;
                            if(lease.End) epoch.Add(lease.Epoch); 
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
                            if (lease.Tm_name != _name) return -1;
                            if (lease.End) epoch.Add(lease.Epoch);
                        }
                        catch (InvalidOperationException ex) { return -1; }
                    }
                    else return -1; // sem entrada mas cria
                }
                //asdasdasda
                string name = null;
                if (epoch.Any()) name = _name;

                epoch = epoch.Distinct().ToList(); //maybe depois fechar todas as suas marcadas para fechar, senao pode dar deadlock
                if (!tmContact.BroadCastChanges(writes, name, epoch)) return -3;
                foreach(string key in reads) reply.Add(new DadIntProto{ Key = key , Value = _store[key] });
                foreach(DadIntProto write in writes) _store[write.Key] = write.Value; //testar se cria a key mesmo se nao tiver la 
            }
            
            return 0;
        }
        public bool Write(List<DadIntTmProto> writes, string tm_name, List<int> epoch)
        {
            lock (this)
            {
                foreach (DadIntTmProto write in writes) { _store[write.Key] = write.Value; }
                if(tm_name != null) LeaseRemove(tm_name, epoch);
            }
            return true; // possivelmente tratamento de erros depois
        }
        public bool LeaseRemove(string tm_name, List<int> epoch) 
        {
            /* apagar todas as incidencias da lease em questao nao so a primeira
              a t1,t2,t32
              b t2
              c t31, t1
            */
            foreach (KeyValuePair<string,Queue<Lease>> pair in _leases)
            {
                bool remove = true;
                foreach (Lease l1 in pair.Value)
                {
                    if (l1.Tm_name == tm_name && epoch.Contains(l1.Epoch)) l1.End = true;
                }
                while (remove)
                {
                    if (pair.Value.TryPeek(out var result) && result.Tm_name != _name && result.End)
                    {
                        pair.Value.Dequeue();
                        remove = true;
                    }
                    else remove = false;
                }   
            }
            return true;
        }
        public bool NewLeases(Dictionary<string, string> keyValuePairs, int epoch)
        {
            lock (this) 
            { // ver intercessao com outras leases e marcar para end
                foreach (KeyValuePair<string, string> pair in keyValuePairs) 
                {
                    foreach (Lease lease in _leases[pair.Key]) if (pair.Value == _name) lease.End = true;// marcar as nossas

                    _leases[pair.Key].Enqueue(new Lease(pair.Value, epoch));  
                }
            }
            return true;
        }
    }
    /// <summary>
    /// Receive Requests from the Clients
    /// </summary>
    public class ServerService : TmService.TmServiceBase
    {
        Store _store;
        LmContact _lmcontact;
        TmContact _tmContact;
        public ServerService(Store store, string name, List<string> tm_urls, List<string> lm_urls)
        {
            _store = store;
            _tmContact = new TmContact(tm_urls);
            _lmcontact = new LmContact(name,lm_urls);
            //foreach(string url in tm_urls)
        }
        public override Task<TxReply> TxSubmit(TxRequest request, ServerCallContext context) // devolve task
        {
            return Task.FromResult(TxSub(request));
        }
        public TxReply TxSub(TxRequest request) //cumpre o pedido
        {
            List<DadIntProto> reply = new List<DadIntProto>();
            List<string> reads = request.Reads.ToList();
            List<DadIntProto> writes = request.Writes.ToList();
            bool requested = false; 

            //foreach (string st in request.Reads) { reads.Add(st); }
            //foreach (DadIntProto dad in request.Writes) { writes.Add(dad); }

            while (true)
            {
                int err = _store.Request(reads, writes, ref reply, _tmContact);
                if (err == 0) break;
                else if(err == -2) { throw new RpcException(new Status(StatusCode.NotFound, "Read key not found")); }
                else if (err == -3) { throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Could not broadcast the writes")); }
                else if (err == -1 && !requested) 
                {
                    LeaseRequest(ref reads, ref writes);
                    requested = true;
                }
                //Monitor.Wait(this); meter a esperar até receber novas leases
            }

            return new TxReply();
        }
        public bool LeaseRequest(ref List<string> reads,ref List<DadIntProto> writes)
        {
            List<string> keys = new List<string>();
            foreach (string key in reads) { keys.Add(key); }
            foreach (DadIntProto write in writes) { keys.Add(write.Key); }
            keys = keys.Distinct().ToList(); // retira duplicados
            _lmcontact.RequestLease(keys); //try catch

            return true;
        }
    }
    /// <summary>
    /// Conctact with Lease Manager
    /// </summary>
    public class LmContact
    {
        private int _lease_id;
        private string _name;
        List<LeaseService.LeaseServiceClient> lm_stubs = new List<LeaseService.LeaseServiceClient>();
        public LmContact (string name, List<string> lm_urls)
        {
            _lease_id = 0;
            _name = name;
            foreach (string url in lm_urls) lm_stubs.Add(new LeaseService.LeaseServiceClient(GrpcChannel.ForAddress(url)));
        }
        public bool RequestLease(List<string> keys)
        {
            LeaseReply reply;
            LeaseRequest request = new LeaseRequest { Id = _name, LeaseRequestId = _lease_id }; //cria request
            request.Keys.AddRange(keys);

            foreach (LeaseService.LeaseServiceClient stub in lm_stubs)
            {
                reply = stub.LeaseAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult(); // tirar isto de syncrono
            }
            incrementLeaseId();
            return true;
        }
        public void incrementLeaseId() { _lease_id++; }
    }
    /// <summary>
    /// Contact with Transaction Manager, to propagate changes
    /// </summary>
    public class TmContact
    {
        List<BroadCastService.BroadCastServiceClient> tm_stubs = new List<BroadCastService.BroadCastServiceClient>();
        public TmContact(List<string> tm_urls)
        {
            foreach (string url in tm_urls) tm_stubs.Add(new BroadCastService.BroadCastServiceClient(GrpcChannel.ForAddress(url)));
        }
        public bool BroadCastChanges(List<DadIntProto> writes, string name, List<int> epoch)
        {
            BroadReply reply;
            BroadRequest request = new BroadRequest { TmName = name }; //cria request
            List<DadIntTmProto> writesTm = new List<DadIntTmProto>();
            foreach(DadIntProto tm in writes) writesTm.Add(new DadIntTmProto { Key = tm.Key , Value = tm.Value });
            request.Epoch.AddRange(epoch);
            request.Writes.AddRange(writesTm);

            foreach (BroadCastService.BroadCastServiceClient stub in tm_stubs)
            {
                reply = stub.BroadCastAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult(); // tirar isto de syncrono
                //nao faz nada com as reply, ainda
            }
            return true;
        }
    }
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
        public BroadReply BCast (BroadRequest request)
        {
            return new BroadReply { Ack = store.Write(request.Writes.ToList(),request.TmName,request.Epoch.ToList()) };
        }
    }
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
            foreach(LeaseProto lp in request.Leases)
            {
                foreach(string key in lp.Keys)
                {
                    keyValuePairs[key] = lp.Tm;
                }
            }
            return new LeaseReply { Ack = store.NewLeases(keyValuePairs,request.Epoch) };
        }
    }
}