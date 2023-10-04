using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;
using System.Xml.Linq;
using Grpc.Core;
using Grpc.Net.Client;
using DADTKV_TM.Contact;
using DADTKV_TM.Structs;

namespace DADTKV_TM
{
    /// <summary>
    /// Where data is stored
    /// </summary>
    public class Store // guarda a info
    {
        private string _name;
        private Dictionary<string, int> _store;
        private Dictionary<string, Queue<Lease>> _leases;
        private RequestList _reqList;
        public Store(string name)
        {
            _reqList = new RequestList(10); // maximum of requests waiting allowed
            _name = name;
            _store = new Dictionary<string, int>(); // Stores the DadInt data
            _leases = new Dictionary<string, Queue<Lease>>(); // Stores the leases in a Lifo associated with a key (string)
        }
        public int insertRequest(List<string> reads, List<DadIntProto> writes)
        {
            return _reqList.insert(reads, writes);
        }
        public ResultOfTransaction getResult(int tnum)
        {
            return _reqList.getResult(tnum);
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
            { // ver intercessao com outras leases e marcar para end// nao so com as atuais mas entre as novas tambem
                //ex recebe 1a 1a marca o primeiro para end (apenas marca os nossos)
                //ex recebe 1a 2a 1a  marca o primeiro para end 
                // NOT WORKING FOR EVERY CASE YET
                foreach (KeyValuePair<string, string> pair in keyValuePairs) 
                {
                    foreach (Lease lease in _leases[pair.Key]) 
                        if (pair.Value == _name){
                            lease.End = true;// marcar as nossas
                        }

                    _leases[pair.Key].Enqueue(new Lease(pair.Value, epoch));  
                }
            }
            return true; 
        }
    }
}