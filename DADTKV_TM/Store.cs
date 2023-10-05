using DADTKV_TM.Contact;
using DADTKV_TM.Structs;
using System.Diagnostics.Contracts;

namespace DADTKV_TM
{
    /// <summary>
    /// Where data is stored
    /// </summary>
    public class Store
    {
        private string _name;
        private Dictionary<string, int> _store;
        private Dictionary<string, Queue<Lease>> _leases;
        private List<FullLease> _fullLeases;

        private RequestList _reqList;
        LmContact _lmcontact;
        TmContact _tmContact;
        public Store(string name, List<string> tm_urls, List<string> lm_urls)
        {
            _reqList = new RequestList(10); // maximum of requests waiting allowed
            _name = name;
            _store = new Dictionary<string, int>(); // Stores the DadInt data
            _leases = new Dictionary<string, Queue<Lease>>(); // Stores the leases in a Lifo associated with a key (string)
            _tmContact = new TmContact(tm_urls);
            _lmcontact = new LmContact(name, lm_urls);
            _fullLeases = new List<FullLease>();
        }
        //////////////////////////////////////////////USED BY SERVERSERVICE////////////////////////////////////////////////////////////
        /// <summary>
        /// Used by the ServerService class (add new client transactions)
        /// </summary>
        /// <param name="reads"></param>
        /// <param name="writes"></param>
        /// <returns></returns>
        public int verifyAndInsertRequest(List<string> reads, List<DadIntProto> writes)
        {
            lock (this) 
            {
                foreach (string read in reads)
                {
                    if (!_store.ContainsKey(read)) { return -1; }
                }
                foreach (DadIntProto write in writes)
                {
                    if (!_store.ContainsKey(write.Key)) 
                    { 
                        _store[write.Key] = -1; //default value (will never be read)
                        _leases[write.Key] = new Queue<Lease>();  
                    } 
                }
            }
            return _reqList.insert(reads, writes);
        }
        /// <summary>
        /// Used by the ServerService class (get the result of a client transactions)
        /// </summary>
        /// <param name="tnum"></param>
        /// <returns></returns>
        public ResultOfTransaction getResult(int tnum)
        {
            return _reqList.getResult(tnum);
        }
        //////////////////////////////////////////////USED BY MAINTHREAD////////////////////////////////////////////////////////////
        /// <summary>
        /// Used By MainThread
        /// Used to verify the requests and then categorises them in Yes,Maybe,No
        /// </summary>
        public void Verify()
        {
            /*
             * Verificar todos os da lista 1 a 1 e meter como S T N
             * Verificar os que ja têm T para saber se voltam a N (e pedimos a lease por isso vai para S)
             */
            List<Request> reqs = _reqList.GetRequests(); // podia estar fora do lock porque nao da conflito de escrita
            lock (this)
            {
                bool maybeOnly = false;
                
                  
                for (int i = 0; i< reqs.Count; i++)
                {
                    if (reqs[i].Situation == leaseRequested.Yes) { continue; }
                    else if(reqs[i].Situation == leaseRequested.No)
                    {
                        /*
                         * Verificar se temos a lease atualmente
                         * Verificar se é um subgrupo de um request marcado a S
                         * depois da veri, se ainda a N pedir se nao tiver com o maybe antes
                         * Marca a T
                         */
                        bool completed = true;
                        List<string> keys = reqs[i].Keys;
                        foreach (string key in keys) 
                        {
                            //veri se somos um subgrupo de uma lease completa (1º lugar em todas as keys dela) e se funcionamos com essa lease
                            if(!_leases[key].TryPeek(out var lease) || lease.Tm_name != _name) // ERROR: temos de verificar se a lease esta toda em 1º lugar
                            {
                                completed = false;
                                break;
                            }
                        }

                        if (completed && !maybeOnly)
                        {
                            // se ha alguem que usa a lease antes de nos
                            // if sim fica a Maybe
                            // if nao fica a Yes se nao tiver nenhuma intersessao com outro request anterior ai fica a N  (para manter ordem total) 
                            reqs[i].Situation = leaseRequested.Yes;
                            continue;
                        }
                        else if (completed && maybeOnly)
                        {
                            reqs[i].Situation = leaseRequested.Maybe;
                            continue;
                        }
                            

                        // vai ter subreposicao com parte da veri de cima, mas tem casos diff, os que ainda nao temos a lease, mas ja pedimos
                        for (int j = 0; j < i; j++)
                        {
                            if (reqs[i].SubGroup(reqs[j]) && reqs[j].Situation != leaseRequested.No)
                            {
                                reqs[i].Situation = leaseRequested.Maybe;
                                maybeOnly = true;
                                break;
                            }
                        }

                        if (!maybeOnly && reqs[i].Situation == leaseRequested.No)
                        {
                            //request lease
                        }
                    }
                    else // é o maybe
                    {
                        /*
                         * se tivermos a lease e nao tivermos intersessoes com pessoal atras fica a sim
                         */
                        bool completed = true;
                        List<string> keys = reqs[i].Keys;
                        foreach (string key in keys)
                        {
                            //veri se somos um subgrupo de uma lease completa (1º lugar em todas as keys dela) e se funcionamos com essa lease
                            if (!_leases[key].TryPeek(out var lease) || lease.Tm_name != _name) // ERROR: temos de verificar se a lease esta toda em 1º lugar
                            {
                                completed = false;
                                break;
                            }
                        }
                        if (!completed)
                        {
                            // pedir lease
                            continue;                        
                        }
                        completed = true;
                        for (int j = 0; j < i; j++)
                        {
                            if (reqs[i].SubGroup(reqs[j])) //ERROR:  intersessao da lease com os J
                            {
                                completed = false;
                                break;
                            }
                        }
                        if(completed)
                        {
                            reqs[i].Situation = leaseRequested.Yes;
                        }
                    }
                }
                Execute(ref reqs);
            }
            
        }
        /// <summary>
        /// Tries to execute the requests marked with Yes
        /// </summary>
        /// <param name="reqs"></param>
        public void Execute(ref List<Request> reqs)
        {
            int i = 0;
            while (true)
            {
                if (reqs[i].Situation != leaseRequested.Yes)
                {
                    break;
                }
                else if (reqs[i].Situation == leaseRequested.Yes)
                {
                    //veri se executavel
                    //veri se somos um subgrupo de uma lease completa (1º lugar em todas as keys dela) e se funcionamos com essa lease
                   
                    //maybe guardar as leases 
                    _reqList.remove(i);
                    List<DadIntProto> reply = new List<DadIntProto>();
                    int err = Request(reqs[i].Reads, reqs[i].Writes, ref reply, _tmContact);
                    if (err == -2)
                    {
                        //try again, only one time
                        err = Request(reqs[i].Reads, reqs[i].Writes, ref reply, _tmContact);
                    }
                    _reqList.move(0, reply, err);

                }
                i++;
                
            }
           
        }
        /// <summary>
        /// Executes a request per say and broadcast the changes
        /// </summary>
        /// <param name="reads"></param>
        /// <param name="writes"></param>
        /// <param name="reply"></param>
        /// <param name="tmContact"></param>
        /// <returns></returns>
        public int Request(List<string> reads, List<DadIntProto> writes, ref List<DadIntProto> reply, TmContact tmContact)
        { //esta a receber sempre 0 epoch depois mudar
            if (!tmContact.BroadCastChanges(writes,_name,0)) return -2; // depois tem de mandar info sobre fechar lease again
            foreach (string key in reads) reply.Add(new DadIntProto { Key = key, Value = _store[key] });
            foreach (DadIntProto write in writes) _store[write.Key] = write.Value; //testar se cria a key mesmo se nao tiver la 
            return 0;
        }
        //////////////////////////////////////////////USED BY BROADSERVICE////////////////////////////////////////////////////////////
        /// <summary>
        /// Used by BroadService
        /// Writes the incoming changes
        /// </summary>
        /// <param name="writes"></param>
        /// <param name="tm_name"></param>
        /// <param name="epoch"></param>
        /// <returns></returns>
        public bool Write(List<DadIntTmProto> writes, string tm_name, int epoch)
        {
            lock (this)
            {
                if (tm_name != null && writes.Count != 0)
                {
                    if (!LeaseRemove(writes[0].Key, tm_name, epoch)) return false;
                }
                foreach (DadIntTmProto write in writes) { _store[write.Key] = write.Value; }
            }
            return true;
        }
        /// <summary>
        /// Removes Leases marked to be removed (of other Tm)
        /// </summary>
        /// <param name="tm_name"></param>
        /// <param name="epoch"></param>
        /// <returns></returns>
        public bool LeaseRemove(string firstkey, string tm_name, int epoch)
        {
            //basta 1 key para descobrir a full lease? testar se funciona
            //no maximo verificar se fl funciona para os writes pedidos (acho que nao ness)

            FullLease fl;
            if(_leases[firstkey].TryPeek(out var lease))
            {
                fl = (FullLease)lease;
            }
            else
            {
                return false;
            }
            // Verify all entries
            foreach (string key in fl.Keys)
            {
                if (_leases[key].TryPeek(out var result))
                {
                    if (result.Tm_name != tm_name) return false;
                }
                else return false;
            }
            // Remove all entries
            foreach (string key in fl.Keys)
            {
                _leases[key].Dequeue();
            }
            return true;
        }
        //////////////////////////////////////////////USED BY LSERVICE////////////////////////////////////////////////////////////
        /// <summary>
        /// Used By LService
        /// Receives new leases for the epoch
        /// </summary>
        /// <param name="keyValuePairs"></param>
        /// <param name="epoch"></param>
        /// <returns></returns>
        public void NewLeases(List<FullLease> leases, int epoch)
        {
            // mensagem especial para eliminar os restos
            lock (this)
            { 
                foreach (FullLease fl in leases)
                {
                    foreach(string  key in fl.Keys)
                    {
                        if(_leases.ContainsKey(key))
                        {
                            foreach (Lease l in _leases[key])
                            {
                                if (l.Tm_name == _name)
                                {
                                    l.End = true;
                                }
                            }
                        }
                        else
                        {
                            _leases[key] = new Queue<Lease>();
                        }
                        // Now the list and the queue are connected to the same lease object
                        _leases[key].Enqueue(fl); // DownCast
                    }
                    _fullLeases.Add(fl);
                }
            }
        }
    }
}