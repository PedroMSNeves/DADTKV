using DADTKV_TM.Contact;
using DADTKV_TM.Structs;

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
                // If we arrive at a request marked with Maybe/No 
                if (reqs[i].Situation != leaseRequested.Yes) break;
                else // Request marked with Yes
                {
                    FullLease fl;
                    /* We can use first key to get the lease from a queue,
                       because you can only be marked with Yes if you have a lease waiting for you or you have requested a lease */
                    if (_leases[reqs[i].Keys[0]].TryPeek(out var lease) && lease.Tm_name == _name)
                    {
                        fl = (FullLease)lease;
                        if (!reqs[i].SubGroup(fl)) continue; /* If he is not a subGroup of the lease then:
                                                              * He is still waiting for is lease to arive and this is an old lease
                                                              * Or some other Transaction is waiting for some other Tm to release is key */
                        bool diff = false;
                        foreach (string key in fl.Keys)
                        {
                            if (_leases[key].TryPeek(out var result))
                            {
                                if (fl != (FullLease)result) // Even if 2 leases are equal will not be a problem, because the firt T that uses it will remove the first equal lease, exemple below
                                {                             
                                    /* EX: t1<a,b> t2<b,c> t3<a,b>
                                     * A  11 13
                                     * B  11 12 13
                                     * C  12
                                     * When t3 tries to execute the lease for the T1 as already been removed
                                     */
                                    diff = true;
                                    break; // Some other Transaction is waiting for some other Tm to release is key
                                }
                            }
                            else
                            {
                                diff = true;
                                break; // If the Queue is empty then the new leases did not arive yet
                            }
                        }
                        if (diff) continue;
                    }
                    else continue;/* If the lease is not of our Tm or the queue is empty:
                                   * The new set of leases did not arrive yet
                                   * Or is waiting for another Tm to end is lease */

                    // Remove from the request list
                    _reqList.remove(i);
                    List<DadIntProto> reply = new List<DadIntProto>();

                    // Tries to propagate
                    int err = Request(reqs[i].Reads, reqs[i].Writes, ref reply, fl.Epoch);

                    // Try again, only one time
                    if (err == -2) err = Request(reqs[i].Reads, reqs[i].Writes, ref reply, fl.Epoch);

                    // Moves it to the pickup waiting place
                    _reqList.move(reqs[i].Transaction_number, reply, err);

                }
                i++;  
                if (i >= reqs.Count)  break;
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
        public int Request(List<string> reads, List<DadIntProto> writes, ref List<DadIntProto> reply, int epoch)
        {
            if (!_tmContact.BroadCastChanges(writes,_name,epoch)) return -2;
            foreach (string key in reads) reply.Add(new DadIntProto { Key = key, Value = _store[key] }); // Does the reads
            foreach (DadIntProto write in writes) _store[write.Key] = write.Value; // Does the writes
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
                    if (!LeaseRemove(writes[0].Key, tm_name, epoch)) return false; // Always goes well because they are all correct servers
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
            FullLease fl;
            if(_leases[firstkey].TryPeek(out var lease)) fl = (FullLease)lease; // Gets FullLease
            else return false; // Theoretically never possible

            // Verify all entries
            foreach (string key in fl.Keys)
            {
                if (_leases[key].TryPeek(out var result))
                {
                    if (result.Tm_name != tm_name || result.Epoch != epoch) return false; // Theoretically never possible
                }
                else return false; // Theoretically never possible
            }
            // Remove all entries
            foreach (string key in fl.Keys)
            {
                _leases[key].Dequeue(); // Removes queue entries
            }
            _fullLeases.Remove(fl); // Removes lease list entry
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

            //eliminar leases residuais
            lock (this)
            {
                DeleteResidualLeases(leases, epoch);
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
                _reqList.incrementEpoch();
            }
        }
        private void DeleteResidualLeases(List<FullLease> newLeases, int epoch)
        {
            List<FullLease> residual = new List<FullLease>();
            foreach (FullLease fl in _fullLeases) { residual.Add(fl); }

            /* EX: We dont consider the new leases arriving
             * EpochM: 0     0    0     0       1      1       1      1     
             *       <A,I> <A,B> <P> <A,B,K>  <A,B,C> <C,D>  <A,B,C> <C,D>
             *          1º epoch               2º epoch    3º epoch
             *      A   10 11 13               15          17      
             *      B   11 13                  15          17
             *      K   13
             *      I   10
             *      P   12
             *      C   14                     15   16     17   18
             *      D   14                          16          18
             */


            // Removes all leases from residual list in queues marked to end, because if they have a lease to end before they execute, they didnt execute yet
            foreach (string key in _leases.Keys) 
            {
                bool end = false;
                foreach(Lease l in _leases[key]) 
                {
                    if (l.End) end = true;
                    if (end) residual.Remove((FullLease)l);
                }
            }

            /* We remove the leases from the queues that have END's
            * EpochM: 0     0    0     0       1      1       1      1     
            *       <A,I> <A,B> <P> <A,B,K>  <A,B,C> <C,D>  <A,B,C> <C,D>
            *          1º epoch
            *      A   
            *      B     
            *      K   
            *      I   
            *      P   12
            *      C   14      
            *      D   14      
            */

            foreach (FullLease fl in residual)
            {
                foreach (Request rq in _reqList.GetRequests())
                {
                    if (rq.Situation == leaseRequested.Yes)
                    {
                        // We test each residual request, the requests that were from a previous epochs (<= epoch-2),
                        if (epoch != 1 && rq.Epoch < epoch - 1 && fl.Epoch > rq.Epoch)
                        {
                            /* We remove the requests that are "new"
                             * EpochM: 0     0    0     0   
                             *       <A,I> <A,B> <P> <A,B,K>
                             *          1º epoch
                             *      P   12
                             *      C   14      
                             *      D   14      
                             */
                            // Is the only possible request to use that list, because we already removed all leases that had intersections with someone
                            if (rq.SubGroup(fl))
                            {
                                /* From the remaining leases we remove the one that have a request using them
                                 * EpochM: 0     0    0     0   
                                 *       <A,I> <A,B> <P> <A,B,K>
                                 *          1º epoch  
                                 *      C   14      
                                 *      D   14      
                                 * Tm1 lease 4 is residual
                                 */

                                // If someone will use it it's not residual then remove
                                residual.Remove(fl);
                            }
                        }
                    }
                }
            }
            

            foreach (FullLease fl in residual)
            {
                // This decreases the number of FullLeases that we have to test if it's residual
                if (fl.Intersection(newLeases))
                {
                    // Eliminates our Lease from all the _leases queues and the _fullLease list
                    LeaseRemove(fl.Keys[0], _name, fl.Epoch);
                    //mandar estas leases para os outros tm
                }
            }
        }
    }
}