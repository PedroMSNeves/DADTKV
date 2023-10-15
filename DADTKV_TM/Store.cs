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
                        Console.WriteLine(reqs[0].Situation);

                        /*
                         * Verificar se temos a lease atualmente
                         * Verificar se é um subgrupo de um request marcado a S
                         * depois da veri, se ainda a N pedir se nao tiver com o maybe antes
                         * Marca a T
                         */
                        bool completed = true;
                        if (_leases[reqs[i].Keys[0]].TryPeek(out var lease) && lease.Tm_name == _name)
                        {
                            FullLease fl = (FullLease) lease;
                            if (reqs[i].SubGroup(fl))
                            {
                                foreach (string key in fl.Keys)
                                {
                                    // Verify if we have all the lease on the top
                                    if (_leases[key].TryPeek(out var l))
                                    {
                                        if (fl != (FullLease)l)
                                        {
                                            completed = false;
                                            break;
                                        }
                                    }
                                    else
                                    {
                                        completed = false;
                                        break; // Theoredicaly impossible
                                    }
                                }
                            }
                        }
                        else completed = false;

                        Console.WriteLine(reqs[0].Situation);

                        if (completed)
                        {
                            if (!maybeOnly)
                            {
                                // se ha alguem que usa a lease antes de nos
                                // if sim fica a Maybe
                                // if nao fica a Yes se nao tiver nenhuma intersessao com outro request anterior ai fica a N  (para manter ordem total) 
                                reqs[i].Situation = leaseRequested.Yes;
                                continue;
                            }
                            else
                            {
                                // to have MAYBE TRUE we can store all maybe keys and see if we intersect some if not, we can turn true 
                                // right now we cant have Maybe true, only maybe maybe or maybe no
                                reqs[i].Situation = leaseRequested.Maybe;
                                continue;
                            }
                        }
                        Console.WriteLine(reqs[0].Situation);



                        // vai ter subreposicao com parte da veri de cima, mas tem casos diff, os que ainda nao temos a lease, mas ja pedimos
                        for (int j = 0; j < i; j++)
                        {
                            if (reqs[i].SubGroup(reqs[j])) // <x,k>S <abc>S <cd>S <x,k>T <x,d>N <x>N
                            {
                                if(reqs[j].Situation != leaseRequested.No)
                                {
                                    reqs[i].Situation = leaseRequested.Maybe;
                                    maybeOnly = true;
                                    break;
                                }
                                else
                                {
                                    reqs[i].Situation = leaseRequested.No;
                                }
                                
                            }
                        }
                        // to have MAYBE TRUE we can store all maybe keys and see if we intersect some if not, we can turn true 
                        // right now we cant have Maybe true, only maybe maybe or maybe no
                        if (!maybeOnly && reqs[i].Situation == leaseRequested.No)
                        {
                            Console.WriteLine(reqs[0].Situation);
                            _lmcontact.RequestLease(reqs[i].Keys);
                            reqs[i].Situation = leaseRequested.Yes;
                            Console.WriteLine(reqs[0].Situation);

                        }
                    }
                    else // é o maybe
                    {
                        /*
                         * se tivermos a lease e nao tivermos intersessoes com pessoal atras fica a sim
                         */
                        bool completed = true;
                        FullLease fl;
                        if (_leases[reqs[i].Keys[0]].TryPeek(out var lease) && lease.Tm_name == _name)
                        {
                            fl = (FullLease)lease;
                            if (reqs[i].SubGroup(fl))
                            {
                                foreach (string key in fl.Keys)
                                {
                                    // Verify if we have all the lease on the top
                                    if (_leases[key].TryPeek(out var l))
                                    {
                                        if (fl != (FullLease)l)
                                        {
                                            completed = false;
                                            break;
                                        }
                                    }
                                    else
                                    {
                                        completed = false;
                                        break; // Theoredicaly impossible
                                    }
                                }
                            }
                        }
                        else
                        {
                            completed = false;
                        }
                        if (!completed)
                        {
                            _lmcontact.RequestLease(reqs[i].Keys);
                            reqs[i].Situation = leaseRequested.Yes;
                            continue;                        
                        }
                        /*    0         1  
                         *   <A,C> <X,Z>   <A,B> <X> <Z>
                         * A 11
                         * B
                         * C 11
                         * 
                         * If the lease intersects any other lease before you, it's impossible to reach you
                         */
                        fl =(FullLease)_leases[reqs[i].Keys[0]].Peek();
                        for (int j = 0; j < i; j++)
                        {
                            /* If the lease intersects someone before you not being a subgroup of the lease:
                             * - the lease is residual and was deleted (for conflicts with the new leases)
                             * - the lease was used for a transaction before you and got deleted (for conflicts with the new leases)
                             * - the lease is there but there is a subgroup of it before you
                             * - your turn
                             */
                            if (fl.Intersection(reqs[j])) //ERROR:  intersessao da lease com os J
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
                Console.WriteLine(reqs[0].Situation);
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
                    List<DadIntProto> reply = new List<DadIntProto>();

                    // Tries to propagate
                    int err = Request(reqs[i].Reads, reqs[i].Writes, ref reply);

                    // Try again, only one time
                    if (err == -2) err = Request(reqs[i].Reads, reqs[i].Writes, ref reply);


                    // Moves it to the pickup waiting place
                    _reqList.move(reqs[i].Transaction_number, reply, err);

                    // Remove from the request list
                    _reqList.remove(i);


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
        public int Request(List<string> reads, List<DadIntProto> writes, ref List<DadIntProto> reply)
        {
            if (!_tmContact.BroadCastChanges(writes,_name)) return -2;
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
        /// <returns></returns>
        public bool Write(List<DadIntTmProto> writes, string tm_name)
        {
            lock (this)
            {
                if (tm_name != null && writes.Count != 0)
                {
                    if (!LeaseRemove(writes[0].Key, tm_name)) return false; // Always goes well because they are all correct servers
                }
                foreach (DadIntTmProto write in writes) { _store[write.Key] = write.Value; }
            }
            return true;
        }
        /// <summary>
        /// Removes Leases marked to be removed (of other Tm)
        /// </summary>
        /// <param name="firstkey"></param>
        /// <param name="tm_name"></param>
        /// <returns></returns>
        public bool LeaseRemove(string firstkey, string tm_name)
        {
            FullLease fl;
            if(_leases[firstkey].TryPeek(out var lease)) fl = (FullLease)lease; // Gets FullLease
            else return false; // Theoretically never possible

            // Verify all entries
            foreach (string key in fl.Keys)
            {
                if (_leases[key].TryPeek(out var result))
                {
                    if (result.Tm_name != tm_name) return false; // Theoretically never possible
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
        /// <summary>
        /// Deletes all residual leases
        /// </summary>
        /// <param name="firstKeys"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool DeleteResidual(List<string> firstKeys, string name)
        {
            foreach (string key in firstKeys)
            {
                LeaseRemove(key, name);
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

            //eliminar leases residuais
            lock (this)
            {
                DeleteResidualLeases(leases, epoch, out int numberOfSameEpoch);
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
                RaiseEpochOfNewRequests(leases,epoch, numberOfSameEpoch);
                _reqList.incrementEpoch();
            }
        }

        private void RaiseEpochOfNewRequests(List<FullLease> newLeases, int epoch, int numberOfSameEpoch)
        {
            /* EX: We have to raise the epoch number of the requests that did not receive a lease in this epoch 
             * EpochM: 0     0    0     0        1     1     1    1         1      1
             *       <A,I> <A,B> <P> <A,B,K>  <A,B,C> <C,D> <X> <X,K>    <A,B,C> <C,D>
             *          1º epoch             | 2º epoch               |  3º epoch 
             */
            int count = numberOfSameEpoch;
            // Count all of our new leases
            foreach (FullLease fullLease in newLeases)
            {
                if (fullLease.Tm_name == _name) count++;
            }
            // The requests saying Yes (from the new leases or old  leases) for this epoch are de sum of new leases for this Tm and numberOfSameEpoch
            foreach (Request rq in _reqList.GetRequests())
            {
                /* EX: We have to raise the epoch number of the requests that did not receive a lease in this epoch 
                * EpochM:    1     1     1    1         2      2
                *         <A,B,C> <C,D> <X> <X,K>    <A,B,C> <C,D>
                *            2º epoch               |  3º epoch 
                */
                if (rq.Situation == leaseRequested.Yes && rq.Epoch == epoch - 1)
                {
                    if (count != 0) count--;
                    else
                    {
                        // when count is 0 we know that we do not have more leases for this epoch for new requests 
                        // does not trouble leases that will be used for various requests, because the next request after the one that will use the lease right now 
                        // will be marked as maybe, only after the one right now finishies, the follow request can turn into a Yes (if possible)
                        rq.Epoch++;
                    }
                }
                else if (rq.Situation != leaseRequested.Yes && rq.Epoch == epoch - 1) // All requests not marked with Yes will increase their epoch
                {
                    rq.Epoch++;
                }
            }


        }


        private void DeleteResidualLeases(List<FullLease> newLeases, int epoch, out int numberOfSameEpoch)
        {
            numberOfSameEpoch = 0;
            List<FullLease> residual = new List<FullLease>();
            foreach (FullLease fl in _fullLeases) { residual.Add(fl); }

            /* EX: We dont consider the new leases arriving
             * EpochM: 0     0    0     0        1     1     1    1         1      1
             *       <A,I> <A,B> <P> <A,B,K>  <A,B,C> <C,D> <X> <X,K>    <A,B,C> <C,D>
             *          1º epoch             | 2º epoch               |  3º epoch
             *      A   10 11 13             | 16                     |     19      
             *      B   11 13                | 16                     |     19
             *      K   13                   |                   18   |
             *      I   10                   |                        |
             *      P   12                   |                        |
             *      C   14                   | 16     17              |     19   20
             *      D   14                   |        17              |          20
             *      X   15                   |                   18   |
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
            * EpochM: 0     0    0     0        1      1    1    1       1      1
            *       <A,I> <A,B> <P> <A,B,K>  <A,B,C> <C,D> <X> <X,K>  <A,B,C> <C,D>
            *          1º epoch
            *      A   
            *      B     
            *      K   
            *      I   
            *      P   12
            *      C   14      
            *      D   14  
            *      X   15
            */
            List<Request> epochRequest = null;
            foreach (FullLease fl in residual)
            {
                foreach (Request rq in _reqList.GetRequests())
                {
                    if (rq.Situation == leaseRequested.Yes)
                    {
                        // We test if the epoch number is smaller (<= epoch-2) than our new epoch number
                        if (epoch != 1 && rq.Epoch < epoch-1)
                        {
                            /* Only looks into the epoch-2 requests left
                             * EpochM: 0     0    0     0    
                             *       <A,I> <A,B> <P> <A,B,K> 
                             *          1º epoch
                             *      P   12
                             *      C   14      
                             *      D   14
                             *      X   15
                             */
                            // Is the only possible request to use that list, because we already removed all leases that had intersections with someone
                            if (rq.SubGroup(fl))
                            {
                                /* From the remaining leases we remove the ones that have a request using them
                                 * EpochM: 0     0    0     0   
                                 *       <A,I> <A,B> <P> <A,B,K>
                                 *          1º epoch  
                                 *      C   14      
                                 *      D   14  
                                 *      X   15
                                 */

                                // If someone will use it it's not residual then remove (in this case the lease for P)
                                
                                residual.Remove(fl);
                            }
                        }
                        else if (rq.Epoch == epoch - 1)
                        {
                            if(epochRequest == null) epochRequest = new List<Request>();
                            epochRequest.Add(rq);
                            /* Only looks into the epoch-1 requests left
                             * EpochM:    1      1    1    1       1      1
                             *         <A,B,C> <C,D> <X> <X,K>  <A,B,C> <C,D>
                             *          1º epoch
                             *      C   14      
                             *      D   14  
                             *      X   15
                             */
                            
                            if (rq.SubGroup(fl))
                            {
                                bool intersect = false;
                                foreach (Request rq2 in epochRequest)
                                {
                                    if (fl.Intersection(rq2)) intersect = true;
                                }
                                if (!intersect)
                                {
                                    residual.Remove(fl);
                                    // Number of requests that have epochNumber -1 increases (used for calculating our epochs received)
                                    numberOfSameEpoch++;
                                }
                                /* We remove any lease that will be used and does not have a intersection behind (from another epoch-1 request)
                                 * EpochM:    1      1    1    1       1      1
                                 *         <A,B,C> <C,D> <X> <X,K>  <A,B,C> <C,D>
                                 *          1º epoch
                                 *      C   14      
                                 *      D   14  
                                 *      
                                 *      Lease from Tm1 lease 4 is residual and will be terminated with a broadcast
                                 */
                            }
                        }
                    }
                }
            }
            // We will send the first key of every list to delete to all other Tm's, because they can get the full lease that way
            List<string> firstKeys = new List<string>();
            foreach (FullLease fl in residual) if (fl.Intersection(newLeases)) firstKeys.Add(fl.Keys[0]);
            if(_tmContact.DeleteResidualKeys(firstKeys, _name)) // Theoredicaly never fails
            {
                foreach (FullLease fl in residual)
                {
                    // This decreases the number of FullLeases that we have to test if it's residual
                    if (fl.Intersection(newLeases))
                    {
                        // Eliminates our Lease from all the _leases queues and the _fullLease list
                        LeaseRemove(fl.Keys[0], _name);
                    }
                }
            }
            
        }
    }
}