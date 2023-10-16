using DADTKV_TM.Contact;
using DADTKV_TM.Structs;
using System;
using System.Collections.Generic;

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
        private bool possible_execute;

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
            possible_execute = false;
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

        public void subgroup(List<Request> reqs, int i, FullLease fl, ref bool maybeOnly)
        {
            for (int j = 0; j < i; j++)
            {
                if (fl != null && reqs[j].SubGroup(fl)) // <x,k>S <abc>S <cd>S <x,k>T <x,d>N <x>N
                {
                    if (reqs[j].Situation != leaseRequested.No)
                    {
                        reqs[i].Situation = leaseRequested.Maybe;
                        maybeOnly = true;
                        break;
                    }
                    else reqs[i].Situation = leaseRequested.No;
                }
            }
        }
        public void subgroup(List<Request> reqs, int i, ref bool maybeOnly)
        {
            for (int j = 0; j < i; j++)
            {
                if (reqs[i].SubGroup(reqs[j])) // <x,k>S <abc>S <cd>S <x,k>T <x,d>N <x>N
                {
                    if (reqs[j].Situation != leaseRequested.No)
                    {
                        reqs[i].Situation = leaseRequested.Maybe;
                        maybeOnly = true;
                        break;
                    }
                    else reqs[i].Situation = leaseRequested.No;
                }
            }
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
                //foreach(Request r in reqs) foreach (DadIntProto key in r.Writes) Console.WriteLine(r.Transaction_number + ":" + key +":" + r.Situation);
                bool maybeOnly = false;
                for (int i = 0; i< reqs.Count; i++)
                {
                    if (reqs[i].Situation == leaseRequested.No)
                    {

                        /*
                         * Verificar se temos a lease atualmente
                         * Verificar se é um subgrupo de um request marcado a S
                         * depois da veri, se ainda a N pedir se nao tiver com o maybe antes
                         * Marca a T
                         */
                        //Verify if there is a completed lease
                        bool completed = true;
                        FullLease fl = null;
                        if (_leases[reqs[i].Keys[0]].TryPeek(out var lease) && lease.Tm_name == _name)
                        {
                            
                            fl = (FullLease) lease;
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
                            else completed = false;
                        }
                        else completed = false;
                        if (completed)
                        {
                            if (!maybeOnly)
                            {
                                // se ha alguem que usa a lease antes de nos
                                // if sim fica a Maybe
                                // if nao fica a Yes se nao tiver nenhuma intersessao com outro request anterior ai fica a N  (para manter ordem total), nao ness, se houvesse intercessao ela estaria marcada para end e fecharia quando acabasse ou era residual e foi apagada
                                reqs[i].Situation = leaseRequested.Yes;
                                subgroup(reqs, i, fl, ref maybeOnly);
                                possible_execute = true;
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

                        // vai ter subreposicao com parte da veri de cima, mas tem casos diff, os que ainda nao temos a lease, mas ja pedimos
                        subgroup(reqs, i, ref maybeOnly);

                        // to have MAYBE TRUE we can store all maybe keys and see if we intersect some if not, we can turn true 
                        // right now we cant have Maybe true, only maybe maybe or maybe no
                        if (!maybeOnly && reqs[i].Situation == leaseRequested.No)
                        {
                            _lmcontact.RequestLease(reqs[i].Keys);

                            reqs[i].Situation = leaseRequested.Yes;
                            possible_execute = true;
                        }                    
                    }
                    else if (reqs[i].Situation == leaseRequested.Maybe) maybeOnly = true;
                }
                if (possible_execute) Execute(ref reqs);
            }
        }
        /// <summary>
        /// Tries to execute the requests marked with Yes
        /// </summary>
        /// <param name="reqs"></param>
        public void Execute(ref List<Request> reqs)
        {
            int i = 0;
            //possible_execute = false;
            
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
                        if (!reqs[i].SubGroup(fl))
                        {
                            i++;
                            if (i >= reqs.Count) break;
                            continue; /* If he is not a subGroup of the lease then:
                                       * He is still waiting for is lease to arive and this is an old lease
                                       * Or some other Transaction is waiting for some other Tm to release is key */
                        }
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
                        if (diff)
                        {
                            i++;
                            if (i >= reqs.Count) break;
                            continue;
                        }
                    }
                    else
                    {
                        i++;
                        if (i >= reqs.Count) break;
                        continue;/* If the lease is not of our Tm or the queue is empty:
                                  * The new set of leases did not arrive yet
                                  * Or is waiting for another Tm to end is lease */
                    }
                    List<DadIntProto> reply = new List<DadIntProto>();

                    // Tries to propagate
                    int err = Request(reqs[i].Reads, reqs[i].Writes, fl, ref reply);

                     // Try again, only one time
                     if (err == -2) err = Request(reqs[i].Reads, reqs[i].Writes, fl, ref reply);

                    verifyMaybes(fl,reqs,i);

                    LeaseRemove(fl.Keys[0], _name, false);


                    // Moves it to the pickup waiting place
                    _reqList.move(reqs[i].Transaction_number, reply, err);

                    // Remove from the request list
                    _reqList.remove(i);
                }
                i++;
                if (i >= reqs.Count)  break;
            }
        }
        public void verifyMaybes(FullLease fl, List<Request> reqs, int i)
        {
            //se for para terminar as outras ficam a nao
            if (fl.End)
            {
                for(int j  = i; j < reqs.Count; j++)
                {
                    if (reqs[j].SubGroup(fl)) reqs[j].Situation = leaseRequested.No;
                }
                return;
            }
            //foreach(Request r in reqs) foreach (DadIntProto key in r.Writes) Console.WriteLine(r.Transaction_number + ":" + key +":" + r.Situation);
            
            //se nao for para apagar mete sim na proxima
            for (int j = i+1; j < reqs.Count; j++)
            {
                if (reqs[j].SubGroup(fl))
                {
                    //maybe verificar intercessoes entre i+1 e j

                    reqs[j].Situation = leaseRequested.Yes;
                    //foreach (Request r in reqs) foreach (DadIntProto key in r.Writes) Console.WriteLine(r.Transaction_number + ":" + key + ":" + r.Situation);
                    return;
                }
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
        public int Request(List<string> reads, List<DadIntProto> writes, FullLease fl, ref List<DadIntProto> reply)
        {
            if(writes.Count != 0)
            {
                if (!_tmContact.BroadCastChanges(writes, _name)) return -2;
            }
            else
            {
                if (fl.End && reads.Count != 0)
                {
                    if (_tmContact.DeleteResidualKeys(new List<string> { fl.Keys[0] }, _name)) // Apaga a lease em todos
                    {
                        LeaseRemove(fl.Keys[0], _name, false); // Apaga a lease para nos
                    }
                }
            }
                
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
                    if (!LeaseRemove(writes[0].Key, tm_name, false)) return false; // Always goes well because they are all correct servers
                }
                foreach (DadIntTmProto write in writes) { _store[write.Key] = write.Value; }
                Console.WriteLine("EnD write");
            }
            return true;
        }
        /// <summary>
        /// Removes Leases marked to be removed (of other Tm)
        /// </summary>
        /// <param name="firstkey"></param>
        /// <param name="tm_name"></param>
        /// <returns></returns>
        public bool LeaseRemove(string firstkey, string tm_name,bool residual)
        {
            FullLease fl;
            if (_leases[firstkey].TryPeek(out var lease)) fl = (FullLease)lease; // Gets FullLease
            else return false; // Theoretically never possible
            if (!residual && fl.End == false) return true;
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
            Console.WriteLine("delete");
            foreach (string key in firstKeys)
            {
                LeaseRemove(key, name, true);
            }
            Console.WriteLine("deleted");

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
                //foreach (FullLease lease in _fullLeases) foreach (string key in lease.Keys) Console.WriteLine(key + ":" + lease.End);
                //foreach (FullLease lease in leases) foreach (string key in lease.Keys) Console.WriteLine(key + ":" + lease.End);

                possible_execute = true;
                DeleteResidualLeases(leases, epoch, out int numberOfSameEpoch);
                Console.WriteLine("added residual");

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
                Console.WriteLine("added end");

                RaiseEpochOfNewRequests(leases,epoch, numberOfSameEpoch);
                Console.WriteLine("added raised");

                _reqList.incrementEpoch();
                //fim
                 Console.WriteLine("added LEASES");
                //foreach (FullLease lease in _fullLeases) foreach (string key in lease.Keys) Console.WriteLine(key + ":" + lease.End);
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
            foreach (Request rq in _reqList.GetRequestsNow())
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
                foreach (Request rq in _reqList.GetRequestsNow())
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
            foreach (FullLease fl in residual)
            {
                if (fl.Intersection(newLeases)) 
                {
                    fl.End = true;
                    firstKeys.Add(fl.Keys[0]); 
                }
            }
            if (firstKeys.Count == 0) return;
            if(_tmContact.DeleteResidualKeys(firstKeys, _name)) // Theoredicaly never fails
            {
                foreach (FullLease fl in residual)
                {
                    // This decreases the number of FullLeases that we have to test if it's residual
                    if (fl.Intersection(newLeases))
                    {
                        // Eliminates our Lease from all the _leases queues and the _fullLease list
                        LeaseRemove(fl.Keys[0], _name,true);
                    }
                }
            }
            
        }
    }
}