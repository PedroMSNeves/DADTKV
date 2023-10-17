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
        private string _name; // Server's name
        private Dictionary<string, int> _store; // DadInts store
        private Dictionary<string, Queue<Lease>> _leases; // key -> queue (of leases) store
        private List<FullLease> _fullLeases; // Leases store
        private RequestList _reqList; // Where Requests are stored
        private TmContact _tmContact; // Used to propagate things to other TM

        public Store(string name, List<string> tm_urls, List<string> lm_urls)
        {
            _reqList = new RequestList(10,name, lm_urls); // Maximum of requests waiting allowed and information necessary to ask for new leases
            _name = name;
            _store = new Dictionary<string, int>();
            _leases = new Dictionary<string, Queue<Lease>>(); // Stores the leases in a Lifo associated with a key (string)
            _tmContact = new TmContact(tm_urls);
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
            int tnum;
            Request req;
            lock (this) 
            {
                // Verifies if it has valid reads and writes
                foreach (string read in reads)
                {
                    if (!_store.ContainsKey(read)) { return -1; } // maybe meter timeout depois renbenta
                }
                foreach (DadIntProto write in writes)
                {
                    if (!_store.ContainsKey(write.Key)) 
                    { 
                        _store[write.Key] = -1; //default value (will never be read) (maybe nao ness)
                        _leases[write.Key] = new Queue<Lease>();  
                    } 
                }
                // Creates Request
                req = new Request(reads, writes);
                // Verifies if it can use an existing lease
                bool lease = verifyLease(req);
                // Inserts the request into the queue, if no lease could be used, asks for a new one
                tnum = _reqList.insert(req,lease);
            }
            return tnum;
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
        /// <summary>
        /// Used to see if a existing lease is compatible with us and can be used by us
        /// </summary>
        /// <param name="rq"></param>
        /// <param name="reqs"></param>
        /// <returns></returns>
        public int CompatibleLease (Request rq, List<Request> reqs)
        {
            FullLease fl = null;
            // If no lease on the queue of one of the keys from the request, then no lease will work
            foreach (Lease lease  in _leases[rq.Keys[0]])
            {
                // If marked to end, cant add more to it
                if (lease.End) continue;
                fl = (FullLease)lease;
                // We can only use leases that are for our TM
                if(lease.Tm_name == _name)
                {
                    // Sees if we can use the lease
                    if (rq.SubGroup(fl))
                    {
                        bool found = false;
                        Request lastrq = null;
                        // Look for the last person using this lease
                        foreach (Request req in reqs)
                        {
                            if (req.Lease_number == fl.Lease_number) lastrq = req;
                        }
                        // If no one will use it, we have to see all the requestList
                        if (lastrq == null) found = true;
                        // Tries to find an intersection between this lease's last request and us
                        foreach (Request req in reqs)
                        {
                            if (lastrq != null && lastrq.Transaction_number == req.Transaction_number) found = true;
                            else if (found)
                            {
                                // If we find an intersection, we cant use this lease
                                if (fl.Intersection(req)) return -1;
                            }
                        }
                        return fl.Lease_number;
                    }
                }
            }
            return -1;
        }
        /// <summary>
        /// Used to see if a requested lease is compatible with us and can be used by us
        /// </summary>
        /// <param name="rq"></param>
        /// <param name="reqs"></param>
        /// <returns></returns>
        public int CompatibleLeaseNotArrived(Request rq, List<Request> reqs)
        {
            int leaseNr = -1;
            Request requestLease = null;
            Request lastrq = null;
            bool found = false;

            foreach (Request req in reqs)
            {
                // Sees if we can use the lease
                if (rq.SubGroup(req)) 
                { 
                    // If we find a newer lease that we are a subgroup we store it
                    if(leaseNr != req.Lease_number)
                    {
                        leaseNr = req.Lease_number;
                        requestLease = req;
                    }
                }
                // If this request is the first one to use this requested lease, we store his info
                if (req.Lease_number == leaseNr) lastrq = req;
            }
            // If leaseNr == -1, no requested lease applied to us
            if (leaseNr == -1) return -1;
            if (requestLease == null) return -1;

            // Tries to find an intersection between this lease's last request and us
            foreach (Request req in reqs)
            {
                if (lastrq != null && lastrq.Transaction_number == req.Transaction_number) found = true;
                else if (found)
                {
                    // If we find an intersection, we cant use this lease
                    if (requestLease.Intersection(req)) return -1;
                }
            }
            return leaseNr;
        }

        //////////////////////////////////////////////USED BY MAINTHREAD////////////////////////////////////////////////////////////
        /// <summary>
        /// Verifies if a request can use an existing/requested lease
        /// </summary>
        /// <param name="req"></param>
        /// <returns></returns>
        public bool verifyLease(Request req)
        {
            // We go look for the requests not fullfield
            List<Request> reqs = _reqList.GetRequestsNow();
            // We try to find a existing lease compatible with us
            int lease_number = CompatibleLease(req, reqs);
            if (lease_number != -1) 
            {
                req.Lease_number = lease_number;
                return true;
            }
            // We try to find a requested lease compatible with us
            lease_number = CompatibleLeaseNotArrived(req, reqs);
            if (lease_number != -1) 
            {
                req.Lease_number = lease_number;
                return true;
            }
            // If no lease can be used, we return false, signaling to create a new lease for this request
            return false;
        }
        /// <summary>
        /// Used to see if our lease is at the top of the queue at all the lease's keys
        /// </summary>
        /// <param name="req"></param>
        /// <param name="fl"></param>
        /// <returns></returns>
        public bool completeLease(Request req, out FullLease fl)
        {
            fl = null;
            // If no lease on the queue of one of the keys from the request, or its not for our TM, then the lease is not complete
            if (_leases[req.Keys[0]].TryPeek(out var lease) && lease.Tm_name == _name)
            {
                fl = (FullLease)lease;
                // Confirm that it's our lease
                if (fl.Lease_number == req.Lease_number)
                {
                    foreach (string key in fl.Keys)
                    {
                        // Verify if we have all parts of the lease at the top
                        if (_leases[key].TryPeek(out var l))
                        {
                            // The lease at the top of a queue is not with our lease_number or our tm_name then we dont have a complete lease
                            if (fl.Lease_number != l.Lease_number || l.Tm_name != _name) return false;
                        }
                        else return false;
                    }
                    return true;
                }
                else return false;
            }
            else return false;
        }
        /// <summary>
        /// Tries to execute the requests
        /// </summary>
        public void Execute()
        {
            int i = 0;
            // Gets requests (waits until there is at least one)
            List<Request> reqs = _reqList.GetRequests();
            lock (this)
            {
                while (true)
                {
                    FullLease fl;
                    List<DadIntProto> reply = new List<DadIntProto>();
                    // Sees if it has the needed complete lease
                    if (!completeLease(reqs[i], out fl))
                    {
                        // Sequencial requests
                        break;
                    }

                    bool close = false;
                    // Sees if we have to close the lease
                    if (fl.End)
                    {
                        close = true;
                        for (int j = i+1; j < reqs.Count; j++)
                        {
                            // If someone will still use this lease then we dont close it
                            if (reqs[j].Lease_number == reqs[i].Lease_number)
                            {
                                close = false;
                                break;
                            }
                        }  
                    }
                    // Tries to propagate
                    int err = Request(reqs[i].Reads, reqs[i].Writes, fl, reqs[i].Epoch + 1, ref reply, close);

                    // Removed the lease if needed
                    if (close) LeaseRemove(fl.Keys[0], _name);

                    // Moves it to the pickup waiting place
                    _reqList.move(reqs[i].Transaction_number, reply, err);

                    // Remove from the request list
                    _reqList.remove(i);
                    if (i >= reqs.Count) break;
                }
            }
        }
        /// <summary>
        /// Used to propagate writes and close leases on other TM's
        /// </summary>
        /// <param name="reads"></param>
        /// <param name="writes"></param>
        /// <param name="fl"></param>
        /// <param name="epoch"></param>
        /// <param name="reply"></param>
        /// <param name="close"></param>
        /// <returns></returns>
        public int Request(List<string> reads, List<DadIntProto> writes, FullLease fl, int epoch, ref List<DadIntProto> reply, bool close)
        {
            // If we have writes we use them to identify the lease to close (if needed)
            if(writes.Count != 0)
            {
                // If lease is to close, we send the name, if not we send "_"
                if (close)
                {
                    if (!_tmContact.BroadCastChanges(writes, _name, epoch)) return -2;
                }
                else
                {
                    if (!_tmContact.BroadCastChanges(writes, "_", epoch)) return -2;
                }
            }
            else if (close && reads.Count != 0)
            {
                // We send the first read key to end the lease (if needed)
                _tmContact.DeleteResidualKeys(new List<string> { fl.Keys[0] }, _name, epoch);
            }
                
            foreach (string key in reads) reply.Add(new DadIntProto { Key = key, Value = _store[key] }); // Does the reads
            foreach (DadIntProto write in writes) _store[write.Key] = write.Value; // Does the writes
            return 0;
        }
        /// <summary>
        /// Remove the Lease that is on top of the queue for the first key
        /// </summary>
        /// <param name="firstkey"></param>
        /// <param name="tm_name"></param>
        /// <returns></returns>
        public bool LeaseRemove(string firstkey, string tm_name)
        {
            FullLease fl;
            if (_leases[firstkey].TryPeek(out var lease)) fl = (FullLease)lease; // Gets FullLease
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
        //////////////////////////////////////////////USED BY BROADSERVICE////////////////////////////////////////////////////////////
        /// <summary>
        /// Used by BroadService
        /// Writes the incoming changes and removes a lease if needed        
        /// </summary>
        /// <param name="writes"></param>
        /// <param name="tm_name"></param>
        /// <param name="epoch"></param>
        /// <returns></returns>
        public bool Write(List<DadIntTmProto> writes, string tm_name, int epoch)
        {
            lock (this)
            {
                if (tm_name != "_")
                {
                    if (!LeaseRemove(writes[0].Key, tm_name, epoch)) return false; // Always goes well because they are all correct servers
                }
                foreach (DadIntTmProto write in writes) { _store[write.Key] = write.Value; }
                Console.WriteLine("EnD write");
            }
            return true;
        }
        /// <summary>
        /// Deletes all residual leases
        /// </summary>
        /// <param name="firstKeys"></param>
        /// <param name="name"></param>
        /// <param name="epoch"></param>
        /// <returns></returns>
        public bool DeleteResidual(List<string> firstKeys, string name, int epoch)
        {
            foreach (string key in firstKeys)
            {
                LeaseRemove(key, name, epoch);
            }
            return true;
        }
        /// <summary>
        /// Remove the Lease that is on top of the queue for the first key (waits if this tm did not receive this epoch's leases)
        /// </summary>
        /// <param name="firstkey"></param>
        /// <param name="tm_name"></param>
        /// <param name="epoch"></param>
        /// <returns></returns>
        public bool LeaseRemove(string firstkey, string tm_name, int epoch)
        {
            FullLease fl;
            while (_reqList.get_epoch() < epoch) Monitor.Wait(this);
            if (_leases[firstkey].TryPeek(out var lease)) fl = (FullLease)lease; // Gets FullLease
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

        //////////////////////////////////////////////USED BY LSERVICE////////////////////////////////////////////////////////////
        /// <summary>
        /// Used By LService
        /// Receives new leases for the epoch
        /// </summary>
        /// <param name="leases"></param>
        /// <param name="epoch"></param>
        public void NewLeases(List<FullLease> leases, int epoch)
        {
            lock (this)
            {
                //foreach (FullLease lease in _fullLeases) foreach (string key in lease.Keys) Console.WriteLine(key + ":" + lease.End);
                //foreach (FullLease lease in leases) foreach (string key in lease.Keys) Console.WriteLine(key + ":" + lease.End);

                // Deletes residual leases
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
                                    // Marks for end all our leases that have conflicts
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
                // Awakes possible waiting propagation thread
                Monitor.PulseAll(this);
                foreach (FullLease lease in _fullLeases) foreach (string key in lease.Keys) Console.WriteLine(key + ":" + lease.End);
            }
        } 

        private void RaiseEpochOfNewRequests(List<FullLease> newLeases, int epoch, int numberOfSameEpoch)
        {
            // possivelmente procurar a lease number das novas newleases e comparar com os request, os que nao tiverem la sobe uma 
            // possivelmente faz epoch recebida -1 e guarda no request
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
                if (rq.Epoch == epoch - 1)
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
            }
        }


        private void DeleteResidualLeases(List<FullLease> newLeases, int epoch, out int numberOfSameEpoch)
        {
            // refazer funcao
            /*
             * Lista de todas as leases
             *  -para cada uma delas verificar se estão marcadas para end (se sao nossas) e se fazem intersessao com as novas 
             */
            numberOfSameEpoch = 0;
            List<FullLease> residual = new List<FullLease>();
            foreach (FullLease fl in _fullLeases) { if(fl.Tm_name == _name) residual.Add(fl); }

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
            foreach (FullLease fullLease in residual)
            {
                Console.WriteLine("FL "+ fullLease.Tm_name +": ");
                foreach (string s in fullLease.Keys) Console.WriteLine(s + " ");
                Console.WriteLine();

            }
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
                        if (epochRequest == null) epochRequest = new List<Request>();
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
            Console.WriteLine("RESIDUAL END");
            foreach (FullLease fullLease in residual)
            {
                Console.WriteLine("FL " + fullLease.Tm_name + ": ");
                foreach (string s in fullLease.Keys) Console.WriteLine(s + " ");
                Console.WriteLine();

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
            if(_tmContact.DeleteResidualKeys(firstKeys, _name, epoch)) // Theoredicaly never fails
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