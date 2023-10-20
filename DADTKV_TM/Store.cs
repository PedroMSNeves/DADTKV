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
        private bool possibleResiduals = false;
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
            Console.WriteLine("VerifyAndInsertRequest");
            lock (this) 
            {
                // Verifies if it has valid reads and writes
                foreach (string read in reads)
                {
                    // A non existing read returns a null value
                    if (!_leases.ContainsKey(read)) 
                    {
                        _leases[read] = new Queue<Lease>();
                    } 
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
                Console.WriteLine("VerifyLease");
                bool lease = verifyLease(req);
                Console.WriteLine("VerifyLease END");

                // Inserts the request into the queue, if no lease could be used, asks for a new one
                Console.WriteLine("Insert");
                tnum = _reqList.insert(req,lease,this);
                Console.WriteLine("Insert END");

            }
            Console.WriteLine("VerifyAndInsertRequest END");
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
            Console.WriteLine("CompatibleLease");
            int lease_number = CompatibleLease(req, reqs);
            Console.WriteLine("CompatibleLease END");
            if (lease_number != -1) 
            {
                req.Lease_number = lease_number;
                return true;
            }
            // We try to find a requested lease compatible with us
            Console.WriteLine("CompatibleLeaseNotArrived");
            lease_number = CompatibleLeaseNotArrived(req, reqs);
            Console.WriteLine("CompatibleLeaseNotArrived END");
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
            //Console.WriteLine("Execute");
            //Console.WriteLine("GetRequests");
            

            lock (this)
            {
                // Gets requests (waits until there is at least one)
                List<Request> reqs = _reqList.GetRequestsNow(); 

                if (reqs.Count == 0) return;
                while (true)
                {
                    FullLease fl;
                    List<DadIntProto> reply = new List<DadIntProto>();
                    // Sees if it has the needed complete lease
                    if (!completeLease(reqs[i], out fl))
                    {
                        // Sequencial requests
                        Console.WriteLine("NO LEASE: WE need " + _name + " "+reqs[i].Lease_number);
                        foreach (FullLease ff in _fullLeases.ToList())
                        {
                            Console.Write("FL: " + ff.Tm_name + " " + ff.Lease_number + " " + ff.End + " KEYS: ");
                            foreach (string s in ff.Keys) Console.Write(s + " ");
                            Console.WriteLine();
                        }
                        break;
                    }

                    // Tries to propagate
                    Console.WriteLine("Request");

                    int err = Request(reqs[i].Reads, reqs[i].Writes, fl, reqs[i].Epoch + 1, ref reply);
                    Console.WriteLine("Request END");

                    // Moves it to the pickup waiting place
                    Console.WriteLine("Move");

                    _reqList.move(reqs[i].Transaction_number, reply, err);
                    Console.WriteLine("Move END");

                    // Remove from the request list
                    Console.WriteLine("Remove");

                    _reqList.remove(i,this);
                    Console.WriteLine("Remove END");

                    if (i >= reqs.Count) break;
                }
            }
            Console.WriteLine("Execute END");

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
        public int Request(List<string> reads, List<DadIntProto> writes, FullLease fl, int epoch, ref List<DadIntProto> reply)
        {
            // If we have writes we use them to identify the lease to close (if needed)
            if(writes.Count != 0)
            {
                // If lease is to close, we send the name, if not we send "_"
                Console.WriteLine("BroadCastChanges");
                if (!_tmContact.BroadCastChanges(writes, _name, epoch, this)) return -2;
                Console.WriteLine("BroadCastChanges END");

            }
            Console.WriteLine("Reads");

            foreach (string key in reads)
            { 
                if(!_store.ContainsKey(key)) reply.Add(new DadIntProto { Key = key, Value = 0, InvalidRead = true }); // Does the reads
                else reply.Add(new DadIntProto { Key = key, Value = _store[key], InvalidRead = false }); // Does the reads

            }
            Console.WriteLine("Reads END");
            Console.WriteLine("Writes");
            foreach (DadIntProto write in writes) _store[write.Key] = write.Value; // Does the writes
            Console.WriteLine("Writes END");
            possibleResiduals = true;

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
            Console.WriteLine("Write");

            lock (this)
            {
                foreach (DadIntTmProto write in writes) { _store[write.Key] = write.Value; }
            }
            Console.WriteLine("Write END");

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
                Console.WriteLine("LeaseRemove");
                LeaseRemove(key, name, epoch);
                Console.WriteLine("LeaseRemove END");
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
            Console.WriteLine("NewLeases");

            lock (this)
            {
                //foreach (FullLease lease in _fullLeases) foreach (string key in lease.Keys) Console.WriteLine(key + ":" + lease.End);
                //foreach (FullLease lease in leases) foreach (string key in lease.Keys) Console.WriteLine(key + ":" + lease.End);
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
                // Deletes residual leases
                Console.WriteLine("DeleteResidualLeases");

                //DeleteResidualLeases(epoch);
                possibleResiduals = true;
                Console.WriteLine("DeleteResidualLeases END");

                // Raises epoch of new requests
                Console.WriteLine("RaiseEpochOfNewRequests");

                RaiseEpochOfNewRequests(epoch);
                Console.WriteLine("RaiseEpochOfNewRequests END");

                _reqList.incrementEpoch();
                // Awakes possible waiting propagation thread
                Monitor.PulseAll(this);
                foreach (FullLease lease in _fullLeases) foreach (string key in lease.Keys) Console.WriteLine(key + ":" + lease.End);
            }

            Console.WriteLine("NewLeases END");
            //Console.ReadKey();
        }

        private void RaiseEpochOfNewRequests(int epoch)
        {
            /* EX: We have to raise the epoch number of the requests that did not receive a lease in this epoch 
             * EpochM: 0     0    0     0        1     1     1    1         1      1
             *       <A,I> <A,B> <P> <A,B,K>  <A,B,C> <C,D> <X> <X,K>    <A,B,C> <C,D>
             *          1º epoch             | 2º epoch               |  3º epoch 
             */
            foreach(Request rq in _reqList.GetRequestsNow())
            {
                bool exists = false;
                foreach (FullLease fl in _fullLeases)
                {
                    if (rq.Lease_number == fl.Lease_number)
                    {
                        exists = true;
                        break;
                    }
                }
                if (!exists) rq.Epoch = epoch;
            }
            /* EX: We have to raise the epoch number of the requests that did not receive a lease in this epoch 
             * EpochM: 0     0    0     0        1     1     1    1         2      2
             *       <A,I> <A,B> <P> <A,B,K>  <A,B,C> <C,D> <X> <X,K>    <A,B,C> <C,D>
             *          1º epoch             | 2º epoch               |  3º epoch 
             */
        }
        public void removeResidual()
        {
            lock (this)
            {
                //if (!possibleResiduals) return;
                bool used = false;
                int maxEpoch = 0;
                List<string> firstKeys = new List<string>();
                foreach (FullLease fullLease in _fullLeases.ToList())
                {
                    if(fullLease.Tm_name == _name && fullLease.End)
                    {
                        foreach (Request rq in _reqList.GetRequestsNow())
                        {
                            if (fullLease.Lease_number == rq.Lease_number )
                            {
                                //if (fullLease.Epoch > maxEpoch) maxEpoch = fullLease.Epoch;
                                used = true;
                                break;
                            }
                        }
                        if (!used)
                        {
                            if (fullLease.Epoch > maxEpoch) maxEpoch = fullLease.Epoch;
                            firstKeys.Add(fullLease.Keys[0]);
                        }
                    }
                    used = false;
                }
                if (firstKeys.Count == 0) return;
                Console.WriteLine("APAGAR LEASES RESIDUAIS: ");
                foreach (string key in firstKeys) Console.WriteLine(key + " ");
                Console.WriteLine();
                //Console.ReadKey();
                if (_tmContact.DeleteResidualKeys(firstKeys, _name, maxEpoch, this))
                {
                    foreach(string key in firstKeys)
                    {
                        LeaseRemove(key, _name);
                    }
                }
            }
            possibleResiduals = false;
            Console.WriteLine("RemoveResidual END");
        }
    }
}