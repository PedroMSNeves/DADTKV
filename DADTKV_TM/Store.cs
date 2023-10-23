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
        private List<FullLease> _fullLeases; // Leases store
        private RequestList _reqList; // Where Requests are stored
        private TmContact _tmContact; // Used to propagate things to other TM
        private Dictionary<int, List<FullLease>> _waitList;
        private int _epoch = 0; // Last epoch received

        public Store(string name, List<string> tm_urls, List<string> lm_urls)
        {
            _reqList = new RequestList(10,name, lm_urls); // Maximum of requests waiting allowed and information necessary to ask for new leases
            _name = name;
            _store = new Dictionary<string, int>();
            _tmContact = new TmContact(tm_urls);
            _fullLeases = new List<FullLease>();
            _waitList = new Dictionary<int, List<FullLease>>();
        }
        public void incrementEpoch() { _epoch++; }

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
            // If no lease on the queue of one of the keys from the request, then no lease will work

            foreach (FullLease fl  in _fullLeases)
            {
                // If marked to end, cant add more to it
                if (fl.End) continue;
                // We can only use leases that are for our TM
                if(fl.Tm_name == _name)
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

        public bool fullLeaseIntersection(FullLease fl)
        {
            foreach (FullLease req in _fullLeases)
            {
                if (fl.Lease_number == req.Lease_number && fl.Tm_name == req.Tm_name) return true;
                else if (fl.Intersection(req)) return false;
            }
            return true;
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
            foreach (FullLease fulllease in _fullLeases)
            {
                if (fulllease.Lease_number == req.Lease_number && fulllease.Tm_name == _name)
                {
                    fl = fulllease;
                }
            }
            if (fl == null) return false;
                        
             
            return fullLeaseIntersection(fl);
        }
        /// <summary>
        /// Tries to execute the requests
        /// </summary>
        public void Execute()
        {
            //Console.WriteLine("Execute");
            //Console.WriteLine("GetRequests");
            

            lock (this)
            {
                // Gets requests (waits until there is at least one)
                List<Request> reqs; 
                while (true)
                {
                    reqs = _reqList.GetRequestsNow();
                    if (reqs.Count == 0) return;

                    FullLease fl;
                    List<DadIntProto> reply = new List<DadIntProto>();
                    // Sees if it has the needed complete lease
                    if (!completeLease(reqs[0], out fl))
                    {
                        // Sequencial requests
                        Console.WriteLine("NO LEASE: WE need " + _name + " "+reqs[0].Lease_number);
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

                    int err = Request(reqs[0].Reads, reqs[0].Writes, fl, ref reply);
                    Console.WriteLine("Request END");

                    // Moves it to the pickup waiting place
                    Console.WriteLine("Move");

                    _reqList.move(reqs[0].Transaction_number, reply, err);
                    Console.WriteLine("Move END");

                    // Remove from the request list
                    Console.WriteLine("Remove");

                    _reqList.remove(0,this);
                    Console.WriteLine("Remove END");
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
        public int Request(List<string> reads, List<DadIntProto> writes, FullLease fl, ref List<DadIntProto> reply)
        {
            // If we have writes we use them to identify the lease to close (if needed)
            if(writes.Count != 0)
            {
                // If lease is to close, we send the name, if not we send "_"
                Console.WriteLine("BroadCastChanges");
                bool ack = _tmContact.BroadCastChanges(_name, fl.Lease_number, fl.Epoch, this);
                _tmContact.ConfirmBroadChanges(writes, ack);
                if(!ack) return -2;

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

            return 0;
        }
        /// <summary>
        /// Remove the Lease that is on top of the queue for the first key
        /// </summary>
        /// <param name="firstkey"></param>
        /// <param name="tm_name"></param>
        /// <returns></returns>
        public bool LeaseRemove(FullLease lease)
        {
            //if (_leases[firstkey].TryPeek(out var lease)) fl = (FullLease)lease; // Gets FullLease
            //else return false; // Theoretically never possible
            // Verify all entries
            /*foreach (string key in lease.Keys)
            {
                if (_leases[key].TryPeek(out var result))
                {
                    if (result.Tm_name != _name && result.Lease_number != lease.Lease_number) return false; 
                }
                else return false;
            }*/

            // Remove all entries
            _fullLeases.Remove(lease); // Removes lease list entry
            return true;
        }
        //////////////////////////////////////////////USED BY BROADSERVICE////////////////////////////////////////////////////////////
        public bool TestWrite (string name, int leaseId, int epoch)
        {
            lock (this)
            {
                while (_epoch < epoch) Monitor.Wait(this);
                foreach(FullLease fullLease in _fullLeases)
                {
                    if(fullLease.Lease_number == leaseId && fullLease.Tm_name == name)
                    {
                        foreach (string key in fullLease.Keys)
                        {
                            FullLease fl = getFirst(key);
                            if (fl == null) return false;
                            if (leaseId != fl.Lease_number || fl.Tm_name != name) return false;
                            // Verify if we have all parts of the lease at the top
                           
                        }
                        return true;
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// Used by BroadService
        /// Writes the incoming changes and removes a lease if needed        
        /// </summary>
        /// <param name="writes"></param>
        /// <param name="tm_name"></param>
        /// <param name="epoch"></param>
        /// <returns></returns>
        public bool Write(List<DadIntTmProto> writes)
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
        public bool[] DeleteResidual(List<LeaseProtoTm> residualLeases, int epoch)
        {
            bool[] residual = new bool[residualLeases.Count];
            lock (this)
            {
                while (_epoch < epoch) Monitor.Wait(this);
                for (int i = 0; i < residualLeases.Count; i++)
                {
                    Console.WriteLine("LeaseRemove");
                    if (LeaseTest(residualLeases[i], epoch)) residual[i] = true;
                    else residual[i] = false;
                    Console.WriteLine("LeaseRemove END");
                }
            }
            

            return residual;
        }
        private FullLease getFirst(string key)
        {
            foreach (FullLease fl in _fullLeases)
            {
                if(fl.Contains(key)) return fl;
            }
            return null;
        }
        /// <summary>
        /// Remove the Lease that is on top of the queue for the first key (waits if this tm did not receive this epoch's leases)
        /// </summary>
        /// <param name="firstkey"></param>
        /// <param name="tm_name"></param>
        /// <param name="epoch"></param>
        /// <returns></returns>
        public bool LeaseTest(LeaseProtoTm lease, int epoch)
        {
            // Verify all entries
            foreach (string key in lease.Keys)
            {
                FullLease fl = getFirst(key);
                if (fl == null) return false;
                if (fl.Tm_name != lease.Tm || fl.Lease_number != lease.LeaseId) return false; // Theoretically never possible
            }
            // guardar e depois da confirmacao e que apagamos
            return true;
        }
        public void LeaseRemove(LeaseProtoTm lease, int epoch)
        {
            FullLease fl = null;
            lock (this)
            {
                foreach (FullLease fullLease in _fullLeases)
                {
                    if (fullLease.Lease_number == lease.LeaseId && fullLease.Tm_name == lease.Tm)
                    {
                        fl = fullLease;
                        break;
                    }
                }
                if (fl == null) return;
                // Remove all entries
                _fullLeases.Remove(fl); // Removes lease list entry
            }
        }

        //////////////////////////////////////////////USED BY LSERVICE////////////////////////////////////////////////////////////
        /// <summary>
        /// Used to store batches of leases that cannot yet be added (mensage delay or multipaxos)
        /// When able, adds all the batches possible
        /// </summary>
        /// <param name="leases"></param>
        /// <param name="epoch"></param>
        public void WaitLeases(List<FullLease> leases, int epoch)
        {
            lock (this)
            {
                //foreach (FullLease lease in _fullLeases) foreach (string key in lease.Keys) Console.WriteLine(key + ":" + lease.End);
                //foreach (FullLease lease in leases) foreach (string key in lease.Keys) Console.WriteLine(key + ":" + lease.End);

                // If it's not the epoch we want, we have to wait for it
                if (epoch != _epoch + 1)
                {
                    _waitList[epoch] = leases;
                    return;
                }
                // Adds this epoch
                Console.WriteLine("ADD NEW LEASES");
                NewLeases(leases, epoch);

                List<int> remove = new List<int>();
                // Adds next epochs (if possible)
                foreach (int epochW in _waitList.Keys.OrderBy(num => num))
                {
                    if (epochW == _epoch + 1)
                    {
                        NewLeases(leases, epoch);
                        remove.Add(epochW);
                    }
                    else break;
                }
                foreach (int i in remove) _waitList.Remove(i);
            }
        }
         

        /// <summary>
        /// Used By LService
        /// Receives new leases for the epoch
        /// </summary>
        /// <param name="leases"></param>
        /// <param name="epoch"></param>
        public void NewLeases(List<FullLease> leases, int epoch)
        {
            Console.WriteLine("NewLeases");

            //foreach (FullLease lease in _fullLeases) foreach (string key in lease.Keys) Console.WriteLine(key + ":" + lease.End);
            //foreach (FullLease lease in leases) foreach (string key in lease.Keys) Console.WriteLine(key + ":" + lease.End);
            foreach (FullLease fl in leases)
            {
                foreach(string  key in fl.Keys)
                {
                    foreach (FullLease fulllease in _fullLeases)
                    {
                        if (fulllease.Keys.Contains(key)) fulllease.End = true;
                    }
                }
                _fullLeases.Add(fl);
            }
            incrementEpoch();
            // Awakes possible waiting propagation thread
            Monitor.PulseAll(this);
            foreach (FullLease lease in _fullLeases) foreach (string key in lease.Keys) Console.WriteLine(key + ":" + lease.End);

            Console.WriteLine("NewLeases END");
            //Console.ReadKey();
        }
        public void removeResidual()
        {
            lock (this)
            {
                bool used = false;
                int maxEpoch = 0;
                List<FullLease> leases = new List<FullLease>();
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
                            leases.Add(fullLease);
                        }
                    }
                    used = false;
                }
                if (leases.Count == 0) return;
                Console.WriteLine("APAGAR LEASES RESIDUAIS: ");
                //foreach (string key in leases) Console.WriteLine(key + " ");
                foreach(FullLease lease in leases)
                {
                    Console.WriteLine("FL: " + lease.Tm_name + " " + lease.End + " " + lease.Lease_number);
                }
                Console.WriteLine();
                //Console.ReadKey();
                bool[] bools = _tmContact.DeleteResidualKeys(leases, maxEpoch, this); // provavelmente ter uma array out para saber que leases podemos apagar
                if (bools == null) return;

                if (_tmContact.ConfirmResidualDeletion(_name, bools, maxEpoch))
                {
                    for (int i = 0; i < bools.Length; i++)
                    {
                        if (bools[i]) LeaseRemove(leases[i]);
                    }
                }
            }
            Console.WriteLine("RemoveResidual END");
        }
    }
}