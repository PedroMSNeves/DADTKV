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
        private int tries;
        private int _triesBeforeDropingRequest;
        private bool _KillMe = false;
        private List<string> _killSuspected;
        public Store(string name, int timeSlotDuration, List<string> tm_urls, List<string> lm_urls, List<string> tm_names, List<string> lm_names)
        {
            tries = timeSlotDuration / 25;
            ResetTimes();
            _reqList = new RequestList(10, name, lm_urls, lm_names); // Maximum of requests waiting allowed and information necessary to ask for new leases
            _name = name;
            _store = new Dictionary<string, int>();
            _tmContact = new TmContact(tm_urls, tm_names);
            _fullLeases = new List<FullLease>();
            _waitList = new Dictionary<int, List<FullLease>>();
            _killSuspected = new List<string>();
        }
        /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        public int GetEpoch() { return _epoch; }
        /// <summary>
        /// Raises epoch by one
        /// </summary>
        public void IncrementEpoch() { _epoch++; }

        public bool GetKillMe() { return _KillMe; }
        /// <summary>
        /// Find any lease that intersects with a particular lease (before the particular lease)
        /// </summary>
        /// <param name="fl"></param>
        /// <returns></returns>
        public bool FullLeaseIntersection(FullLease fl)
        {
            foreach (FullLease req in _fullLeases)
            {
                if (fl.Lease_number == req.Lease_number && fl.Tm_name == req.Tm_name) return true;
                else if (fl.Intersection(req)) return false;
            }
            return true;
        }
        /// <summary>
        /// Gets the lease needed if it exists
        /// </summary>
        /// <param name="leaseId"></param>
        /// <param name="tmName"></param>
        /// <returns></returns>
        public FullLease GetFullLease(int leaseId, string tmName)
        {
            FullLease fl = null;
            foreach (FullLease fulllease in _fullLeases)
            {
                if (fulllease.Lease_number == leaseId && fulllease.Tm_name == tmName)
                {
                    fl = fulllease;
                }
            }
            return fl;
        }
        /// <summary>
        /// Gets the first lease with a specific key if possible
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        private FullLease GetFirst(string key)
        {
            foreach (FullLease fl in _fullLeases)
            {
                if (fl.Contains(key)) return fl;
            }
            return null;
        }
        public void DecreaseRelease()
        {
            _triesBeforeDropingRequest--;
        }
        public void ResetTimes()
        {
            _triesBeforeDropingRequest = tries;
        }
        public void CrashedServer(string name)
        {
            lock (this)
            {
                _tmContact.CrashedServer(name);
                _reqList.CrashedServer(name);
            }
        }
        public void AddKillSuspected(string name)
        {
            lock (this)
            {
                _killSuspected.Add(name);
            }
        }
        public List<string> GetKillSuspected()
        {
            return _killSuspected;
        }
        public void ClearKillSuspected()
        {
            _killSuspected.Clear();
        }
        public void Suspected(string name)
        {
            Console.WriteLine("ProcessOF");
            if (!(_tmContact.ContactSuspect(name, this)  || _reqList.ContactSuspect(name, this)))
            {
                CrashedServer(name);
                Console.WriteLine("SUSPECTED");
                _tmContact.KillSuspect(name, this);
                _reqList.KillSuspect(name, this);
            }
            Console.WriteLine("NOTSUSPECTED");
        }
        //////////////////////////////////////////////USED BY SERVERSERVICE////////////////////////////////////////////////////////////
        /// <summary>
        /// Used by the ServerService class (add new client transactions)
        /// </summary>
        /// <param name="reads"></param>
        /// <param name="writes"></param>
        /// <returns></returns>
        public int VerifyAndInsertRequest(List<string> reads, List<DadIntProto> writes)
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
                bool kill = false;
                tnum = _reqList.Insert(req, lease, ref kill, this);
                if (kill)
                {
                    _KillMe = true;
                    return -1;
                }
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
        public ResultOfTransaction GetResult(int tnum)
        {
            return _reqList.GetResult(tnum);
        }
        /// <summary>
        /// Used to see if a existing lease is compatible with us and can be used by us
        /// </summary>
        /// <param name="rq"></param>
        /// <param name="reqs"></param>
        /// <returns></returns>
        public int CompatibleLease(Request rq, List<Request> reqs)
        {
            // If no lease on the queue of one of the keys from the request, then no lease will work
            bool found = false;
            Request lastrq = null;
            FullLease fulllease = null;
            foreach (FullLease fl in _fullLeases)
            {
                // If marked to end, cant add more to it
                if (fl.End) continue;
                // We can only use leases that are for our TM
                if (fl.Tm_name != _name) continue;
                // Sees if we can use the lease
                if (!rq.SubGroup(fl)) continue;
                fulllease = fl; // It can only exist one (others are marked to end)
                break;
            }
            if (fulllease == null) return -1;

            // Look for the last person using this lease
            foreach (Request req in reqs)
            {
                if (req.Lease_number == fulllease.Lease_number) lastrq = req;
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
                    if (fulllease.Intersection(req)) return -1;
                }
            }
            if (fulllease.Lease_number == rq.Lease_number) return -1;
            return fulllease.Lease_number;
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
                    if (leaseNr != req.Lease_number)
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
            if (leaseNr == rq.Lease_number) return -1;
            return leaseNr;
        }
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
        //////////////////////////////////////////////USED BY MAINTHREAD (EXECUTE)////////////////////////////////////////////////////////////
        /// <summary>
        /// Used to see if our lease is at the top of the queue at all the lease's keys
        /// </summary>
        /// <param name="req"></param>
        /// <param name="fl"></param>
        /// <returns></returns>
        public bool CompleteLease(Request req, out FullLease fl)
        {
            // Finds the Lease needed
            fl = GetFullLease(req.Lease_number, _name);
            if (fl == null) return false;
            // Tries to find intersections, if any is found, then is not our time to execute yet
            return FullLeaseIntersection(fl);
        }
        public void Rerequest(Request req)
        {
            // If Tm is stuck in a loop the server will drop the lease and possibly request a new one
            ResetTimes();
            List<Request> rqs = new List<Request>();
            List<DadIntProto> reply = new List<DadIntProto>();
            Console.WriteLine("REREQUEST LEASE " + req.Lease_number);
            //Console.ReadKey();
            // It will add the request in the 0 position and all the others that use that lease
            foreach (Request rq in _reqList.GetRequestsNow())
            {
                if (rq.Lease_number == req.Lease_number)
                {
                    rqs.Add(rq);
                    Console.WriteLine(rq.Transaction_number + " " + rq.Lease_number);
                }
            }
            // Drops all the requests
            _reqList.Remove(rqs, this);
            bool lease;
            int err;
            // In the best case cenario will request 0 new leases (if they can use another existing lease)
            // In the worst cenario (with no crashes) will request 1 lease for the first one, and the rest use that lease
            foreach (Request rq in rqs)
            {
                // Tries to use other existing leases
                lease = verifyLease(req);
                // If couldnt use an existing lease, requests a new one (and inserts the requests again)
                bool killMe = false;
                // To further impede Dead Locks
                if (!lease) Thread.Sleep(new Random().Next(100, 500));
                err = _reqList.Insert(req, lease, req.Transaction_number, ref killMe, this);
                if (killMe)
                {
                    _KillMe = true;
                    return;
                }
                if (err == -1)
                {
                    // If couldnt request the lease from the Lm's marks the request has done with error
                    _reqList.Move(req.Transaction_number, reply, err);
                }
            }

            Console.WriteLine("REREQUEST LEASE END " + req.Lease_number);
        }
        /// <summary>
        /// Tries to execute the requests (sequentially)
        /// </summary>
        public void Execute()
        {
            lock (this)
            {
                Request req;
                while (true)
                {
                    // Gets first request
                    req = _reqList.GetRequestNow();
                    // Aborts if no requests exist
                    if (req == null) return;

                    FullLease fl;
                    List<DadIntProto> reply = new List<DadIntProto>();
                    // Sees if it has the needed complete lease
                    if (!CompleteLease(req, out fl))
                    {
                        Console.WriteLine("TRIES BEFORE DROPING" + _triesBeforeDropingRequest);
                        DecreaseRelease();
                        if (_triesBeforeDropingRequest == 0)
                        {
                            Rerequest(req);
                            return;
                        }
                        Console.WriteLine("NO LEASE: WE need " + _name + " " + req.Lease_number);
                        foreach (FullLease ff in _fullLeases.ToList())
                        {
                            Console.Write("FL: " + ff.Tm_name + " " + ff.Lease_number + " " + ff.End + " KEYS: ");
                            foreach (string s in ff.Keys) Console.Write(s + " ");
                            Console.WriteLine();
                        }
                        // Sequencial requests
                        return;
                    }
                    ResetTimes();
                    // Tries to propagate
                    int err = Request(req.Reads, req.Writes, fl, ref reply);
                    // Moves it to the pickup waiting place
                    _reqList.Move(req.Transaction_number, reply, err);
                    // Remove from the request list
                    _reqList.Remove(0, this);
                }
            }
        }
        /// <summary>
        /// Used to propagate writes to other TM's and prepare reply to the client
        /// </summary>
        /// <param name="reads"></param>
        /// <param name="writes"></param>
        /// <param name="fl"></param>
        /// <param name="reply"></param>
        /// <returns></returns>
        public int Request(List<string> reads, List<DadIntProto> writes, FullLease fl, ref List<DadIntProto> reply)
        {
            // If we have writes we try to propagate them
            if (writes.Count != 0)
            {
                bool killMe = false;
                bool ack = _tmContact.BroadCastChanges(_name, fl.Lease_number, fl.Epoch, ref killMe, this);
                if (killMe)
                {
                    _KillMe = true;
                    return -2;
                }
                _tmContact.ConfirmBroadChanges(writes, ack);
                if (!ack) return -2;
            }
            // Prepare the reply to the client (and execute writes)
            foreach (string key in reads)
            {
                if (!_store.ContainsKey(key)) reply.Add(new DadIntProto { Key = key, Value = 0, InvalidRead = true }); // Does the reads
                else reply.Add(new DadIntProto { Key = key, Value = _store[key], InvalidRead = false }); // Does the reads

            }
            foreach (DadIntProto write in writes) _store[write.Key] = write.Value; // Does the writes
            return 0;
        }
        //////////////////////////////////////////////USED BY BROADSERVICE////////////////////////////////////////////////////////////
        /// <summary>
        /// Tests if we have the needed lease at the top of every key queue
        /// </summary>
        /// <param name="name"></param>
        /// <param name="leaseId"></param>
        /// <param name="epoch"></param>
        /// <returns></returns>
        public bool TestWrite(string name, int leaseId, int epoch)
        {
            lock (this)
            {
                ResetTimes();
                // Waits to have the epoch needed
                while (_epoch < epoch) Monitor.Wait(this);
                // Tries to find the needed lease
                FullLease fl = GetFullLease(leaseId, name);
                if (fl == null) return false;

                foreach (string key in fl.Keys)
                {
                    FullLease ful = GetFirst(key);
                    if (ful == null) return false;
                    if (leaseId != ful.Lease_number || ful.Tm_name != name) return false;
                }
                return true;
            }
        }
        /// <summary>
        /// Writes the incoming changes
        /// </summary>
        /// <param name="writes"></param>
        /// <returns></returns>
        public bool Write(List<DadIntTmProto> writes)
        {
            lock (this)
            {
                ResetTimes();
                foreach (DadIntTmProto write in writes) { _store[write.Key] = write.Value; }
            }
            return true;
        }
        /// <summary>
        /// Sees if can delete all received residual leases
        /// </summary>
        /// <param name="residualLeases"></param>
        /// <param name="epoch"></param>
        /// <returns></returns>
        public bool[] DeleteResidual(List<LeaseProtoTm> residualLeases, int epoch)
        {
            bool[] residual = new bool[residualLeases.Count];
            lock (this)
            {
                ResetTimes();
                while (_epoch < epoch) Monitor.Wait(this);
                for (int i = 0; i < residualLeases.Count; i++)
                {
                    if (LeaseTest(residualLeases[i])) residual[i] = true;
                    else residual[i] = false;
                }
            }
            return residual;
        }
        /// <summary>
        /// Tests if it can remove a specific lease
        /// </summary>
        /// <param name="lease"></param>
        /// <returns></returns>
        public bool LeaseTest(LeaseProtoTm lease)
        {
            // Verify all entries
            foreach (string key in lease.Keys)
            {
                ResetTimes();
                FullLease fl = GetFirst(key);
                // If the lease on top of the queue is not ours or the queue is empty returns false
                if (fl == null) return false;
                if (fl.Tm_name != lease.Tm || fl.Lease_number != lease.LeaseId) return false;
            }
            return true;
        }
        /// <summary>
        /// Remove the received Lease (waits if this tm did not receive this epoch's leases)
        /// </summary>
        /// <param name="lease"></param>
        /// <param name="epoch"></param>
        public void LeaseRemove(LeaseProtoTm lease, int epoch)
        {
            FullLease fl = null;
            lock (this)
            {
                ResetTimes();
                while (_epoch < epoch) Monitor.Wait(this);
                foreach (FullLease fullLease in _fullLeases)
                {
                    if (fullLease.Lease_number == lease.LeaseId && fullLease.Tm_name == lease.Tm)
                    {
                        fl = fullLease;
                        break;
                    }
                }
                if (fl == null) return;
                // Removes lease list entry
                _fullLeases.Remove(fl);
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
                ResetTimes();
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
        /// Receives new leases for the epoch
        /// </summary>
        /// <param name="leases"></param>
        /// <param name="epoch"></param>
        public void NewLeases(List<FullLease> leases, int epoch)
        {
            Console.WriteLine("NewLeases");
            foreach (FullLease fl in leases)
            {
                foreach (string key in fl.Keys)
                {
                    foreach (FullLease fulllease in _fullLeases)
                    {
                        if (fulllease.Keys.Contains(key)) fulllease.End = true;
                    }
                }
                _fullLeases.Add(fl);
            }
            IncrementEpoch();
            // Awakes possible waiting propagation thread
            Monitor.PulseAll(this);
            foreach (FullLease lease in _fullLeases) foreach (string key in lease.Keys) Console.WriteLine(key + ":" + lease.End);
            Console.WriteLine("NewLeases END");
        }
        //////////////////////////////////////////////USED BY MAINTHREAD (REMOVERESIDUAL)////////////////////////////////////////////////////////////
        /// <summary>
        /// Sees and tries to remove any residual leases
        /// </summary>
        public void RemoveResidual()
        {
            lock (this)
            {
                Console.WriteLine("REMOVE RESIDUAL");
                bool used = false;
                int maxEpoch = 0;
                Console.WriteLine("Requests");
                foreach (Request rq in _reqList.GetRequestsNow())
                {
                    Console.WriteLine(rq.Transaction_number + " " + rq.Lease_number);
                }
                // Dead tms, remove leases
                List<string> dead = GetDeadNamesTm();
                List<FullLease> remove = new List<FullLease>();
                foreach (FullLease fulllease in _fullLeases)
                {
                    if (dead.Contains(fulllease.Tm_name))
                    {
                        remove.Add(fulllease);
                    }
                }
                foreach (FullLease fulllease in remove)
                {
                    _fullLeases.Remove(fulllease);
                }


                List<FullLease> leases = new List<FullLease>();
                foreach (FullLease fullLease in _fullLeases.ToList())
                {
                    if (fullLease.Tm_name == _name && fullLease.End)
                    {
                        foreach (Request rq in _reqList.GetRequestsNow())
                        {
                            if (fullLease.Lease_number == rq.Lease_number)
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
                Console.WriteLine("REMOVE RESIDUAL1");
                Console.WriteLine("epoch:" + _epoch);
                if (leases.Count == 0) return;
                Console.WriteLine("APAGAR LEASES RESIDUAIS: ");
                //foreach (string key in leases) Console.WriteLine(key + " ");
                foreach (FullLease lease in leases)
                {
                    Console.WriteLine("FL: " + lease.Tm_name + " " + lease.End + " " + lease.Lease_number);
                }
                Console.WriteLine();
                //Console.ReadKey();
                bool killMe = false;
                bool[] bools = _tmContact.DeleteResidualKeys(leases, maxEpoch, ref killMe, this); // provavelmente ter uma array out para saber que leases podemos apagar
                if (killMe) _KillMe = true;
                if (bools == null) return;

                if (_tmContact.ConfirmResidualDeletion(_name, bools, maxEpoch))
                {
                    ResetTimes();
                    for (int i = 0; i < bools.Length; i++)
                    {
                        if (bools[i]) LeaseRemove(leases[i]);
                    }
                }
            }
            Console.WriteLine("RemoveResidual END");
        }
        /// <summary>
        /// Removes the lease from the arguments
        /// </summary>
        /// <param name="lease"></param>
        /// <returns></returns>
        public bool LeaseRemove(FullLease lease)
        {
            // Removes lease list entry
            _fullLeases.Remove(lease);
            return true;
        }
        public List<string> GetDeadNamesTm()
        {
            return _tmContact.GetDeadNames();
        }
        public List<string> GetDeadNamesLm()
        {
            return _reqList.GetDeadNames();
        }
    }
}