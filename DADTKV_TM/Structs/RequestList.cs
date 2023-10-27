using System.Collections.Generic;
using DADTKV_TM.Contact;

namespace DADTKV_TM.Structs
{
    /// <summary>
    /// Class that stores the transaction requests and their results    
    /// </summary>
    public class RequestList
    {
        List<Request> buffer;
        Dictionary<int, ResultOfTransaction> result;
        private int MAX;
        private int buzy = 0;
        private int transaction_number = 1;
        private LmContact _lmContact;
        public RequestList(int size, string name, List<string> lm_urls, List<string> lm_names)
        {
            result = new Dictionary<int, ResultOfTransaction>();
            buffer = new List<Request>();
            MAX = size;
            _lmContact = new LmContact(name, lm_urls, lm_names);
        }
        public List<Request> GetRequestsNow()
        {
            return buffer;
        }
        public Request GetRequestNow()
        {
            if (buffer.Count == 0) return null;
            return buffer[0];
        }
        public int Insert(Request req, bool lease, int tNum, ref bool killMe, Store st)
        {
            int tnumber;
            while (buzy == MAX) Monitor.Wait(st);
            tnumber = transaction_number++;
            if (!lease)
            {
                // Use of distinct because we only need one copy of each key
                if (!_lmContact.RequestLease(req.Keys.Distinct().ToList(), tnumber, ref killMe)) return -1;
                req.Lease_number = tnumber;
            }
            buffer.Add(req);
            buzy++;
            Monitor.PulseAll(st);
            return tnumber;
        }
        public int Insert(Request req, bool lease, ref bool killMe, Store st)
        {
            int tnumber;
            while (buzy == MAX) Monitor.Wait(st);
            tnumber = transaction_number++;
            req.Initialize(tnumber);
            if (!lease)
            {
                // Use of distinct because we only need one copy of each key
                if (!_lmContact.RequestLease(req.Keys.Distinct().ToList(), tnumber, ref killMe)) return -1;
                req.Lease_number = tnumber;
            }
            buffer.Add(req);
            buzy++;
            Monitor.PulseAll(st);
            return tnumber;
        }
        public void Remove(int i, Store st)
        {
            buffer.RemoveAt(i);
            buzy--;
            Monitor.PulseAll(st);
        }
        public void Remove(List<Request> reqs, Store st)
        {
            foreach (Request request in reqs)
            {
                for (int i = 0; i < buffer.Count; i++)
                {
                    if (buffer[i].Transaction_number == request.Transaction_number)
                    {
                        Remove(i, st);
                        break;
                    }
                }
            }

        }
        public void Move(int transaction_number, List<DadIntProto> resultT, int err)
        {
            lock (this)
            {
                result.Add(transaction_number, new ResultOfTransaction(resultT, err));
                Monitor.PulseAll(this);
            }
        }
        public ResultOfTransaction GetResult(int t_number)
        {
            ResultOfTransaction resultT;
            lock (this)
            {
                while (!result.ContainsKey(t_number))
                {
                    Monitor.Wait(this);
                }
                resultT = result[t_number];
                result.Remove(t_number);
            }
            return resultT;
        }
        public void CrashedServer(string name)
        {
            _lmContact.CrashedServer(name);
        }
        public List<string> GetDeadNames()
        {
            return _lmContact.GetDeadNames();
        }
        public bool ContactSuspect(string name, Store st)
        {
            return _lmContact.ContactSuspect(name, st);
        }
        public bool KillSuspect(string name, Store st)
        {
            return _lmContact.KillSuspect(name, st);
        }
    }
}