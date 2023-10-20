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
        private int _epoch = 0; // Last epoch received
        private LmContact _lmContact;
        public RequestList(int size, string name, List<string> lm_urls)
        {
            result = new Dictionary<int, ResultOfTransaction>();
            buffer = new List<Request>() ;
            MAX = size;
            _lmContact = new LmContact(name, lm_urls);
        }
        public int get_epoch() { return _epoch; }
        public List<Request> GetRequests() 
        {
            List<Request> buff;
            lock (this)
            {
                while (buzy == 0) Monitor.Wait(this);
                buff = buffer;
            }
            return buff; 
        }
        public List<Request> GetRequestsNow()
        {
            List<Request> buff;
            lock (this)
            {
                buff = buffer;
            }
            return buff;
        }
        public int insert(Request req, bool lease)
        {
            int tnumber;
            lock (this)
            {
                while (buzy == MAX) Monitor.Wait(this);
                tnumber = transaction_number++;
                req.initialize(tnumber, _epoch);
                if (!lease)
                {
                    _lmContact.RequestLease(req.Keys, tnumber);
                    req.Lease_number = tnumber;
                }
                buffer.Add(req);
                buzy++;
                Monitor.PulseAll(this);
            }
            return tnumber;
        }
        public void remove(int i)
        {
            lock (this)
            {
                buffer.RemoveAt(i);
                buzy--;

                Monitor.PulseAll(this);
            }
        }
        public void move(int transaction_number, List<DadIntProto> resultT, int err)
        {
            lock (this)
            {
                result.Add(transaction_number, new ResultOfTransaction(resultT, err));
                Monitor.PulseAll(this);
            }
        }
        public ResultOfTransaction getResult(int t_number)
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
        public void incrementEpoch() { _epoch++; }
    }
}
