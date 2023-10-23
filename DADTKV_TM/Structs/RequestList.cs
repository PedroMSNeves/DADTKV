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
        public RequestList(int size, string name, List<string> lm_urls)
        {
            result = new Dictionary<int, ResultOfTransaction>();
            buffer = new List<Request>() ;
            MAX = size;
            _lmContact = new LmContact(name, lm_urls);
        }
        public List<Request> GetRequests(Store st) 
        {
            List<Request> buff;

            while (buzy == 0) Monitor.Wait(st);
            buff = buffer;

            return buff; 
        }
        public List<Request> GetRequestsNow()
        {
            List<Request> buff;
            buff = buffer;

            return buff;
        }
        public int insert(Request req, bool lease, Store st)
        {
            int tnumber;
            while (buzy == MAX) Monitor.Wait(st);
            tnumber = transaction_number++;
            req.initialize(tnumber);
            if (!lease)
            {
                // Use of distinct because we only need one copy of each key
                if (!_lmContact.RequestLease(req.Keys.Distinct().ToList(), tnumber)) return -1;
                req.Lease_number = tnumber;
            }
            buffer.Add(req);
            buzy++;
            Monitor.PulseAll(st);
            return tnumber;
        }
        public void remove(int i, Store st)
        {

                buffer.RemoveAt(i);
                buzy--;

                Monitor.PulseAll(st);
            
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
    }
}
