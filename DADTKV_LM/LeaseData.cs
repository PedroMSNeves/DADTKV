using DADTKV_LM.Structs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DADTKV_LM
{
    public class LeaseData
    {   /* se um lm morrer podemos ter bitmap dos lm vivos */
        List<Request> _requests = new List<Request>(); //requests received from TMs
        private Dictionary<int, int> read_ts;
        private Dictionary<int, int> write_ts;

        public LeaseData(bool leader)
        {
            Possible_Leader = 0;
            IsLeader = leader;
            read_ts = new Dictionary<int, int>();
            write_ts = new Dictionary<int, int>();
            RoundID = 0;
            Epoch = 0;
        }
        public int Epoch { get; set; }
        public int GetReadTS(int epoch) 
        {
            if (read_ts.ContainsKey(epoch)) return read_ts[epoch];
            else return 0;
        }
        public void SetReadTS(int epoch, int val) { read_ts[epoch] = val; }
        public int GetWriteTS(int epoch) 
        { 
            if (write_ts.ContainsKey(epoch)) return write_ts[epoch];
            else return 0; 
        }
        public void SetWriteTS(int epoch, int val) { write_ts[epoch] = val; }
        public int RoundID { get; set; }
        public int IncrementRoundID(int epoch)
        {
            return RoundID++;
        }
        public int Possible_Leader { get; set; }
        public bool IsLeader { get; set; }
        public void AddRequest(Request request) //adds in the end
        {
            lock (this)
            {
                _requests.Add(request);                
            }
        }
        public void AddRequestBeginning(Request request) //nao ness
        {
            lock (this)
            {
                _requests.Insert(0, request);
            }
        }
        public bool RemoveRequest() //removes from beginning //nao ness
        {
            lock (this)
            {
                if (_requests.Count == 0) return false;
                _requests.RemoveAt(0);
                return true;
            }
        }
        public bool Intersect(List<string> l1, List<string> l2)
        {
            return l1.Intersect(l2).Any();
        }
        public List<Request> GetMyValue()
        {
            lock (this)
            {
                List<string> tm_names = new List<string>();
                List<Request> myValue = new List<Request>();
                foreach (Request request in _requests)
                {
                    if (!tm_names.Contains(request.Tm_name))
                    {
                        tm_names.Add(request.Tm_name);
                    }
                }
                foreach (String tm in tm_names)
                {
                    foreach (Request request in _requests)
                    {
                        if (request.Tm_name == tm) myValue.Add(request);
                    }
                }
                _requests.Clear();
                return myValue;
            }
        }
    }
}
