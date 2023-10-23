namespace DADTKV_TM.Structs
{
    /// <summary>
    /// Class for storing the information of a transaction request
    /// </summary>
    public class Request
    {
        public Request(List<string> reads, List<DadIntProto> writes)
        {
            Reads = reads;
            Writes = writes;
            Keys = GetKeys();
        }
        public List<string> Reads { get; }
        public List<DadIntProto> Writes { get; }
        public int Transaction_number { set; get; }
        public int Lease_number { set;  get; }
        public List<string> Keys { get; }

        public void Initialize (int transaction_number)
        {
            Transaction_number = transaction_number;
        }

        private List<string> GetKeys()
        {
            List<string> keys = new List<string>();
            foreach (string key in Reads)
            {
                keys.Add(key);
            }
            foreach (DadIntProto dad in Writes)
            {
                keys.Add(dad.Key);
            }
            return keys;
        }
        public bool SubGroup(Request other)
        {
            foreach (string key in Keys)
            {
                if (!other.Keys.Contains(key)) return false;
            }
            return true;
        }
        public bool SubGroup(FullLease other)
        {
            foreach (string key in Keys)
            {
                if (!other.Keys.Contains(key)) return false;
            }
            return true;
        }
        public bool Intersection(Request other)
        {
            foreach (string key in Keys)
            {
                if (other.Keys.Contains(key)) return true;
            }
            return false;
        }
    }
}
