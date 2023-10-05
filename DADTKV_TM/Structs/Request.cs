namespace DADTKV_TM.Structs
{
    /// <summary>
    /// Enum to retain the information about the lease request
    /// </summary>
    public enum leaseRequested
    {
        No,
        Maybe,
        Yes
    }
    /// <summary>
    /// Class for storing the information of a transaction request
    /// </summary>
    public class Request
    {
        public Request(List<string> reads, List<DadIntProto> writes, int transaction_number)
        {
            Reads = reads;
            Writes = writes;
            Transaction_number = transaction_number;
            Situation = leaseRequested.No;
            Keys = GetKeys();
        }
        public leaseRequested Situation { set; get; }
        public List<string> Reads { get; }
        public List<DadIntProto> Writes { get; }
        public int Transaction_number { set; get; }

        public List<string> Keys { get; }

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
    }
}
