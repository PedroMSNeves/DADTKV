using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        }
        public leaseRequested Situation { set; get; }
        public List<string> Reads { get; }
        public List<DadIntProto> Writes { get; }
        public int Transaction_number { set; get; }
    }
}
