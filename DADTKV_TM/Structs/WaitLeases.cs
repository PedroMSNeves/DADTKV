using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DADTKV_TM.Structs
{
    public class WaitLeases
    {

        public WaitLeases(int epoch, List<FullLease> leases) 
        {
            Epoch = epoch;
            Acks = 0;
            Leases = leases;
        }
        public int Epoch { get; }
        public int Acks { set; get; }
        public List<FullLease> Leases { get; }

        public void increaseAcks()
        {
            Acks++;
        }

    }
}
