using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DADTKV_TM.Structs
{
    /// <summary>
    /// Class lease for storing information about a certain lease
    /// </summary>
    public class Lease
    {
        public Lease(string tm_name, int epoch)
        {
            Tm_name = tm_name;
            Epoch = epoch;
            End = false;
        }
        public string Tm_name { get; }
        public int Epoch { get; }
        public bool End { set; get; }

        public override string ToString() => $"({Tm_name}, {Epoch}, {End})";
    }
}
