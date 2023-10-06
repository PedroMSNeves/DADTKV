namespace DADTKV_TM.Structs
{
    /// <summary>
    /// Class lease for storing all information about a certain lease
    /// </summary>
    public class FullLease : Lease
    {
        public FullLease(string tm_name, int epoch,List<string> keys) : base (tm_name, epoch)
        {
            Keys = keys;
        }

        public List<string> Keys { get; }

        public bool Intersection(FullLease other)
        {
            foreach (string key in Keys) 
            {
                if (other.Contains(key)) return true;
            }
            return false;
        }
        public bool Intersection(List<FullLease> others)
        {
            foreach(FullLease other in others)
            {
                if (Intersection(other)) return true;
            }
            return false;
        }
        public bool Contains(string key)
        { return Keys.Contains(key); }
        public override string ToString() => $"({Tm_name}, {Epoch}, {End})";
    }


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
