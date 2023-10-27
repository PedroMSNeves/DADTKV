namespace DADTKV_TM.Structs
{
    /// <summary>
    /// Class lease for storing all information about a certain lease
    /// </summary>
    public class FullLease
    {
        public FullLease(string tm_name, int epoch, List<string> keys, int lease_number)
        {
            Keys = keys;
            Tm_name = tm_name;
            Epoch = epoch;
            End = false;
            Lease_number = lease_number;
        }
        public string Tm_name { get; }
        public int Epoch { get; }
        public bool End { set; get; }
        public int Lease_number { get; }
        public List<string> Keys { get; }

        public bool Intersection(FullLease other)
        {
            foreach (string key in Keys)
            {
                if (other.Contains(key)) return true;
            }
            return false;
        }
        public bool Intersection(Request other)
        {
            foreach (string key in Keys)
            {
                if (other.Keys.Contains(key)) return true;
            }
            return false;
        }
        public bool Intersection(List<FullLease> others)
        {
            foreach (FullLease other in others)
            {
                if (Intersection(other)) return true;
            }
            return false;
        }
        public bool Contains(string key)
        { return Keys.Contains(key); }

        public bool Equal(FullLease other)
        {
            if (Tm_name != other.Tm_name) return false;
            if (Epoch != other.Epoch) return false;
            if (Lease_number != other.Lease_number) return false;
            if (Keys.Count != other.Keys.Count) return false;
            for (int i = 0; i < Keys.Count; i++)
            {
                if (!Keys[i].Equals(other.Keys[i])) return false;
            }

            return true;
        }
        public override string ToString() => $"({Tm_name}, {Epoch}, {End})";
    }
}