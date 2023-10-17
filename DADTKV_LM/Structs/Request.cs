
namespace DADTKV_LM.Structs
{
    public class Request
    {
        public Request(string tm_name, List<string> keys, int lease_id)
        {
            Tm_name = tm_name;
            Keys = keys;
            Lease_ID = lease_id;
        }
        public string Tm_name { get; }
        public List<string> Keys { get; }
        public int Lease_ID { get; }
    }
}
