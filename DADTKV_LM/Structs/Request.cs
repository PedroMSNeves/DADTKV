
namespace DADTKV_LM.Structs
{
    public class Request
    {
        public Request(string tm_name, List<string> keys)
        {
            Tm_name = tm_name;
            Keys = keys;

        }
        public string Tm_name { get; }
        public List<string> Keys { get; }
    }
}
