namespace DADTKV_Client_Lib
{
    public class DADTKV_Client
    {
        private Dictionary<string, int> store_replica;
        //provavelmente guarda coneccao para todos os TM
        public DADTKV_Client()
        {
            store_replica = new Dictionary<string, int>();
        }
        public List<DadInt> TxSubmit(List<string> read,List<DadInt> write)
        {
            //TODO
            return new List<DadInt>();
        }
        public bool Status()
        {
            //TODO
            return true;
        }

    }
    public struct DadInt
    {
        public DadInt(string obj, int val)
        {
            Obj = obj;
            Val = val;
        }

        public string Obj { get; }
        public int Val { get; }

        public override string ToString() => $"({Obj}, {Val})";
    }
}