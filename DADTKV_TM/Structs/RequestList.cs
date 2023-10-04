namespace DADTKV_TM.Structs
{
    /// <summary>
    /// Class that stores the transaction requests and their results    
    /// </summary>
    public class RequestList
    {
        Request[] buffer;
        Dictionary<int, ResultOfTransaction> result;
        private int MAX;
        private int buzy = 0;
        private int cursorIN = 0;
        private int cursorOUT = 0;
        private int transaction_number = 0;
        public RequestList(int size)
        {
            result = new Dictionary<int, ResultOfTransaction>();
            buffer = new Request[size];
            MAX = size;
        }
        public int insert(List<string> reads, List<DadIntProto> writes)
        {
            int tnumber;
            lock (this)
            {
                while (buzy == MAX) Monitor.Wait(this);
                tnumber = transaction_number++;
                buffer[cursorIN] = new Request(reads, writes, tnumber);
                this.cursorIN = ++this.cursorIN % this.MAX;
                buzy++;
                Monitor.PulseAll(this);
            }
            return tnumber;
        }
        public Request execute()
        {
            Request req;
            lock (this)
            {
                while (buzy == 0)
                {
                    Monitor.Wait(this);
                }
                req = buffer[cursorOUT];
                this.cursorOUT = ++this.cursorOUT % this.MAX;
                buzy--;
                Monitor.PulseAll(this);
            }
            return req;
        }
        public void move(int transaction_number, List<DadIntProto> resultT, int err)
        {
            lock (this)
            {
                result.Add(transaction_number, new ResultOfTransaction(resultT, err));
                Monitor.PulseAll(this);
            }
        }
        public ResultOfTransaction getResult(int t_number)
        {
            ResultOfTransaction resultT;
            lock (this)
            {
                while (!result.ContainsKey(t_number))
                {
                    Monitor.Wait(this);
                }
                resultT = result[t_number];
                result.Remove(t_number);
            }
            return resultT;
        }
    }
}
