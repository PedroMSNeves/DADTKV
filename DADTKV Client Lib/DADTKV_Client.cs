using Grpc.Core;
using Grpc.Net.Client;

namespace DADTKV_Client_Lib
{
    public class DADTKV_Client
    {
        private List<GrpcChannel> channels;
        private TmService.TmServiceClient tm;
        private int tm_cursor;

        public DADTKV_Client()
        {
            channels = new List<GrpcChannel>();
            tm = null; //starts null, for us to define in the first time it's used
            tm_cursor = 0;
        }
        /// <summary>
        /// Adds Tm server url and detects invalid url's
        /// </summary>
        /// <param name="url"></param>
        public void AddServer(string url)
        {
            try
            {
                channels.Add(GrpcChannel.ForAddress(url));
            }
            catch (System.UriFormatException ex)
            {
                Console.WriteLine("ERROR: Invalid url");
            }

        }
        /// <summary>
        /// Receives the client name, read list and write list for a transaction
        /// Prepares the transaction and then requests it
        /// </summary>
        /// <param name="cname"></param>
        /// <param name="read"></param>
        /// <param name="write"></param>
        /// <returns></returns>
        public List<DadInt> TxSubmit(string cname, List<string> read, List<DadInt> write)
        {
            TxReply reply;
            List<DadInt> result = new List<DadInt>();
            TxRequest request = new TxRequest { Id = cname };
            request.Reads.AddRange(read);
            foreach (DadInt d in write) { request.Writes.Add(new DadIntProto { Key = d.Key, Value = d.Val }); }

            if (tm == null)
            {
                // We calculate the server, to use, this way, to have a "good" distribuition of the clients to the servers
                foreach (char c in cname) tm_cursor += (int)c; // calculate value of name
                tm_cursor = tm_cursor % channels.Count(); // chooses one of the servers
                tm = new TmService.TmServiceClient(channels[tm_cursor]);
            }
            try
            {
                reply = tm.TxSubmitAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(15))).GetAwaiter().GetResult();
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded || ex.StatusCode == StatusCode.Unavailable)
            {
                Console.WriteLine("ERROR: Couldn't contact Server, please try again");
                tm_cursor = ++tm_cursor % channels.Count();
                tm = new TmService.TmServiceClient(channels[tm_cursor]); // Changes the server to contact in the next request
                return new List<DadInt>();
            }
            foreach (DadIntProto dad in reply.Reads) { result.Add(new DadInt(dad.Key, dad.Value)); }
            Console.WriteLine("Transaction succeded!");
            return result;
        }
        public bool Status() // por enquanto nao faz nada, visto que os servidores ainda nao dao crash
        {
            //TODO
            return true;
        }

    }
    /// <summary>
    /// Struct of DadInt, a pair of key (string), value (int)
    /// </summary>
    public struct DadInt
    {
        public DadInt(string key, int val)
        {
            Key = key;
            Val = val;
        }
        public string Key { get; }
        public int Val { get; }

        public override string ToString() => $"({Key}, {Val})";
    }
}