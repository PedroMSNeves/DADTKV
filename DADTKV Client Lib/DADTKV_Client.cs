using Grpc.Core;
using Grpc.Net.Client;
using System.Threading.Channels;

namespace DADTKV_Client_Lib
{
    public class DADTKV_Client
    {
        private List<GrpcChannel> channels;
        private TmService.TmServiceClient tm;
        private int tm_cursor; // futuramente ira ser usado para saber o "nosso" tm, updated quando esse tm nao responder

        public DADTKV_Client()
        {
            channels = new List<GrpcChannel>();
            tm = null;
            tm_cursor = 0;
        }
        public void AddServer(string url)
        {
            channels.Add(GrpcChannel.ForAddress(url));
        }
        public List<DadInt> TxSubmit(string cname, List<string> read,List<DadInt> write)
        {
            TxReply reply;
            List<DadInt> result = new List<DadInt>();
            TxRequest request = new TxRequest { Id = cname };
            request.Reads.AddRange(read);
            foreach (DadInt d in write) { request.Writes.Add(new DadIntProto { Key = d.Key , Value = d.Val }); }

            if(tm == null)
            {
                foreach (char c in cname) tm_cursor += (int) c; //calcular o valor do nome
                tm_cursor = tm_cursor % channels.Count(); // saber que server vai escolher
                tm = new TmService.TmServiceClient(channels[tm_cursor]);
            }
            try
            {
                reply = tm.TxSubmitAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult();
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded || ex.StatusCode == StatusCode.Unavailable)
            {
                Console.WriteLine("Couldn't contact Server");
                tm_cursor= ++tm_cursor % channels.Count();
                tm = new TmService.TmServiceClient(channels[tm_cursor]); // muda o servidor que estava a contactar
                return new List<DadInt>();
            }
            foreach ( DadIntProto dad in reply.Reads) { result.Add(new DadInt(dad.Key, dad.Value)); }
            return result;
        }
        public bool Status()
        {
            //TODO
            return true;
        }

    }
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