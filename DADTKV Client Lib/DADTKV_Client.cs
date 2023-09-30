using Grpc.Core;
using Grpc.Net.Client;

namespace DADTKV_Client_Lib
{
    public class DADTKV_Client
    {
        private readonly TmService.TmServiceClient tm;

        public DADTKV_Client()
        {
            GrpcChannel channel = GrpcChannel.ForAddress("http://localhost:" + "1000");
            tm = new TmService.TmServiceClient(channel);
        }
        public List<DadInt> TxSubmit(string cname, List<string> read,List<DadInt> write)
        {
            TxReply reply;
            List<DadInt> result = new List<DadInt>();
            TxRequest request = new TxRequest { Id = cname };
            request.Reads.AddRange(read);
            foreach (DadInt d in write) { request.Writes.Add(new DadIntProto { Key = d.Key , Value = d.Val }); }

            try
            {
                reply = tm.TxSubmitAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult();
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded || ex.StatusCode == StatusCode.Unavailable)
            {
                Console.WriteLine("Couldn't contact Server");
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