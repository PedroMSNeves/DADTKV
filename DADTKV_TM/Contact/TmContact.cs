using Grpc.Core;
using Grpc.Net.Client;

namespace DADTKV_TM.Contact
{
    /// <summary>
    /// Contact with Transaction Manager, to propagate changes
    /// </summary>
    public class TmContact
    {
        List<BroadCastService.BroadCastServiceClient> tm_stubs = new List<BroadCastService.BroadCastServiceClient>();
        public TmContact(List<string> tm_urls)
        {
            foreach (string url in tm_urls)
            {
                try
                {
                    tm_stubs.Add(new BroadCastService.BroadCastServiceClient(GrpcChannel.ForAddress(url)));
                }
                catch (System.UriFormatException ex)
                {
                    Console.WriteLine("ERROR: Invalid Tm server url");
                }
            }
        }
        public bool BroadCastChanges(List<DadIntProto> writes, string name, List<int> epoch)
        {
            BroadReply reply;
            BroadRequest request = new BroadRequest { TmName = name };
            List<DadIntTmProto> writesTm = new List<DadIntTmProto>();
            foreach (DadIntProto tm in writes) writesTm.Add(new DadIntTmProto { Key = tm.Key, Value = tm.Value });
            request.Epoch.AddRange(epoch);
            request.Writes.AddRange(writesTm);

            foreach (BroadCastService.BroadCastServiceClient stub in tm_stubs)
            {
                // perguntar se temos sempre de receber todos os ack de todos os Tm senão dá revert
                // propagar para todos os servers corretos
                // provavelmente meter quando se recebe a dar broadcast again
                // para isso possivelmente temos uma lista em cada Tm com nºtransacao, nome Tm
                reply = stub.BroadCastAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult(); // tirar isto de syncrono
            }
            return true;
        }
    }
}
