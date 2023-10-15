using Grpc.Core;
using Grpc.Net.Client;

namespace DADTKV_TM.Contact
{
    /// <summary>
    /// Contact with Transaction Manager, to propagate changes
    /// </summary>
    public class TmContact
    {
        List<BroadCastService.BroadCastServiceClient> tm_stubs = null;
        List<GrpcChannel> tm_channels = new List<GrpcChannel>();

        public TmContact(List<string> tm_urls)
        {
            foreach (string url in tm_urls)
            {
                try
                {
                    tm_channels.Add(GrpcChannel.ForAddress(url));
                }
                catch (System.UriFormatException)
                {
                    Console.WriteLine("ERROR: Invalid Tm server url");
                }
            }
        }
        public bool BroadCastChanges(List<DadIntProto> writes, string name)
        {
            BroadReply reply;
            BroadRequest request = new BroadRequest { TmName = name};
            List<DadIntTmProto> writesTm = new List<DadIntTmProto>();
            foreach (DadIntProto tm in writes) writesTm.Add(new DadIntTmProto { Key = tm.Key, Value = tm.Value });
            request.Writes.AddRange(writesTm);

            if (tm_stubs == null)
            {
                tm_stubs = new List<BroadCastService.BroadCastServiceClient>();
                foreach (GrpcChannel channel in tm_channels)
                {
                    tm_stubs.Add(new BroadCastService.BroadCastServiceClient(channel));
                }
            }

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
        public bool DeleteResidualKeys(List<string> residualKeys , string name) 
        {
            BroadReply reply;
            ResidualDeletionRequest residualDeletionRequest = new ResidualDeletionRequest { TmName = name };
            residualDeletionRequest.FirstKeys.AddRange(residualKeys);
            if (tm_stubs == null)
            {
                tm_stubs = new List<BroadCastService.BroadCastServiceClient>();
                foreach (GrpcChannel channel in tm_channels)
                {
                    tm_stubs.Add(new BroadCastService.BroadCastServiceClient(channel));
                }
            }
            foreach (BroadCastService.BroadCastServiceClient stub in tm_stubs)
            {
                reply = stub.ResidualDeletionAsync(residualDeletionRequest, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(5))).GetAwaiter().GetResult(); // tirar isto de syncrono
            }
            // for now returns always true
            return true;
        }
    }
}
