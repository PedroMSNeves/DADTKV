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
        public bool BroadCastChanges(List<DadIntProto> writes, string name, int epoch)
        {
            BroadReply reply;
            BroadRequest request = new BroadRequest { TmName = name , Epoch = epoch };
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
                //Console.WriteLine("In stubs");
                //Console.ReadKey();
                reply = stub.BroadCastAsync(request).GetAwaiter().GetResult(); // tirar isto de syncrono
            }
            return true;
        }
        public bool DeleteResidualKeys(List<string> residualKeys , string name, int epoch) 
        {
            List<Grpc.Core.AsyncUnaryCall<BroadReply>> replies = new List<Grpc.Core.AsyncUnaryCall<BroadReply>>();
            ResidualDeletionRequest residualDeletionRequest = new ResidualDeletionRequest { TmName = name, Epoch = epoch };
            int acks = 0;
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
                replies.Add(stub.ResidualDeletionAsync(residualDeletionRequest)); // tirar isto de syncrono
            }
            foreach (Grpc.Core.AsyncUnaryCall<BroadReply> reply in replies)
            {
                if (reply.ResponseAsync.Result.Ack) acks++;
            }
            Console.Write("RESULTADO PROPAGATE CHEGOU AOS TMs? ");
            Console.WriteLine(acks);

            // for now returns always true
            return true;
        }
    }
}
