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
        public bool BroadCastChanges(List<DadIntProto> writes, string name, int epoch, Store st)
        {
            List<Grpc.Core.AsyncUnaryCall<BroadReply>> replies = new List<Grpc.Core.AsyncUnaryCall<BroadReply>>();
            BroadRequest request = new BroadRequest { TmName = name, Epoch = epoch };
            List<DadIntTmProto> writesTm = new List<DadIntTmProto>();
            int acks = 0;
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
                replies.Add(stub.BroadCastAsync(request)); // tirar isto de syncrono
            }
            Random rd = new Random();
            while (acks < tm_stubs.Count)
            {
                Monitor.Wait(st, rd.Next(100, 150));
                for (int i = 0; i < replies.Count; i++)
                {
                    if (replies[i].ResponseAsync.IsCompleted)
                    {
                        if (replies[i].ResponseAsync.Result.Ack == true) acks++;
                        replies.Remove(replies[i]);
                        i--;
                        if (acks == tm_stubs.Count) break;
                    }
                }
                if (replies.Count == 0) break; //error
            }
            Console.Write("RESULTADO PROPAGATE CHEGOU AOS TMs? ");
            Console.WriteLine(acks);
            return true;
        }
        public bool DeleteResidualKeys(List<string> residualKeys , string name, int epoch, Store st) 
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
            Random rd = new Random();
            while (acks < tm_stubs.Count)
            {
                Monitor.Wait(st, rd.Next(100, 150));
                for (int i = 0; i < replies.Count; i++)
                {
                    if (replies[i].ResponseAsync.IsCompleted)
                    {
                        if (replies[i].ResponseAsync.Result.Ack == true) acks++;
                        replies.Remove(replies[i]);
                        i--;
                        if (acks == tm_stubs.Count) break;
                    }
                }
                if (replies.Count == 0) break; //error
            }
            Console.Write("RESULTADO Residual Lease CHEGOU AOS TMs? ");
            Console.WriteLine(acks);

            // for now returns always true
            return true;
        }
    }
}
//depois de receber maioria mandar msg de confirmacao
//se correr mal mandar nack