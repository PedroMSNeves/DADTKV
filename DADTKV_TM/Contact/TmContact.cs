using DADTKV_TM.Structs;
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
        public bool BroadCastChanges(string name, int leaseId, int epoch, Store st)
        {
            List<Grpc.Core.AsyncUnaryCall<BroadReply>> replies = new List<Grpc.Core.AsyncUnaryCall<BroadReply>>();
            BroadRequest request = new BroadRequest { TmName = name, LeaseId = leaseId, Epoch = epoch };
            //List<DadIntTmProto> writesTm = new List<DadIntTmProto>();
            int acks = 0;
            int responses = 0;
            //foreach (DadIntProto tm in writes) writesTm.Add(new DadIntTmProto { Key = tm.Key, Value = tm.Value });
            //request.Writes.AddRange(writesTm);

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
            while (responses < tm_stubs.Count)
            {
                Monitor.Wait(st, rd.Next(100, 150));
                for (int i = 0; i < replies.Count; i++)
                {
                    if (replies[i].ResponseAsync.IsCompleted)
                    {
                        if (replies[i].ResponseAsync.Result.Ack == true) acks++;
                        responses++;
                        replies.Remove(replies[i]);
                        i--;
                        if (responses == tm_stubs.Count) break;
                    }
                }
                if (replies.Count == 0) break; //error
            }
            Console.Write("RESULTADO PROPAGATE CHEGOU AOS TMs? ");
            Console.WriteLine(acks);
            return acks == tm_stubs.Count;
        }
        public bool[] DeleteResidualKeys(List<FullLease> residualLeases, int epoch, Store st) 
        {
            List<Grpc.Core.AsyncUnaryCall<ResidualReply>> replies = new List<Grpc.Core.AsyncUnaryCall<ResidualReply>>();
            ResidualDeletionRequest residualDeletionRequest = new ResidualDeletionRequest { Epoch = epoch };
            int responses = 0;
            int[] acks = new int[residualLeases.Count];
            for (int i = 0; i < acks.Length; i++) acks[i] = 0;

            foreach (FullLease lease in residualLeases) 
            {
                LeaseProtoTm leaseProtoTm = new LeaseProtoTm { LeaseId = lease.Lease_number, Tm = lease.Tm_name };
                leaseProtoTm.Keys.AddRange(lease.Keys);
                residualDeletionRequest.ResidualLeases.Add(leaseProtoTm);
            }
            // we have to order the leaseProtoTm list by the lease number
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
            while (responses < tm_stubs.Count) // depois meter maioria
            {
                Monitor.Wait(st, rd.Next(100, 150));
                for (int i = 0; i < replies.Count; i++)
                {
                    if (replies[i].ResponseAsync.IsCompleted)
                    {
                        if (replies[i].ResponseAsync.IsCompletedSuccessfully)
                        {
                            for (int j = 0; j < acks.Length; j++) if(replies[i].ResponseAsync.Result.Acks[i]) acks[j]++;
                        }
                        responses++;
                        replies.Remove(replies[i]);
                        i--;
                        if (responses == tm_stubs.Count) break;
                    }
                }
                if (replies.Count == 0) break; //error
            }
            Console.Write("RESULTADO Residual Lease CHEGOU AOS TMs? ");
            Console.WriteLine(responses);
            foreach (int i in acks) Console.Write(i + " ");
            bool[] bools = new bool[residualLeases.Count];
            for (int i = 0; i < acks.Length; i++)
            {
                if (acks[i] == tm_stubs.Count) bools[i] = true;
                else bools[i] = false;
            }
            // for now returns always true
            return bools;
        }
        public void ConfirmBroadChanges(List<DadIntProto> writes, bool ack)
        {
            if (!ack) return;
            ConfirmBroadChangesRequest confirmRequest = new ConfirmBroadChangesRequest { Ack = ack };
            // we have to order the leaseProtoTm list by the lease number
            List<DadIntTmProto> writesTm = new List<DadIntTmProto>();
            foreach (DadIntProto tm in writes) writesTm.Add(new DadIntTmProto { Key = tm.Key, Value = tm.Value });
            confirmRequest.Writes.AddRange(writesTm);

            foreach (BroadCastService.BroadCastServiceClient stub in tm_stubs)
            {
                stub.ConfirmBroadChangesAsync(confirmRequest); // tirar isto de syncrono
            }

            // for now returns always true
            return;
        }
        public bool ConfirmResidualDeletion(string name, bool[] acks, int epoch)
        {
            ConfirmResidualDeletionRequest confirmRequest = new ConfirmResidualDeletionRequest { TmName = name, Epoch = epoch };
            confirmRequest.Bools.AddRange(acks);
            // we have to order the leaseProtoTm list by the lease number

            foreach (BroadCastService.BroadCastServiceClient stub in tm_stubs)
            {
                stub.ConfirmResidualDeletionAsync(confirmRequest); // tirar isto de syncrono
            }

            // for now returns always true
            return true;
        }
    }
}
//depois de receber maioria mandar msg de confirmacao
//se correr mal mandar nack