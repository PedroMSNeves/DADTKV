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
        List<string> tm_names;
        List<BroadCastService.BroadCastServiceClient> tm_stubs = null;
        List<GrpcChannel> tm_channels = new List<GrpcChannel>();
        bool[] bitmap;

        public TmContact(List<string> tm_urls, List<string> t_names)
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
            tm_names = t_names;
            bitmap = new bool[tm_channels.Count];
            for (int i = 0; i < bitmap.Length; i++) bitmap[i] = true;
        }

        /// <summary>
        /// Puts the server down
        /// </summary>
        /// <param name="name"></param>
        public void CrashedServer(string name)
        {
            for (int i = 0; i < tm_names.Count; i++)
            {
                if (tm_names[i] == name)
                {
                    bitmap[i] = false;
                    break;
                }
            }
        }

        /// <summary>
        /// Pings 
        /// </summary>
        /// <param name="name"></param>
        public bool ContactSuspect(string name, Store st)
        {
            for (int i = 0; i < tm_names.Count; i++)
            {
                if (tm_names[i] == name)
                {
                    if (bitmap[i])
                    {
                        return PingSuspect(i, st);
                    }
                    else return false;
                }
            }
            return false;
        }

        public bool PingSuspect(int index, Store st)
        {
            Grpc.Core.AsyncUnaryCall<PingTm> ping = tm_stubs[index].PingSuspectAsync(new PingTm(), new CallOptions(deadline: DateTime.UtcNow.AddSeconds(10)));
            Random rd = new Random();

            while (true)
            {
                Monitor.Wait(st, rd.Next(100, 150));
                if (ping.ResponseAsync.IsCompleted)
                {
                    if (ping.ResponseAsync.IsCompletedSuccessfully) return true;
                    return false;
                }
            }
        }
        public bool KillSuspect(string name, Store st)
        {
            List<Grpc.Core.AsyncUnaryCall<BroadReply>> replies = new List<Grpc.Core.AsyncUnaryCall<BroadReply>>();
            KillRequestTm request = new KillRequestTm { TmName = name };
            int acks = 1;
            int responses = 0;
            for (int i = 0; i < tm_names.Count; i++)
            {

                if (tm_names[i] != name && bitmap[i])
                {
                    replies.Add(tm_stubs[i].KillSuspectAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(10))));
                }
                else
                {
                    replies.Add(null);
                    responses++;
                }
            }

            Random rd = new Random();
            while (responses < tm_stubs.Count)
            {
                Monitor.Wait(st, rd.Next(100, 150));
                for (int i = 0; i < replies.Count; i++)
                {
                    if (replies[i] != null)
                    {
                        if (replies[i].ResponseAsync.IsCompleted)
                        {
                            if (replies[i].ResponseAsync.IsFaulted)
                            {
                                bitmap[i] = false;
                            }
                            else if (replies[i].ResponseAsync.Result.Ack == true) acks++;
                            responses++;
                            replies[i] = null;
                        }
                    }
                }
            }
            return acks == TmAlive();
        }

        public bool BroadCastChanges(string name, int leaseId, int epoch, ref bool killMe, Store st)
        {
            List<Grpc.Core.AsyncUnaryCall<BroadReply>> replies = new List<Grpc.Core.AsyncUnaryCall<BroadReply>>();
            BroadRequest request = new BroadRequest { TmName = name, LeaseId = leaseId, Epoch = epoch };
            int acks = 1;
            int responses = 0;
            // Initialize TM's stubs
            if (tm_stubs == null)
            {
                tm_stubs = new List<BroadCastService.BroadCastServiceClient>();
                foreach (GrpcChannel channel in tm_channels)
                {
                    tm_stubs.Add(new BroadCastService.BroadCastServiceClient(channel));
                }
            }
            // If true, we will never be able to reach majority
            if (TmAlive() <= Majority())
            {
                killMe = true;
                return false;
            }
            // Sends request to all the Alive Tm's
            for (int i = 0; i < tm_stubs.Count; i++)
            {
                if (bitmap[i])
                {
                    replies.Add(tm_stubs[i].BroadCastAsync(request, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(10))));
                }
                else
                {
                    replies.Add(null);
                    responses++;
                }
            }
            Random rd = new Random();
            while (responses < tm_stubs.Count)
            {
                Monitor.Wait(st, rd.Next(100, 150));
                for (int i = 0; i < replies.Count; i++)
                {
                    if (replies[i] != null)
                    {
                        if (replies[i].ResponseAsync.IsCompleted)
                        {
                            if (replies[i].ResponseAsync.IsFaulted)
                            {
                                Console.WriteLine(replies[i].ResponseAsync.Exception.ToString());
                                bitmap[i] = false;
                            }
                            else if (replies[i].ResponseAsync.Result.Ack == true) acks++;
                            responses++;
                            replies[i] = null;
                        }
                    }
                }
            }
            Console.Write("RESULTADO PROPAGATE CHEGOU AOS TMs? ");
            Console.WriteLine(acks);

            return acks == TmAlive();
        }
        public bool[] DeleteResidualKeys(List<FullLease> residualLeases, int epoch, ref bool killMe, Store st)
        {
            List<Grpc.Core.AsyncUnaryCall<ResidualReply>> replies = new List<Grpc.Core.AsyncUnaryCall<ResidualReply>>();
            ResidualDeletionRequest residualDeletionRequest = new ResidualDeletionRequest { Epoch = epoch };
            int responses = 0;
            int[] acks = new int[residualLeases.Count];
            for (int i = 0; i < acks.Length; i++) acks[i] = 0;
            // Prepare the request
            foreach (FullLease lease in residualLeases)
            {
                LeaseProtoTm leaseProtoTm = new LeaseProtoTm { LeaseId = lease.Lease_number, Tm = lease.Tm_name };
                leaseProtoTm.Keys.AddRange(lease.Keys);
                residualDeletionRequest.ResidualLeases.Add(leaseProtoTm);
            }
            // Initialize TM's stubs
            if (tm_stubs == null)
            {
                tm_stubs = new List<BroadCastService.BroadCastServiceClient>();
                foreach (GrpcChannel channel in tm_channels)
                {
                    tm_stubs.Add(new BroadCastService.BroadCastServiceClient(channel));
                }
            }
            if (TmAlive() <= Majority())
            {
                killMe = true;
                return null;
            }
            // Sends request to all the Alive Tm's
            for (int i = 0; i < tm_stubs.Count; i++)
            {
                if (bitmap[i])
                {
                    replies.Add(tm_stubs[i].ResidualDeletionAsync(residualDeletionRequest, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(10))));
                }
                else
                {
                    replies.Add(null);
                    responses++;
                }
            }
            Random rd = new Random();
            while (responses < tm_stubs.Count)
            {
                Monitor.Wait(st, rd.Next(100, 150));
                for (int i = 0; i < replies.Count; i++)
                {
                    if (replies[i] != null)
                    {
                        if (replies[i].ResponseAsync.IsCompleted)
                        {
                            if (replies[i].ResponseAsync.IsFaulted)
                            {
                                bitmap[i] = false;
                            }
                            else if (replies[i].ResponseAsync.IsCompletedSuccessfully)
                            {
                                for (int j = 0; j < acks.Length; j++) if (replies[i].ResponseAsync.Result.Acks[j]) acks[j]++;
                            }
                            responses++;
                            replies[i] = null;
                        }
                    }
                }
            }
            Console.Write("RESULTADO Residual Lease CHEGOU AOS TMs? ");
            Console.WriteLine(responses);
            foreach (int i in acks) Console.Write(i + " ");
            bool[] bools = new bool[residualLeases.Count];
            Console.WriteLine("ACKS");
            int alive = TmAlive();
            for (int i = 0; i < acks.Length; i++)
            {
                Console.Write(acks[i] + " ");
                if (acks[i] + 1 == alive) bools[i] = true;
                else bools[i] = false;
                Console.Write(bools[i] + " ");
            }
            Console.WriteLine();
            Console.WriteLine("ACKS");

            return bools;
        }
        public void ConfirmBroadChanges(List<DadIntProto> writes, bool ack)
        {
            // Ignores if can't propagate
            if (!ack) return;
            // Prepares request
            ConfirmBroadChangesRequest confirmRequest = new ConfirmBroadChangesRequest { Ack = ack };
            List<DadIntTmProto> writesTm = new List<DadIntTmProto>();
            foreach (DadIntProto tm in writes) writesTm.Add(new DadIntTmProto { Key = tm.Key, Value = tm.Value });
            confirmRequest.Writes.AddRange(writesTm);

            // Sends request to all the Alive Tm's
            for (int i = 0; i < tm_stubs.Count; i++)
            {
                if (bitmap[i])
                {
                    tm_stubs[i].ConfirmBroadChangesAsync(confirmRequest); // tirar isto de syncrono
                }
            }
            return;
        }
        public bool ConfirmResidualDeletion(string name, bool[] acks, int epoch)
        {
            ConfirmResidualDeletionRequest confirmRequest = new ConfirmResidualDeletionRequest { TmName = name, Epoch = epoch };
            confirmRequest.Bools.AddRange(acks);
            // we have to order the leaseProtoTm list by the lease number

            for (int i = 0; i < tm_stubs.Count; i++)
            {
                if (bitmap[i])
                {
                    tm_stubs[i].ConfirmResidualDeletionAsync(confirmRequest); // tirar isto de syncrono
                }
            }

            return true;
        }
        /// <summary>
        /// In reality is majority - 1
        /// </summary>
        /// <returns></returns>
        private int Majority()
        {
            return (int)Math.Floor((decimal)((tm_stubs.Count + 1) / 2)); // perguntar se é dos vivos ou do total
        }
        private int TmAlive()
        {
            int count = 1;
            foreach (bool b in bitmap) if (b) count++;
            return count;
        }
        public List<string> GetDeadNames()
        {
            List<string> names = new List<string>();
            for (int i = 0; i < tm_names.Count; i++)
            {
                if (!bitmap[i])
                {
                    names.Add(tm_names[i]);
                }
            }
            return names;
        }
    }
}