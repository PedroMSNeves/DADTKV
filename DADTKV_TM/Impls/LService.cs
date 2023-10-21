using Grpc.Core;
using DADTKV_TM.Structs;

namespace DADTKV_TM.Impls
{
    /// <summary>
    /// Receive new leases from the Lease Managers
    /// </summary>
    public class LService : LeaseService.LeaseServiceBase
    {
        private int _lm_count;
        private Store store;
        public List<WaitLeases> waitLeases;
        public LService(Store st, int lm_count)
        {
            _lm_count = lm_count;
            store = st;
            waitLeases = new List<WaitLeases>();
        }
        public override Task<LeaseReply> LeaseBroadCast(LeaseBroadCastRequest request, ServerCallContext context)
        {
            return Task.FromResult(LBCast(request));
        }
        public LeaseReply LBCast(LeaseBroadCastRequest request)
        {
            List<FullLease> leases = new List<FullLease>();
            bool ready = false;
            bool exists = false;
            lock (waitLeases)
            {
                foreach (LeaseProto lp in request.Leases)
                {
                    Console.WriteLine("LM: " + lp.ToString());
                    leases.Add(new FullLease(lp.Tm, request.Epoch, lp.Keys.ToList(), lp.LeaseId));
                }
                List<WaitLeases> remove = new List<WaitLeases>();
                foreach (WaitLeases wl in waitLeases)
                {
                    if (wl.Epoch == request.Epoch)
                    {
                        if (Equal(wl.Leases,leases))
                        {
                            exists = true;
                            wl.increaseAcks();
                            if (wl.Acks == _lm_count) ready = true;
                        }
                        remove.Add(wl);
                    }
                }
                //adicionar se nao existir
                if (!exists)
                {
                    if (_lm_count == 1) store.WaitLeases(leases, request.Epoch);
                    else waitLeases.Add(new WaitLeases(request.Epoch, leases));
                }
                Console.WriteLine("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
                Console.WriteLine(exists + " " +  ready);
                //Console.ReadKey();
                if (ready)
                {
                    Console.WriteLine("ADD NEW LEASES");
                    foreach (WaitLeases wl in remove) waitLeases.Remove(wl);
                    store.WaitLeases(leases, request.Epoch);
                }

            }
            return new LeaseReply { Ack = true };
        }
        public bool Equal(List<FullLease> others1, List<FullLease> others2)
        {
            if( others1.Count != others2.Count) return false;
            for (int i = 0; i < others1.Count; i++)
            {

                if (!others1[i].Equal(others2[i])) return false;
            }
            return true;
        }

    }
}
