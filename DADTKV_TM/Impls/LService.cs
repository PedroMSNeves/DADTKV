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
        private Store _store;
        public List<WaitLeases> waitLeases;
        public LService(Store st, int lm_count)
        {
            _lm_count = lm_count;
            _store = st;
            waitLeases = new List<WaitLeases>();
        }
        /// <summary>
        /// Message from LM to give us the results of a epoch
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<LeaseReply> LeaseBroadCast(LeaseBroadCastRequest request, ServerCallContext context)
        {
            return Task.FromResult(LBCast(request));
        }
        /// <summary>
        /// Sees if already has majority to add has final epoch lease batch
        /// </summary>
        /// <param name="request"></param>
        /// <returns></returns>
        public LeaseReply LBCast(LeaseBroadCastRequest request)
        {
            List<FullLease> leases = new List<FullLease>();
            bool ready = false;
            bool exists = false;
            lock (waitLeases)
            {
                Console.WriteLine("EPOCH: " + request.Epoch);
                // To ignore other responses of the epoch after we already had majority
                if (_store.GetEpoch() > request.Epoch) return new LeaseReply { Ack = true };

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
                            wl.IncreaseAcks();
                            if (wl.Acks > Majority()) ready = true;
                        }
                        remove.Add(wl);
                    }
                }
                // Adds if it doesnt exist
                if (!exists)
                {
                    if (_lm_count == 1) _store.WaitLeases(leases, request.Epoch);
                    else waitLeases.Add(new WaitLeases(request.Epoch, leases));
                }
                Console.WriteLine(exists + " " +  ready);
                // Passes to the store if already has majority
                if (ready)
                {
                    foreach (WaitLeases wl in remove) waitLeases.Remove(wl);
                    _store.WaitLeases(leases, request.Epoch);
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
        private int Majority()
        {
            return (int)Math.Floor((decimal)((_lm_count) / 2)); // perguntar se é dos vivos ou do total
        }

    }
}
