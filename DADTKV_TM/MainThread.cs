namespace DADTKV_TM
{
    public class MainThread
    {
        Store _store;
        public int _myCrashEpoch;
        public Dictionary<int, List<string>> _crashedP;
        int _last_epoch_read = -1;
        public MainThread(Store st, int myCrashEpoch, Dictionary<int, List<string>> crashedP)
        {
            _store = st;
            _myCrashEpoch = myCrashEpoch;
            _crashedP = crashedP;
        }
        public void cycle()
        {
            Random rd = new Random();
            while (true)
            {
                _store.Execute();
                _store.RemoveResidual();
                Thread.Sleep(rd.Next(100,500));
                lock (_store)
                {
                    if (_store.GetKillMe()) return;
                    int epoch = _store.GetEpoch();

                    // To see if someone crashed
                    for (int i = _last_epoch_read + 1; i <= epoch; i++)
                    {
                        // If we are signaled to crash, we close the servers and then end the program
                        if (_myCrashEpoch != -1 && i >= _myCrashEpoch) return;

                        if (_crashedP.ContainsKey(i))
                        {
                            foreach (string name in _crashedP[i])
                            {
                                // If we are not signaled to crash, we look for the name that crashed
                                _store.CrashedServer(name);
                            }
                        }
                    }
                    _last_epoch_read = epoch;

                }
            }
        }
    }
}
