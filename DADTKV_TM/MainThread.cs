namespace DADTKV_TM
{
    public class MainThread
    {
        Store _store;
        public int _myCrashEpoch;
        public Dictionary<int, List<string>> _crashedP;
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
                    int epoch = _store.GetEpoch();
                    // If we are signaled to crash, we close the servers and then end the program
                    if (epoch == _myCrashEpoch) return; 

                    // To see if someone crashed
                    if (_crashedP.ContainsKey(epoch))
                    {
                        foreach (string name in _crashedP[epoch])
                        {

                            // If we are not signaled to crash, we look for the name that crashed
                            _store.CrashedServer(name);
                        }
                    }
                }
            }
        }
    }
}
