namespace DADTKV_TM
{
    public class MainThread
    {
        Store _store;
        public MainThread(Store st)
        {
            _store = st;
        }
        public void cycle()
        {
            while (true) _store.Verify();
        }
    }
}
