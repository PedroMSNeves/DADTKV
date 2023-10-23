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
            Random rd = new Random();
            while (true)
            {
                _store.Execute();
                _store.RemoveResidual();
                Thread.Sleep(rd.Next(100,500));
            }
        }
    }
}
