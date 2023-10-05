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
            while (true)
            {
                /*
                 * Verificar todos os requests
                 * Retirar o primeiro request se possivel
                 * 
                 */
                _store.Verify(); // Verificar todos os requests e mete os a S T ou N
                 // tenta executar 1 pedido (o primeiro ou algum depois caso nao tenha intersessoes)(de momento vai ser so o primeiro)


            }
        }
    }
}
