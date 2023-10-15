using DADTKV_Client_Lib;
public class ClientLogic
{
    private DADTKV_Client _dadtkv;
    private string _name;

    public ClientLogic(string name)
    {
        _dadtkv = new DADTKV_Client();
        _name = name;
    }

    /// <summary>
    /// Adds a new Tm server url
    /// </summary>
    /// <param name="server"></param>
    public void AddServer(string server)
    {
        _dadtkv.AddServer(server);
    }

    /// <summary>
    /// Thread (client) sleeps for a X amount of seconds
    /// </summary>
    public void Sleep(int miliseconds)
    {
        Console.WriteLine("Client will sleep for: " + miliseconds + " miliseconds");
        Thread.Sleep(miliseconds);
    }

    /// <summary>
    /// Parses the input string into the read and write parts of the transaction
    /// Requests a transaction
    /// </summary>
	public void Transaction(List<string> reads, Dictionary<string, int> writes)
    {
        List<DadInt> reply = _dadtkv.TxSubmit(this._name, reads, DictToList(writes));
        if (reply.Any()) Console.WriteLine("Reads replies:");
        foreach (DadInt rep in reply) { Console.WriteLine(rep.ToString()); }
    }
    private List<DadInt> DictToList(Dictionary<string, int> writes)
    {
        List<DadInt> dadInts = new List<DadInt>();
        foreach (KeyValuePair<string, int> dad in writes) { dadInts.Add(new DadInt(dad.Key, dad.Value)); }
        return dadInts;
    }

    /// <summary>
    /// Asks for the status of the servers
    /// </summary>
    public void Status()
    {
        _dadtkv.Status();
    }
}
