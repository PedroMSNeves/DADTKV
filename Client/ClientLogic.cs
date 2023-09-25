using System;
using System.Collections;
using DADTKV_Client_Lib;
public class ClientLogic
{
    public DADTKV_Client _dadtkv;
	public ClientLogic()
	{
        _dadtkv = new DADTKV_Client();
	}
	public void Sleep()
	{
		int miliseconds = Parse_Lib.Parse.ParseInt();
		if(miliseconds < 0 ) { Console.WriteLine("ERROR: Input valid number of miliseconds"); return; }
        Console.WriteLine("Client will sleep for: " + miliseconds + " miliseconds");
		Thread.Sleep(miliseconds);
	}
	public void Transaction()
	{
        string newline = Parse_Lib.Parse.ParseStringNoSpaces();
        if (newline == "") { Console.WriteLine("ERROR: Invalid Transaction"); return; }
		List<string> reads;
		Dictionary<string,int> writes;
        int i = 0;
		


        if(!Parse_Lib.Parse.ParseReads(newline, ref i,out reads)) { Console.WriteLine("ERROR: Bad Read Transaction Format"); return; }
        foreach(string s in reads) Console.WriteLine(s);
        if (!Parse_Lib.Parse.ParseWrites(newline, ref i, out writes)) { Console.WriteLine("ERROR: Bad Write Transaction Format"); return; }
        foreach (KeyValuePair<string,int> a in writes) Console.WriteLine(a);
        
        List<DadInt> reply = _dadtkv.TxSubmit(reads, DictToList(writes));
        Console.WriteLine("Reads replies:");
        foreach (DadInt rep in reply) { Console.WriteLine(rep.ToString()); }
        return;
    }
    private List<DadInt> DictToList(Dictionary<string,int> writes)
    {
        List<DadInt> dadInts = new List<DadInt>();
        foreach (KeyValuePair<string, int> dad in writes) { dadInts.Add(new DadInt(dad.Key, dad.Value)); }
        return dadInts;
    }
}
