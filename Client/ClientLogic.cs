using System;
using DADTKV_Client_Lib;
public class ClientLogic
{
	public ClientLogic()
	{
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
        string newline = Console.ReadLine();
        if (newline == null) { Console.WriteLine("ERROR: Invalid Transaction"); return; }
		List<string> reads = new List<string>();
		bool read = false;
		List<DadInt> writes = new List<DadInt>();
        int i = 0;
		while (i < newline.Length)
		{
			
        }
		
        //Todo
    }
}
