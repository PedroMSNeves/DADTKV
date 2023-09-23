using System;
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
        string newline = Console.ReadLine();
        if (newline == null) { Console.WriteLine("ERROR: Invalid Transaction"); return; }
		List<string> reads = new List<string>();
		bool read = false;
		List<DadInt> writes = new List<DadInt>();
        int i = 0;
		
		while (i < newline.Length)
		{
			if (newline[i] == '(') 
			{
                i++;
                if (i >= newline.Length)
                {
                    Console.WriteLine("ERROR: Bad Transaction Format");
                    return;
                }
                while (newline[i] == ' ' || newline[i] == '\t')
                {
                    //verificar se e menor que length
                    i++;
                    if (i >= newline.Length)
                    {
                        Console.WriteLine("ERROR: Bad Transaction Format");
                        return;
                    }
                }
                if (!read)
                {
                    bool ask = false;
                    bool com = false;
                    if (newline[i] == ')') { read = true; }
                    else if (newline[i] == '"')
                    {
                        string st = "";
                        i++;
                        if (i >= newline.Length)
                        {
                            Console.WriteLine("ERROR: Bad Transaction Format");
                            return;
                        }
                        ask = true;
                        while (!(newline[i] == ')' && !ask && !com))
                        {
                            if (ask && newline[i] == '"')
                            {
                                reads.Add(st);
                                st = "";
                                ask = false;
                                com = false;
                            }
                            else if (ask)
                            {
                                st += newline[i];
                            }
                            else if (!com && newline[i] == ',')
                            {
                                com = true;
                            }
                            else if (com && newline[i] == '"')
                            {
                                ask = true;
                            }
                            else if (!(newline[i] == ' ' || newline[i] == '\t'))
                            {
                                Console.WriteLine("ERROR: Bad Read Transaction Format");
                                return;
                            }
                            i++;
                            if (i > newline.Length)
                            {
                                Console.WriteLine("ERROR: Bad Read Transaction Format");
                                return;
                            }
                        }
                        read = true;
                    }
                    else
                    {
                        Console.WriteLine("ERROR: Bad Read Transaction Format");
                        return;
                    }
                    foreach (string s in reads) { Console.WriteLine(s); }
                }
                else
                {
                    int ask = 0;
                    bool com = false;
                    bool bra = false;
                    bool numE = false;
                    if (newline[i] == ')') { break; } // break's out of the cycle
                    else if (newline[i] == '<')
                    {
                        string st = "";
                        int val = -1;
                        i++;
                        if (i >= newline.Length)
                        {
                            Console.WriteLine("ERROR: Bad Transaction Format");
                            return;
                        }
                        bra = true;
                        while(newline[i] == ' ' || newline[i] == '\t')
                        {
                            //verificar se e menor que length
                            i++;
                            if (i >= newline.Length)
                            {
                                Console.WriteLine("ERROR: Bad Transaction Format");
                                return;
                            }
                        }
                        if (newline[i] != '"')
                        {
                            Console.WriteLine("ERROR: Bad Write Transaction Format");
                            return;
                        }
                        ask = 1;
                         i++;
                        if (i >= newline.Length)
                        {
                            Console.WriteLine("ERROR: Bad Transaction Format");
                            return;
                        }
                        while (!(newline[i] == ')' && ask==0 && !com && !bra))
                        {
                            //////////////////////////////////////
                            ///
                            if (bra)
                            {
                                if (ask==2 && com && val != -1 && newline[i] == '>')
                                {
                                    writes.Add(new DadInt(st, val));
                                    st = "";
                                    val = -1;
                                    bra = false;
                                    ask = 0;
                                    com = false;
                                    numE = false;
                                }
                                else if (ask == 1 && newline[i] == '"')
                                {
                                    ask = 2;
                                }
                                else if (ask == 1)
                                {
                                    st += newline[i];
                                }
                                else if (!com && ask == 2 && newline[i] == ',')
                                {
                                    com = true;

                                }
                                else if (com && ask == 2) 
                                {
                                    if (val == -1) 
                                    {
                                        if (Char.IsNumber(newline[i]))
                                            val = newline[i]-'0';

                                        else if (!(newline[i] == ' ' || newline[i] == '\t'))
                                        {
                                            Console.WriteLine("ERROR: Bad Write Transaction Format");
                                            return;
                                        }  
                                    }
                                    else if (Char.IsNumber(newline[i]) && !numE) val = val * 10 + newline[i]-'0';
                                    else
                                    {
                                        if (!numE && val > -1 && (newline[i] == ' ' || newline[i] == '\t')) numE = true;
                                        else
                                        {
                                            Console.WriteLine("ERROR: Bad Write Transaction Format");
                                            return;
                                        }

                                    }
                                }
                                else if (!(newline[i] == ' ' || newline[i] == '\t'))
                                {
                                    if (!(ask == 0 && newline[i] == '"'))
                                    {
                                        Console.WriteLine("ERROR: Bad Write Transaction Format");
                                        return;
                                    }
                                    ask = 1;
                                }
                            } 
                            else if( !com && newline[i] == ',')
                            {
                                com = true;
                            }
                            else if( com && newline[i] == '<')
                            {
                                bra = true;
                                com = false;
                            }
                            else if (!(newline[i] == ' ' || newline[i] == '\t'))
                            {
                                Console.WriteLine("ERROR: Bad Write Transaction Format");
                                return;
                            }

                            //////////////////////////////////////
                            i++;
                            if (i >= newline.Length)
                            {
                                Console.WriteLine("ERROR: Bad Write Transaction Format");
                                return;
                            }
                        }
                        foreach(DadInt a in writes) Console.WriteLine(a.ToString());
                        break;
                    }
                    else
                    {
                        Console.WriteLine("ERROR: Bad Write Transaction Format");
                        return;
                    }
                }
			}
			else if (newline[i] == ' ' || newline[i] == '\t'){ } // does nothing
			else
			{
                Console.WriteLine("ERROR: Bad Transaction Format"); 
				return;
            }
			i++;

        }
        List<DadInt> reply = _dadtkv.TxSubmit(reads, writes);
        Console.WriteLine("Reads replies:");
        foreach (DadInt rep in reply) { Console.WriteLine(rep.ToString()); }
        return;
        //Todo
    }
}
