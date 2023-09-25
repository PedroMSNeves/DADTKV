namespace Parse_Lib
{
    public class Parse
    {
        public static int ParseInt()
        {
            int val;
            string newline = Console.ReadLine();
            if (newline == null) { return -1; }
            if (int.TryParse(newline, out val))
            {
                return val;
            }
            else
            {
                return -1;
            }
        }
        public static string ParseStringNoSpaces()
        {
            string newline = Console.ReadLine();
            string reduced = "";
            if( newline == null ) { return ""; }
            int i=0;
            bool ask=false;
            while(i < newline.Length) 
            {
                if (!ask && newline[i] == '"') ask = true;
                else if (ask && newline[i] == '"') ask = false;
                else if (!ask && (newline[i] == ' ' || newline[i] == '\t')) { i++; continue; }
                reduced += newline[i];
                i++;
            }
            return reduced;
        }
        public static bool ParseReads(string newline, ref int i, out List<string> reads) 
        {
            reads = new List<string>();
            if (newline[i] != '('){return false;}
            i++;
            while (i < newline.Length && newline[i] != ')') 
            {
                string name = "";
                if(!ParseString(newline, ref i, ref name)) return false;
                reads.Add(name);

                if (newline[i] == ',') { i++; }
                else if (newline[i] != ')') return false;
            }
            if (i >= newline.Length) return false;
            i++;
            if (i >= newline.Length) return false;
            return true;
        }
        public static bool ParseWrites(string newline, ref int i, out Dictionary<string, int> writes)
        {
            writes = new Dictionary<string, int>();
            if (newline[i] != '(') { return false; }
            i++;
            while (i < newline.Length && newline[i] != ')')
            {
                string name;
                int val;
                if (!ParseDict(newline, ref i, out name, out val)) return false;
                writes.Add(name,val);

                if (newline[i] == ',') { i++; }
                else if (newline[i] != ')') return false;
            }
            if (i >= newline.Length) return false;
            return true;
        }
        public static bool ParseDict(string newline, ref int i, out string name, out int val)
        {
            name = "";
            val = 0;
            if (newline[i] != '<') { return false; }
            i++;
            if (!ParseString(newline,ref i, ref name)) return false;
            if (newline[i] != ',') { return false; }
            if (++i >= newline.Length) return false;
            if (!ParseNumber(newline, ref i, ref val)) return false;            
            return true;
        }
        public static bool ParseNumber(string newline, ref int i, ref int val)
        {
            bool neg = false;
            if (newline[i] == '-')
            {
                neg = true;
                if (++i >= newline.Length) return false;
            }
            if (newline[i] == '>') { return false; }
            while (i < newline.Length && newline[i] != '>') 
            {
                if (Char.IsNumber(newline[i])) val = val * 10 + newline[i] - '0';
                else return false;
                i++;
            }
            if (i >= newline.Length) return false;
            i++;
            if (i >= newline.Length) return false;
            if (neg) val = -val;
            return true;
        }
        public static bool ParseString(string newline, ref int i, ref string name)
        {
            if (newline[i] != '"') { return false; }
            i++;
            while (i < newline.Length && newline[i] != '"') 
            {
                name += newline[i];
                i++;
            }
            if(i >= newline.Length) return false;
            i++;
            if(i >= newline.Length) return false;
            return true;
        }
    }
}