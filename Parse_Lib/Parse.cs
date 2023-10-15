namespace Parse_Lib
{
    public class Parse
    {
        /// <summary>
        /// Tries to read an int from the input, gives -1 if it does not succeed
        /// </summary>
        /// <returns></returns>
        public static int ParseInt(string word)
        {
            int val;
            if (word == null) return -1;
            if (int.TryParse(word, out val)) return val;
            return -1;
        }

        /// <summary>
        /// Tries to parse the string to have a string with no "normal" spaces (does not eliminate the spaces in names)
        /// </summary>
        /// <returns></returns>
        public static string ParseStringNoSpaces(string newline)
        {
            string reduced = "";
            if (newline == null) { return ""; }
            int i = 0;
            bool quotes = false;
            while (i < newline.Length)
            {
                if (!quotes && newline[i] == '"') quotes = true;
                else if (quotes && newline[i] == '"') quotes = false;
                else if (!quotes && (newline[i] == ' ' || newline[i] == '\t')) { i++; continue; }
                reduced += newline[i];
                i++;
            }
            return reduced;
        }

        /// <summary>
        /// Tries to parse the read part of a transaction, from a string to a List of strings
        /// </summary>
        /// <param name="newline"></param>
        /// <param name="i"></param>
        /// <param name="reads"></param>
        /// <returns></returns>
        public static bool ParseReads(string line, ref int i, out List<string> reads)
        {
            reads = new List<string>();
            if (line[i] != '(') { return false; }
            i++;
            while (i < line.Length && line[i] != ')')
            {
                string name = "";
                if (!ParseString(line, ref i, ref name)) return false;
                reads.Add(name);

                if (line[i] == ',') { i++; }
                else if (line[i] != ')') return false;
            }
            if (i >= line.Length) return false;
            i++;
            if (i >= line.Length) return false;
            return true;
        }

        /// <summary>
        /// Tries to parse the write part of a transaction, from a string to a Dictionary of <string,int> (key,value)
        /// </summary>
        /// <param name="newline"></param>
        /// <param name="i"></param>
        /// <param name="writes"></param>
        /// <returns></returns>
        public static bool ParseWrites(string line, ref int i, out Dictionary<string, int> writes)
        {
            writes = new Dictionary<string, int>();
            if (line[i] != '(') { return false; }
            i++;
            while (i < line.Length && line[i] != ')')
            {
                string name;
                int val;
                if (!ParseDict(line, ref i, out name, out val)) return false;
                writes.Add(name, val);

                if (line[i] == ',') { i++; }
                else if (line[i] != ')') return false;
            }
            if (i >= line.Length) return false;
            return true;
        }
        /// <summary>
        /// Tries to parse an entry of a Dictionary of <string,int> (key,value)
        /// </summary>
        /// <param name="newline"></param>
        /// <param name="i"></param>
        /// <param name="name"></param>
        /// <param name="val"></param>
        /// <returns></returns>
        public static bool ParseDict(string newline, ref int i, out string name, out int val)
        {
            name = "";
            val = 0;
            if (newline[i] != '<') return false;

            if (++i >= newline.Length) return false;
            if (!ParseString(newline, ref i, ref name)) return false;
            if (newline[i] != ',') return false;
            if (++i >= newline.Length) return false;
            if (!ParseNumber(newline, ref i, ref val)) return false;
            return true;
        }
        /// <summary>
        /// Tries to parse a number from a string
        /// </summary>
        /// <param name="newline"></param>
        /// <param name="i"></param>
        /// <param name="val"></param>
        /// <returns></returns>
        public static bool ParseNumber(string newline, ref int i, ref int val)
        {
            bool neg = false;
            if (newline[i] == '-')
            {
                neg = true;
                if (++i >= newline.Length) return false;
            }
            if (newline[i] == '>') return false;
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
        /// <summary>
        /// Tries to parse a name string from the string
        /// </summary>
        /// <param name="newline"></param>
        /// <param name="i"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public static bool ParseString(string newline, ref int i, ref string name)
        {
            if (newline[i] != '"') return false;
            i++;
            while (i < newline.Length && newline[i] != '"')
            {
                name += newline[i];
                i++;
            }
            if (i >= newline.Length) return false;
            i++;
            if (i >= newline.Length) return false;
            return true;
        }
    }
}