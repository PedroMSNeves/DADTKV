namespace Parse_Lib
{
    public class Parse
    {
        public static int ParseInt()
        {
            int val;
            string newline = Console.ReadLine();
            if( newline == null ) { return -1; }
            if (int.TryParse(newline, out val))
            {
                return val;
            }
            else
            {
                return -1;
            }
        }
        public static string ParseString()
        {
            
            return "";
        }

    }
}