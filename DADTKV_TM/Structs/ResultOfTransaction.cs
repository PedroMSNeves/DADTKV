using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DADTKV_TM.Structs
{
    /// <summary>
    /// Class that stores the result of a transaction, including it's error code
    /// </summary>
    public readonly struct ResultOfTransaction
    {
        public ResultOfTransaction(List<DadIntProto> result, int error_code)
        {
            Result = result;
            Error_code = error_code;
        }
        public List<DadIntProto> Result { get; }
        public int Error_code { get; }

    }
}
