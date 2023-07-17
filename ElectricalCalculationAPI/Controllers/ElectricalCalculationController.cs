using DBConnector;
using DBConnector.PG;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using System.Collections.Generic;
using System.Data;

namespace ElectricalCalculationAPI.Controllers
{
    [ApiController]
    [Route("tddy/api")]

    public class ElectricalCalculationController : ControllerBase
    {

        [HttpGet]
        [Route("GetData/{key}")]
        public string GetData(string key)
        {
            NpgsqlConnection cn = PGClass.GetPGConn();
            string sql = "SELECT org_id FROM newdata.t_substation where oid=@param0";
            DataTable dt = PGClass.QueryNpgDatabase(cn, sql, new List<object>() { key });
            if (dt != null || dt.Rows.Count > 0)
            {
                return dt.Rows[0].ItemArray[0].ToString();
            }
            return key;
        }

        [HttpPost]
        [Route("PostData")]
        public string CalcNMinusonePost([FromForm] string taskid, [FromForm] string json, [FromForm] string loadTime, [FromForm] int loadNew)
        {
            return taskid;
        }

        [HttpPost]
        [Route("getData")]
        public object GetData([FromBody] object para)
        {
            return para.ToString();
        }
        
    }
}
