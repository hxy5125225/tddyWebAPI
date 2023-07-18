using DBConnector.PG;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
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

        /// <summary>
        /// 查询Sql
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="tableName">表名</param>
        /// <param name="values">参数</param>
        /// <returns></returns>
        [HttpPost]
        [Route("GetData")]
        public string GetData([FromForm] string sql, [FromForm] List<object> values)
        {
            NpgsqlConnection cn = PGClass.GetPGConn();
            DataTable dt = PGClass.QueryNpgDatabase(cn, sql, values);
            if (dt ==null || dt.Rows.Count ==0) return "";
            return JsonConvert.SerializeObject(dt);
        }

        /// <summary>
        /// 执行插入、删除、更新数据的sql
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="tableName">表名</param>
        /// <param name="values">参数</param>
        /// <returns></returns>
        [HttpPost]
        [Route("MergeData")]
        public int MergeData([FromForm] string sql, [FromForm] List<object> values)
        {
            return PGClass.ExecuteNoneQuery(PGClass.GetPGConn(), sql, values);
        }

    }
}
