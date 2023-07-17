using System;
using System.Collections.Generic;
using System.Data;
namespace DBConnector
{
    public class InterfaceClass
    {

        public static T DbNullValue<T>(object value)
        {
            if (value == null && value == DBNull.Value)
            {
                return default(T);
            }
            else if (value == null)
            {
                if (typeof(T).Name == "String")
                    return (T)Convert.ChangeType("", typeof(T));
            }
            return (T)Convert.ChangeType(value, typeof(T));
        }

        private string GetParamIndex(string parameters, int maxIndex)
        {
            string baseStr = "@param";
            if (maxIndex < 0) return baseStr + "0";
            for (int i = maxIndex; i >= 0; i--)
            {
                if (parameters.IndexOf(baseStr + i) > 0)
                {
                    return baseStr + (i + 1);
                }
            }
            return baseStr + "0";
        }
        
        /// <summary>
        /// 根据条件筛选并返回表
        /// </summary>
        /// <param name="dt">原始表</param>
        /// <param name="condition">条件</param>
        /// <returns></returns>
        private static DataTable GetTableByCondition(DataTable dt, string condition)
        {
            DataRow[] dr = dt.Select(condition);
            return dr.Length == 0 ? dt.Clone() : dr.CopyToDataTable();
        }
        private static DataTable GetTableByCondition(ref DataTable dt, string condition)
        {
            DataRow[] dr = dt.Select(condition);
            DataTable result = dt.Clone();
            if (dr.Length > 0)
            {
                result = dr.CopyToDataTable();
            }
            foreach (DataRow item in dr)
            {
                dt.Rows.Remove(item);
            }
            return result;
        }


    }
}
