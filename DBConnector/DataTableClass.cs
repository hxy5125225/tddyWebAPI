using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace DBConnector
{
    public class DataTableClass
    {
        /// <summary>
        /// 合并表格
        /// </summary>
        /// <param name="dt1"></param>
        /// <param name="dt2"></param>
        /// <returns></returns>
        public static System.Data.DataTable JoinTable(DataTable dt1, System.Data.DataTable dt2, string field1, string field2)
        {
            System.Data.DataTable dt = new DataTable();
            GetColumns(dt1.Columns, ref dt);
            GetColumns(dt2.Columns, ref dt);
            if (dt1.Rows.Count == 0) return dt;
            try
            {
                var query = from tab1 in dt1.AsEnumerable()
                            join rowRight in dt2.AsEnumerable() on tab1[field1] equals rowRight[field2] into gj
                            from tab2 in gj.DefaultIfEmpty()
                            select tab1.ItemArray.Concat((tab2 == null) ? (dt2.NewRow().ItemArray) : tab2.ItemArray).ToArray();
                foreach (object[] values in query)
                {
                    dt.Rows.Add(values);
                }
                dt.Columns.Remove(field2);
            }
            catch (Exception ex)
            {
                throw new Exception(ex.StackTrace);
            }
            return dt;
        }

        public static System.Data.DataTable JoinTable(DataTable dt1, System.Data.DataTable dt2, List<string> field1, List<string> field2)
        {
            System.Data.DataTable dt = new DataTable();
            GetColumns(dt1.Columns, ref dt);
            GetColumns(dt2.Columns, ref dt);
            try
            {
                var query = from tab1 in dt1.AsEnumerable()
                            join rowRight in dt2.AsEnumerable() 
                            on new { A = tab1.Field<long>(field1[0]), B = tab1.Field<long>(field1[1]) } equals new { A = rowRight.Field<long>(field2[0]), B = rowRight.Field<long>(field2[1]) }
                            into gj
                            from tab2 in gj.DefaultIfEmpty()
                            select tab1.ItemArray.Concat((tab2 == null) ? (dt2.NewRow().ItemArray) : tab2.ItemArray).ToArray();
                foreach (object[] values in query)
                {
                    dt.Rows.Add(values);
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.StackTrace);
            }
            return dt;
        }

        public static dynamic getObj1(DataRow dr, Dictionary<string, Type> dic)
        {
            List<string> list = new List<string>() { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z" };
            Dictionary<object, object> dics = new Dictionary<object, object>();
            int count = 0;
            foreach (var item in dic)
            {
                dics.Add(list[count], dr[item.Key]);
                count++;
            }
            string json = Newtonsoft.Json.JsonConvert.SerializeObject(dics);
            dynamic dy = Newtonsoft.Json.JsonConvert.DeserializeObject(json);
            return dy;
        }

        public static void GetColumns(DataColumnCollection columns, ref DataTable dtAll)
        {
            foreach (System.Data.DataColumn column in columns)
            {
                string columnName = column.ColumnName;
                if (dtAll.Columns.Contains(columnName))
                {
                    columnName = columnName + "_1";
                }
                dtAll.Columns.Add(columnName, column.DataType);
            }
        }

        /// <summary>
        /// 去重（按照某一个字段）
        /// </summary>
        /// <param name="dt">原始表</param>
        /// <param name="fieldName">字段名称</param>
        /// <returns></returns>
        public static DataTable Distinct1(DataTable dt, string fieldName)
        {
            DataTable dtResult = dt.Clone();
            List<object> listOid = new List<object>();
            foreach (DataRow item in dt.Rows)
            {
                if (listOid.Contains(item[fieldName])) continue;
                listOid.Add(item[fieldName]);
                dtResult.Rows.Add(item.ItemArray);
            }
            return dtResult;
        }
        public static void Dispose(DataTable dt)
        {
            dt.Clear();
            dt.Dispose();
            dt = null;
        }

        internal static DataTable Distinct(DataTable dt, string field)
        {
            DataTable result = dt.Clone();
            try
            {
                var groups = dt.AsEnumerable().GroupBy(row => row[field].ToString());
                foreach (var group in groups)
                {
                    DataRow newRow = result.NewRow();
                    newRow.ItemArray = group.ElementAt(0).ItemArray;
                    result.Rows.Add(newRow);
                }
            }
            catch
            {
                result = Distinct1(dt, field);
            }
            return result;
        }
        internal static DataTable Distinct(DataTable dt, string field, string field2)
        {
            DataTable result = dt.Clone();
            try
            {
                var groups = dt.AsEnumerable().GroupBy(row => row[field].ToString() + row[field2].ToString());
                foreach (var group in groups)
                {
                    DataRow newRow = result.NewRow();
                    newRow.ItemArray = group.ElementAt(0).ItemArray;
                    result.Rows.Add(newRow);

                }
            }
            catch
            {
                
            }
            return result;
        }
        public static DataTable LINQToDataTable<T>(IEnumerable<T> varlist, DataTable dt)
        {
            DataTable dtReturn = new DataTable();
            PropertyInfo[] oProps = null;
            if (varlist == null)
            {
                return dt;
            }
            foreach (T rec in varlist)
            {
                if (oProps == null)
                {
                    oProps = ((Type)rec.GetType()).GetProperties();
                    foreach (PropertyInfo pi in oProps)
                    {
                        Type colType = pi.PropertyType;
                        if ((colType.IsGenericType) && (colType.GetGenericTypeDefinition() == typeof(Nullable<>)))
                        {
                            colType = colType.GetGenericArguments()[0];
                        }
                        dtReturn.Columns.Add(new DataColumn(pi.Name, colType));
                    }
                }
                DataRow dr = dtReturn.NewRow();
                foreach (PropertyInfo pi in oProps)
                {
                    dr[pi.Name] = pi.GetValue(rec, null) == null ? DBNull.Value : pi.GetValue(rec, null);
                }
                dtReturn.Rows.Add(dr);
            }
            if (dtReturn.Rows.Count == 0)
            {
                return dt;
            }
            return dtReturn;
        }
    }
}
