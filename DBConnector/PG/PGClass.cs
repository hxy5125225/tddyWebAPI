using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;

namespace DBConnector.PG
{
    public class PGClass
    {
        public static string PGConString = "Server=47.95.255.108;Port=5432;UserId=postgres;Password=tddy123456;Database=wuxi;Encoding=UTF8;CommandTimeout=300;Pooling=true;MaxPoolSize=100;";

        public static NpgsqlConnection GetPGConn()
        {
            NpgsqlConnection conn = new Npgsql.NpgsqlConnection(PGConString);
            conn.Open();
            conn.TypeMapper.UseNetTopologySuite(handleOrdinates: GeoAPI.Geometries.Ordinates.XYZ);
            return conn;
        }
        /// <summary>
        /// 查询数据库
        /// </summary>
        /// <param name="sql">sql脚本</param>
        /// <returns>DataTable</returns>
        public static DataTable QueryNpgDatabase(string sql)
        {
            DataTable dt = new DataTable();
            try
            {
                using (NpgsqlConnection cn = new NpgsqlConnection(PGConString))
                {
                    cn.Open();
                    using (NpgsqlCommand cmd = new NpgsqlCommand(sql, cn))
                    {
                        using (NpgsqlDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader == null) return dt;
                            dt.Locale = System.Globalization.CultureInfo.InvariantCulture;
                            int fieldCount = reader.FieldCount;
                            // 在表中创建字段
                            for (int counter = 0; counter < fieldCount; counter++)
                            {
                                dt.Columns.Add(reader.GetName(counter), reader.GetFieldType(counter));
                            }
                            dt.BeginLoadData();
                            object[] values = new object[fieldCount];
                            reader.GetEnumerator();
                            while (reader.Read())
                            {
                                // 添加行
                                reader.GetValues(values);
                                dt.LoadDataRow(values, true);
                            }
                            // 完成转换并返回
                            dt.EndLoadData();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.StackTrace);
            }
            return dt;
        }
        public static NpgsqlParameter[] InitNpgsqlParameter(List<object> objects)
        {
            List<NpgsqlParameter> parameters = new List<NpgsqlParameter>();
            for (int i = 0; i < objects.Count; i++)
            {
                parameters.Add(new NpgsqlParameter("param" + i, objects[i]));
            }
            return parameters.ToArray();
        }


        /// <summary>
        /// 查询数据库
        /// </summary>
        /// <param name="cn">NpgsqlConnection连接</param>
        /// <param name="sql">sql脚本</param>
        /// <returns>DataTable</returns>
        public static DataTable QueryNpgDatabase(NpgsqlConnection cn, string sql, List<object> objects)
        {
            DataTable dt = new DataTable();
            try
            {
                if (cn.State != ConnectionState.Open)
                    cn.Open();
                using (NpgsqlCommand cmd = new NpgsqlCommand(sql, cn))
                {
                    if (objects.Count > 0)
                    {
                        cmd.Parameters.AddRange(InitNpgsqlParameter(objects));
                    }
                    using (NpgsqlDataAdapter da = new NpgsqlDataAdapter(cmd))
                    {
                        DataSet ds = new DataSet();
                        da.Fill(ds);
                        dt = ds.Tables[0];
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.StackTrace + Environment.NewLine + ex.Message);
            }
            finally
            {
                cn.Close();
            }
            return dt;
        }

        public static DataTable CreateTableBySchemaTable(DataTable pSchemaTable)
        {
            DataTable dtReturn = new DataTable();
            DataColumn dc = null;
            DataRow dr = null;
            for (int i = 0; i < pSchemaTable.Rows.Count; i++)
            {
                dr = pSchemaTable.Rows[i];
                dc = new DataColumn(dr["ColumnName"].ToString(), dr["DataType"] as Type);
                dtReturn.Columns.Add(dc);
            }
            dr = null;
            dc = null;
            return dtReturn;
        }

        public static void ExecuteNoneQuery(string sql)
        {
            using (NpgsqlConnection cn = new NpgsqlConnection(PGConString))
            {
                cn.Open();
                using (NpgsqlCommand cmd = new NpgsqlCommand(sql, cn))
                {
                    cmd.CommandType = CommandType.Text;
                    cmd.ExecuteNonQuery();
                }
            }
        }

        public static int ExecuteNoneQuery(NpgsqlConnection cn, string sql, List<object> objects)
        {
            int nums = 0;
            if (string.IsNullOrEmpty(sql)) return nums;
            try
            {
                if (cn.State != ConnectionState.Open)
                    cn.Open();
                using (NpgsqlCommand cmd = new NpgsqlCommand(sql, cn))
                {
                    if (objects.Count > 0) cmd.Parameters.AddRange(InitNpgsqlParameter(objects));
                    cmd.CommandType = CommandType.Text;
                    nums = cmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                throw new Exception(ex.StackTrace + Environment.NewLine + ex.Message);
            }
            finally
            {
                cn.Close();
            }
            return nums;
        }
        
        /// <summary>
        /// 创建临时表
        /// </summary>
        public static void CreateTempTable(NpgsqlConnection cn, string sql)
        {
            using (NpgsqlCommand cmd = new NpgsqlCommand(sql, cn))
            {
                cmd.CommandType = CommandType.Text;
                cmd.ExecuteNonQuery();
            }
        }

        public static List<string> GetColumnNames(NpgsqlConnection cn, string tabName)
        {
            DataTable dtCol = QueryNpgDatabase(cn, "select * from  " + tabName + "  limit 1", new List<object>());
            List<string> list = new List<string>();
            foreach (DataColumn item in dtCol.Columns)
            {
                list.Add(item.ColumnName.ToLower());
            }
            return list;
        }

        public static void ExecuteProcedure(string sql, NpgsqlParameter[] parameters, NpgsqlConnection conn)
        {
            try
            {
                using (NpgsqlCommand cmd = conn.CreateCommand())
                {
                    cmd.CommandText = sql;
                    cmd.CommandType = CommandType.StoredProcedure;
                    cmd.Parameters.AddRange(parameters);
                    cmd.ExecuteNonQuery();
                }
            }
            catch (Exception){}
        }

    }
}
