using MySql.Data.MySqlClient;
using System;
using System.Data;
using System.IO;
using System.Linq;

namespace mysql
{
    class Program
    {
        static string _outfile = null;

        static void LogLine(string format, params object[] args)
        {
            string str = "";
            if (args == null || args.Length == 0)
                str = format;
            else
                str = string.Format(format, args);
            str += Environment.NewLine;
            if (_outfile != null)
                File.AppendAllText(_outfile, str);
            else
                Console.Write(str);
        }

        static void Query(MySqlConnection conn, string query)
        {
            MySqlDataReader reader = null;

            try
            {
                MySqlCommand cmd = new MySqlCommand(query, conn);
                reader = cmd.ExecuteReader();

                DataTable dt = new DataTable();
                dt.Load(reader);

                if (dt.Rows.Count > 0)
                {
                    string[] columnNames = dt.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToArray();
                    LogLine(string.Join(",", columnNames));

                    foreach (DataRow dr in dt.Rows)
                    {
                        string[] values = new string[dt.Columns.Count];
                        for (int i = 0; i < dt.Columns.Count; ++i)
                        {
                            values[i] = string.Format("{0}", dr[i]).Trim();
                        }

                        LogLine(string.Join(",", values));
                    }
                }
                else
                {
                    Console.WriteLine("Empty.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                if (reader != null)
                    reader.Close();
            }
        }

        static void Main(string[] args)
        {
            try
            {
                if (args.Length == 5 || args.Length == 6)
                {
                    string server = args[0];
                    string user = args[1];
                    string pass = args[2];
                    string port = args[3];

                    if (args.Length == 6)
                        _outfile = args[5];

                    string connStr = String.Format("server={0};user id={1};password={2};port={3};pooling=false;charset=utf8", server, user, pass, port);

                    var conn = new MySqlConnection(connStr);
                    conn.Open();

                    Query(conn, args[4]);

                    conn.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}