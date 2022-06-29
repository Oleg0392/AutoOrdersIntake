using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using System.Xml.Linq;


namespace AutoOrdersIntake
{
    class StartProgram
    {
        internal static int GetTanderSettings()
        {
            int result = 0;
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "select top 1 Enabled as e from U_CHXLSMetod where NameMetod = 'Tander'";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            while (dr.Read())
            {
                result = Convert.ToInt32(dr.GetValue(0));

            }
            conn.Close();
            return result;
        }

        internal static int GetProviantSettings()
        {
            int result = 0;
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "select Enabled as e from U_CHXLSMetod where NameMetod = 'Proviant'";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            while (dr.Read())
            {
                result = Convert.ToInt32(dr.GetValue(0));
            }
            conn.Close();
            return result;
        }

    }
}
