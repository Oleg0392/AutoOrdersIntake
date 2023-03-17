using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.IO;
using System.Configuration;
using System.Diagnostics;
using System.Data.SqlClient;
//using Microsoft.Data;
using System.Data;
using ICSharpCode.SharpZipLib;
using System.Data.OleDb;

namespace AutoOrdersIntake
{
    class DBF
    {
        public static void IntakeDBFNew()
        {
            string _path = DispOrders.GetValueOption("DBF.ЗАКАЗ");  // @"\\edi-mgn\ZAKAZ\EXPIMP\Loading\DBF\tempf\";
            string _path_arch = DispOrders.GetValueOption("DBF.Архив");
            string[] files = Directory.GetFiles(_path, "*.dbf");
            string connString = Settings.Default.ConnStringISPRO;
            System.Globalization.NumberFormatInfo nfi = new System.Globalization.CultureInfo("en-US", false).NumberFormat;

            Dictionary<int, string> FieldIndex = new Dictionary<int, string>();
            FieldIndex.Add(0, "NUMZAK");
            FieldIndex.Add(1, "GPLCODE");
            FieldIndex.Add(2, "ART");
            FieldIndex.Add(3, "ARTNAME");
            FieldIndex.Add(4, "EI");
            FieldIndex.Add(5, "QT");
            FieldIndex.Add(6, "DATEZAK");
            FieldIndex.Add(7, "DATEOTG");

            foreach (string parsfile in files)
            {
                DispOrders.ClearTmpDbf();//очистка таблицы 
                Boolean error = false;
                FileInfo file = new FileInfo(Path.GetFullPath(parsfile));
                SaveToTable(file, "U_ChTMPDBF", connString, FieldIndex);//перенос данных из dbf в таблицу
                DispOrders.RenumberOrdersTmpDbf();
                //перенос из TMPDBF в TMPZKG
                string[] ListNumZak = DispOrders.GetNumZakFromDbf();

                foreach (string NZ in ListNumZak)
                {
                    object[,] CurrentItems = DispOrders.GetItemsFromTMPDBF(NZ);//товарные позиции в заказе
                    string[] buyer = Verifiacation.GetBuyerOptimum(Convert.ToString(CurrentItems[0, 0]));//плательщик 0 - ptn_cd  1 - ptn_NmSh 2- Filia_Adr
                    string[] deliv = Verifiacation.Verification_Tander_Buyer(Convert.ToString(CurrentItems[0, 0]));//шрузополучатель 0 - ptn_cd  1 - ptn_NmSh 2- Filia_Adr
                    //CurrentItems s переносим в темпTRSD
                    DispOrders.ClearTmpZkg();//очищаем временную таблицу с заказом от предыдущей точки 
                    for (int j = 0; j < CurrentItems.GetLength(0); j++)
                    {
                        object[] InfoItem = Verifiacation.GetDataOrderFromArt(Convert.ToString(CurrentItems[j, 1]));
                        //получить прейскурант грузополучателя
                        object[] PL = Verifiacation.GetPriceList(Convert.ToString(deliv[0]), Convert.ToInt32(InfoItem[5]));
                        if (Convert.ToInt32(PL[0]) == 0)
                            PL = Verifiacation.GetPriceList(Convert.ToString(buyer[0]), Convert.ToInt32(InfoItem[5]));
                        string quantity = (Convert.ToString(CurrentItems[j, 4])).Replace(",", ".");
                        string dd = (Convert.ToString(CurrentItems[j, 6]));
                        string dz = (Convert.ToString(CurrentItems[j, 5]));
                        string rcddog = (Convert.ToString(CurrentItems[j, 7] as object));
                        if (InfoItem[0] == null || InfoItem[1] == null || InfoItem[2] == null || InfoItem[3] == null || InfoItem[4] == null)
                        {
                            error = true;
                            DispOrders.WriteOrderLog("DBF", buyer[0] + " - " + buyer[1], "  ", Path.GetFileName(parsfile), "  ", 1, "не смог найти позицию по артикулу:" + Convert.ToString(CurrentItems[j, 2]) + " - " + Convert.ToString(CurrentItems[j, 1]), DateTime.Today, DateTime.Now, 0);
                            break;
                        }
                        if (Convert.ToDateTime(dd) < DateTime.Now) //если дата отгрузки меньше текущего дня, тогда прекратить разбор файла
                        {
                            error = true;
                            DispOrders.WriteOrderLog("DBF", buyer[0] + " - " + buyer[1], "  ", Path.GetFileName(parsfile), "  ", 1, "неверная дата отгрузки в файле", DateTime.Today, DateTime.Now, 0);
                            break;
                        }
                        if (rcddog == "" || rcddog == "0") //если договора нет, позицию не прогружать, записать в сообщения
                        {
                            error = true;
                            DispOrders.WriteOrderLog("DBF", buyer[0] + " - " + buyer[1], "  ", Path.GetFileName(parsfile), "  ", 1, "не проставлен номер договора в U_MGDOGNOMGPL", DateTime.Today, DateTime.Now, 0);
                            break;
                        }
                        if (decimal.Parse(quantity,nfi) == 0) //нулевые позиции не загружаем
                        {
                            DispOrders.WriteOrderLog("DBF", buyer[0] + " - " + buyer[1], deliv[0] + " - " + deliv[1], file.Name, " ", 16, "Нулевое количество в позиции "+ InfoItem[2] + "! Данная позиция пропущена.", DateTime.Today, DateTime.Now, 0);
                        }
                        else if(!error)
                        {

                            DispOrders.RecordToTmpZkg(buyer[0], Convert.ToString(CurrentItems[j, 0]), dd, Convert.ToString(CurrentItems[j, 1]), Convert.ToString(InfoItem[4]), quantity, dz, "DBF", Convert.ToString(PL[0]), Convert.ToInt32(InfoItem[5]), "DBF", Convert.ToString(PL[1]), rcddog);
                            
                        }
                        
                    }

                    if (!error)
                    {
                        DispOrders.TMPtoPrdZkg(buyer, deliv, "DBF", "DBF", "DBF-" + NZ);
                        int i = 0;
                        Program.WriteLine(Convert.ToString(i));
                    }
                    else
                    {
                        break;
                    }    
                    
                }

                //перемещение файла
                string oldPath = Path.GetFullPath(parsfile);
                string newPath = _path_arch + (DateTime.Now).ToString("ddMMyyyy_HHmmss") + "_" + Path.GetFileName(parsfile);
                Directory.Move(oldPath, newPath);
            }
        }

        public static void IntakeDBF()
        {
            string _path = DispOrders.GetValueOption("DBF.ЗАКАЗ");
            string _path_arch = DispOrders.GetValueOption("DBF.Архив");
            string[] files = Directory.GetFiles(_path, "*.dbf");
            string connString = Settings.Default.ConnStringISPRO;
            System.Globalization.NumberFormatInfo nfi = new System.Globalization.CultureInfo("en-US", false).NumberFormat;

            Dictionary<int, string> FieldIndex = new Dictionary<int, string>();
            FieldIndex.Add(0, "NUMZAK");
            FieldIndex.Add(1, "GPLCODE");
            FieldIndex.Add(2, "ART");
            FieldIndex.Add(3, "ARTNAME");
            FieldIndex.Add(4, "EI");
            FieldIndex.Add(5, "QT");
            FieldIndex.Add(6, "DATEZAK");
            FieldIndex.Add(7, "DATEOTG");

            foreach (string parsfile in files)
            {
                DispOrders.ClearTmpDbf();//очистка таблицы 
                Boolean error = false;
                FileInfo file = new FileInfo(Path.GetFullPath(parsfile));
                SaveToTable(file, "U_ChTMPDBF", connString, FieldIndex);//перенос данных из dbf в таблицу
                //перенос из TMPDBF в TMPZKG
                string[] ListNumZak = DispOrders.GetNumZakFromDbf();

                foreach (string NZ in ListNumZak)
                {
                    object[,] CurrentItems = DispOrders.GetItemsFromTMPDBF(NZ);//товарные позиции в заказе
                    string[] buyer = Verifiacation.GetBuyerOptimum(Convert.ToString(CurrentItems[0, 0]));//плательщик 0 - ptn_cd  1 - ptn_NmSh 2- Filia_Adr
                    string[] deliv = Verifiacation.Verification_Tander_Buyer(Convert.ToString(CurrentItems[0, 0]));//шрузополучатель 0 - ptn_cd  1 - ptn_NmSh 2- Filia_Adr
                    //CurrentItems s переносим в темпTRSD
                    DispOrders.ClearTmpZkg();//очищаем временную таблицу с заказом от конкретной предыдущей точки 
                    for (int j = 0; j < CurrentItems.GetLength(0); j++)
                    {
                        // deliv[0], CurentItems[0, 6], CurrentItems[j, 1], CurrentItems[j, 4], CurrentItems[j, 3]
                        object[] InfoItem = Verifiacation.GetDataOrderFromArt(Convert.ToString(CurrentItems[j, 1]));
                        object[] PL = Verifiacation.GetPriceList(Convert.ToString(buyer[0]), Convert.ToInt32(InfoItem[5]));
                        string quantity = (Convert.ToString(CurrentItems[j, 4])).Replace(",", ".");
                        string dd = (Convert.ToString(CurrentItems[j, 6]));
                        string dz = (Convert.ToString(CurrentItems[j, 5]));
                        if (InfoItem[0] == null || InfoItem[1] == null || InfoItem[2] == null || InfoItem[3] == null || InfoItem[4] == null)
                        {
                            error = true;
                            DispOrders.WriteOrderLog("DBF", buyer[0] + " - " + buyer[1], "  ", Path.GetFileName(parsfile), "  ", 1, "не смог найти позицию по артикулу:" + Convert.ToString(CurrentItems[j, 2]) + " - " + Convert.ToString(CurrentItems[j, 1]), DateTime.Today, DateTime.Now, 0);
                            break;
                        }
                        if (Convert.ToDateTime(dd) < DateTime.Now) //если дата отгрузки меньше текущего дня, тогда прекратить разбор файла
                        {
                            error = true;
                            DispOrders.WriteOrderLog("DBF", buyer[0] + " - " + buyer[1], "  ", Path.GetFileName(parsfile), "  ", 1, "неверная дата отгрузки в файле", DateTime.Today, DateTime.Now, 0);
                            break;
                        }
                        if (decimal.Parse(quantity, nfi) == 0) //нулевые позиции не загружаем
                        {
                            DispOrders.WriteOrderLog("DBF", buyer[0] + " - " + buyer[1], deliv[0] + " - " + deliv[1], file.Name, " ", 16, "Нулевое количество в позиции " + InfoItem[2] + "! Данная позиция пропущена.", DateTime.Today, DateTime.Now, 0);
                        }
                        else if (!error)
                        {
                            DispOrders.RecordToTmpZkg(buyer[0], Convert.ToString(CurrentItems[j, 0]), dd, Convert.ToString(CurrentItems[j, 1]), Convert.ToString(InfoItem[4]), quantity, dz, "DBF", Convert.ToString(PL[0]), Convert.ToInt32(InfoItem[5]), "DBF", Convert.ToString(PL[1]));
                        }

                    }

                    if (!error)
                    {
                        DispOrders.TMPtoPrdZkg(buyer, deliv, "DBF", "DBF", "DBF-" + NZ);
                        int i = 0;
                        Program.WriteLine(Convert.ToString(i));
                    }
                    else
                    {
                        break;
                    }

                }


                //перемещение файла
                string oldPath = Path.GetFullPath(parsfile);
                string newPath = _path_arch + (DateTime.Now).ToString("ddMMyyyy_HHmmss") + "_" + Path.GetFileName(parsfile);
                Directory.Move(oldPath, newPath);


            }
        }

        public static void SaveToTable(FileInfo dir, string TableName, string connestionString, Dictionary<int, string> FieldIndex)
        {
            using (var loader = new SqlBulkCopy(connestionString, SqlBulkCopyOptions.Default))
            {
                loader.DestinationTableName = TableName;
                loader.BulkCopyTimeout = 9999;
                BDFBulkReader DBF_file = new BDFBulkReader(dir.FullName, FieldIndex);
                loader.WriteToServer(DBF_file);

                DBF_file.Dispose();
            }
            
        }

        public static bool Limited(string GplCd, string DtOtg, string NomArt, string EI, string qt)
        {
            bool result = true;
            SqlConnection connection = new SqlConnection(Settings.Default.ConnStringISPRO);
            SqlCommand command = connection.CreateCommand();
            SqlDataReader dataReader;

            string queryString = "SELECT LimDc_Rcd, LimDc_RcdPtn, LimDc_DtEnd, LimDc_DtBeg \n";           
            queryString += ", (SELECT MAX(LimTrnHst_Dt) FROM dbo.U_ChLimTrnHst WHERE LimTrnHst_RcdDc = LimDc_Rcd) AS LimDcMaxDtClc \n";
            queryString += ", (SELECT Opt_Val FROM dbo.U_CHOPTIONS WHERE Opt_RcdMdl = 506 and Opt_Rcd = 41) AS UseReserve ";
            queryString += "FROM dbo.U_ChLimDc JOIN dbo.PTNRK ON Ptn_Rcd = LimDc_RcdPtn WHERE Ptn_Cd = '" + GplCd + "' AND ('" + DtOtg + "' BETWEEN LimDc_DtBeg AND LimDc_DtEnd)\n";
            queryString += "AND LimDc_Stt > 1";
            command.CommandText = queryString;
            
            object[] LimInfo; object[] Limits;
            connection.Open();
            dataReader = command.ExecuteReader();
            if (dataReader.Read())
            {
                LimInfo = new object[dataReader.VisibleFieldCount];
                dataReader.GetValues(LimInfo);

                dataReader.Close();
                command.CommandText = "SELECT Ptn_Rcd FROM PTNRK WHERE Ptn_Cd = '" + GplCd + "'";
                dataReader = command.ExecuteReader();
                string PtnRcd = dataReader.GetString(0);

                if (LimInfo[5].ToString().Equals("1"))
                {
                    queryString = "SELECT LimSpc_QtOsn - LimSpc_QtOtg + LimSpc_QtRet QtLimit, SUM(ISNULL(TrdS_QtOsn,0)) QtReserve\n";
                    queryString += "FROM dbo.U_ChLimSpc \n";
                    queryString += "JOIN dbo.SKLN ON SklN_Rcd = LimSpc_RcdNom AND SklN_Cd = '" + NomArt + "'\n";
                    queryString += "LEFT JOIN dbo.PRDZKG WITH (NOLOCK) ON PrdZkg_RcvrID = " + PtnRcd + " AND PrdZkg_DtOtg > '20221223' AND PrdZkg_Dt <= '20221231' AND PrdZkg_Rcd <> 1\n";
                    queryString += "LEFT JOIN dbo.TRDS WITH (NOLOCK) ON TrdS_RcdHdr = PrdZkg_Rcd AND TrdS_Mov = 0 and TrdS_TypHdr = 17 AND LimSpc_RcdNom = TrdS_RcdNom \n";
                    queryString += "WHERE LimSpc_RcdDc = " + LimInfo[0].ToString() + " \n";
                    queryString += "GROUP BY LimSpc_QtOsn,LimSpc_QtOtg,LimSpc_QtRet";
                }
                else
                {
                    queryString = "SELECT LimSpc_QtOsn - LimSpc_QtOtg + LimSpc_QtRet QtLimit \n";
                    queryString += "FROM dbo.U_ChLimSpc \n";
                    queryString += "JOIN dbo.SKLN ON SklN_Rcd = LimSpc_RcdNom AND SklN_Cd = '" + NomArt + "'\n";
                    queryString += "WHERE LimSpc_RcdDc = " + LimInfo[0].ToString() + "\n";
                    queryString += "GROUP BY LimSpc_QtOsn,LimSpc_QtOtg,LimSpc_QtRet\n";
                }

                dataReader.Close();
                command.CommandText = queryString;
                dataReader = command.ExecuteReader();
                if (dataReader.Read())
                {
                    Limits = new object[dataReader.VisibleFieldCount];
                    dataReader.GetValues(Limits);

                    decimal limit = Convert.ToDecimal(Limits[0]);
                    decimal quantity = Convert.ToDecimal(qt);
                    
                    if (!EI.Equals("кг"))
                    {
                        dataReader.Close();
                        queryString = "SELECT NmEi_QtOsn * " + quantity + " FROM SKLNOMEI\n";
                        queryString += "JOIN EI ON EI_Rcd = NmEi_Cd\n";
                        queryString += "WHERE Ei_ShNm = '" + EI + "' AND NmEi_RcdNom = (SELECT SklN_Rcd FROM SKLN WHERE SklN_Cd = '" + NomArt + "') ";
                        command.CommandText = queryString;
                        dataReader = command.ExecuteReader();
                        if (dataReader.Read()) quantity = Convert.ToDecimal(dataReader.GetValue(0));
                    }
                    
                    if (LimInfo[5].ToString().Equals("1"))
                    {
                        decimal qtReserv = Convert.ToDecimal(Limits[1]);
                        limit = limit - qtReserv;
                    }

                    if (quantity > limit)
                    {
                        Program.WriteLine("!!!Превышение лимита");
                        result = false;
                    }

                    dataReader.Close();
                }

            }
            connection.Close();

            return result;
        }

        public class BDFBulkReader : IDataReader //интерфейс для чтения dbf
        {
            public object GetValue(int i) { return R[FieldIndex[i]]; }

            System.IO.FileStream FS;
            byte[] buffer;
            int _FieldCount;
            int FieldsLength;
            System.Globalization.DateTimeFormatInfo dfi = new System.Globalization.CultureInfo("en-US", false).DateTimeFormat;
            System.Globalization.NumberFormatInfo nfi = new System.Globalization.CultureInfo("en-US", false).NumberFormat;
            System.Globalization.NumberStyles number_styles = System.Globalization.NumberStyles.AllowCurrencySymbol | System.Globalization.NumberStyles.AllowDecimalPoint;
            System.Globalization.CultureInfo cultureInfo = new System.Globalization.CultureInfo("en-US");
            string[] FieldName;
            string[] FieldType;
            byte[] FieldSize;
            byte[] FieldDigs;
            int RowsCount;
            int ReadedRow = 0;

            Dictionary<string, object> R = new Dictionary<string, object>();
            Dictionary<int, string> FieldIndex = new Dictionary<int, string>();

            public BDFBulkReader(string FileName, Dictionary<int, string> FieldIndex)
            {
                FS = new System.IO.FileStream(FileName, System.IO.FileMode.Open);
                buffer = new byte[4];
                FS.Position = 4; FS.Read(buffer, 0, buffer.Length);
                RowsCount = buffer[0] + (buffer[1] * 0x100) + (buffer[2] * 0x10000) + (buffer[3] * 0x1000000);
                buffer = new byte[2];
                FS.Position = 8; FS.Read(buffer, 0, buffer.Length);
                _FieldCount = (((buffer[0] + (buffer[1] * 0x100)) - 1) / 32) - 1;
                FieldName = new string[_FieldCount];
                FieldType = new string[_FieldCount];
                FieldSize = new byte[_FieldCount];
                FieldDigs = new byte[_FieldCount];
                buffer = new byte[32 * _FieldCount];
                FS.Position = 32; FS.Read(buffer, 0, buffer.Length);
                FieldsLength = 0;
                for (int i = 0; i < _FieldCount; i++)
                {
                    FieldName[i] = System.Text.Encoding.Default.GetString(buffer, i * 32, 10).TrimEnd(new char[] { (char)0x00 });
                    FieldType[i] = "" + (char)buffer[i * 32 + 11];
                    FieldSize[i] = buffer[i * 32 + 16];
                    FieldDigs[i] = buffer[i * 32 + 17];
                    FieldsLength = FieldsLength + FieldSize[i];
                }
                FS.ReadByte();
                this.FieldIndex = FieldIndex;
            }

            public bool Read()
            {
                if (ReadedRow >= RowsCount) return false;

                R.Clear();
                buffer = new byte[FieldsLength];
                FS.ReadByte();
                FS.Read(buffer, 0, buffer.Length);
                int Index = 0;
                for (int i = 0; i < FieldCount; i++)
                {
                    string l = System.Text.Encoding.GetEncoding(1251).GetString(buffer, Index, FieldSize[i]).TrimEnd(new char[] { (char)0x00 }).TrimEnd(new char[] { (char)0x20 });
                    Index = Index + FieldSize[i];
                    object Tr;
                    int int_;
                    decimal decimal_;
                    if (l.Trim() != "")
                    {
                        switch (FieldType[i])
                        {
                            case "L": Tr = l == "T" ? true : false; break;
                            case "D": Tr = DateTime.ParseExact(l, "yyyyMMdd", dfi); break;
                            case "N":
                                {
                                    if (FieldDigs[i] == 0)
                                    {
                                        if (int.TryParse(l, out int_))
                                            Tr = int.Parse(l, nfi);
                                        else
                                            Tr = int.Parse("0", nfi);
                                    }
                                    else
                                    {
                                        if (decimal.TryParse(l.Trim(), number_styles, cultureInfo, out decimal_))
                                            Tr = decimal.Parse(l, nfi);
                                        else
                                            Tr = decimal.Parse("0", nfi);
                                    }

                                    break;
                                }
                            case "F": Tr = double.Parse(l, nfi); break;
                            default: Tr = l; break;
                        }

                    }
                    else
                    {
                        Tr = DBNull.Value;
                    }
                    R.Add(FieldName[i], Tr);
                }
                ReadedRow++;
                return true;
            }

            public int FieldCount { get { return _FieldCount; } }

            public void Dispose() { FS.Close(); }
            public int Depth { get { return -1; } }
            public bool IsClosed { get { return false; } }
            public Object this[int i] { get { return new object(); } }
            public Object this[string name] { get { return new object(); } }
            public int RecordsAffected { get { return -1; } }

            public void Close() { }
            public bool NextResult() { return true; }
            public bool IsDBNull(int i) { return false; }
            public string GetString(int i) { return ""; }
            public DataTable GetSchemaTable() { return null; }
            public int GetOrdinal(string name) { return -1; }
            public string GetName(int i) { return ""; }
            public long GetInt64(int i) { return -1; }
            public int GetInt32(int i) { return -1; }
            public short GetInt16(int i) { return -1; }
            public Guid GetGuid(int i) { return new Guid(); }
            public float GetFloat(int i) { return -1; }
            public Type GetFieldType(int i) { return typeof(string); }
            public double GetDouble(int i) { return -1; }
            public decimal GetDecimal(int i) { return -1; }
            public DateTime GetDateTime(int i) { return new DateTime(); }
            public string GetDataTypeName(int i) { return ""; }
            public IDataReader GetData(int i) { return this; }
            public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) { return -1; }
            public char GetChar(int i) { return ' '; }
            public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) { return -1; }
            public byte GetByte(int i) { return 0x00; }
            public bool GetBoolean(int i) { return false; }
            public int GetValues(Object[] values) { return -1; }
        }
    }
}
