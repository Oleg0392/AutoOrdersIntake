using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Data.SqlClient;
//using Microsoft.Data;
using System.Data;
using System.IO;
using System.Collections;
using System.Xml;
using System.Threading;
// Класс для приема заказов, резервированием rcd документа и заказа.

namespace AutoOrdersIntake
{
    class DispOrders
    {
        static private volatile bool exceptionFlag;

        internal static void RecordToTmpZkg(string IsPlatCd, string IsDelivCd, string DocDateExp, string IsArtCd, string IsArtEI, string ArtQuantity, string DocDateDoc, string DocComment, string PriceList,int TypeSkln,string filename,string PriceList_Rcd, string RcdDog = "0", string OrderPrice = "0")
        {
            int Price_EI = DispOrders.GetEIPrice(Convert.ToInt32(PriceList), IsArtCd);
            int Current_EI = Convert.ToInt32(IsArtEI);

            if (String.IsNullOrWhiteSpace(Convert.ToString(Price_EI)) || Price_EI == 0)//товарной позиции нет в данном прайс листе
            {
                string NamePrc = Verifiacation.GetNamePrice(PriceList_Rcd);
                DispOrders.WriteOrderLog("ИС-ПРО", IsPlatCd, IsDelivCd, filename, "  ", 15, "В прайс-листе " + NamePrc + " нет товарной позиции: " + IsArtCd, DateTime.Today, DateTime.Now, TypeSkln);
                Program.WriteLine("В прайс-листе нет товарной позиции ");  
            }
            else
            {
                if (Price_EI == Current_EI)
                {
                    DispOrders.InsertTmpZkg(IsPlatCd, IsDelivCd, DocDateExp, IsArtCd, IsArtEI, ArtQuantity, DocDateDoc, DocComment, PriceList, TypeSkln, filename,PriceList_Rcd, RcdDog, OrderPrice);
                }
                else
                {
                    object[] quantity_EI = DispOrders.ConvertEI(IsArtCd, Current_EI, ArtQuantity, PriceList_Rcd, DocDateExp);
                    int q = Convert.ToInt32(quantity_EI[0]);
                    //Console.WriteLine(q);
                    //Console.Read();
                    if ((Convert.ToString(quantity_EI[1]) == "39") && (q < 1))//если ЕИ коробка и меньше 1
                    {
                        DispOrders.WriteOrderLog("ИС-ПРО", IsPlatCd, IsDelivCd, filename, DocComment, 16, "Заказано меньше одной коробки продукции! Данная позиция пропущена.", DateTime.Today, DateTime.Now, TypeSkln);
                    }
                    else
                    {
                        try //точка или запятая в системе? а ХЗ!!! в хмле идёт точка. у меня в системе запятая.
                        {
                            OrderPrice = Convert.ToString(Convert.ToDecimal(OrderPrice) * Convert.ToDecimal(ArtQuantity) / q);
                        }
                        catch
                        {
                            OrderPrice = Convert.ToString(Convert.ToDecimal(OrderPrice.Replace(".", ",")) * Convert.ToDecimal(ArtQuantity.Replace(".", ",")) / q);
                            OrderPrice = OrderPrice.Replace(",", "."); // Возвращаю опять точку, чтобы было как везде
                        }
                        DispOrders.InsertTmpZkg(IsPlatCd, IsDelivCd, DocDateExp, IsArtCd, Convert.ToString(quantity_EI[1]), Convert.ToString(quantity_EI[0]), DocDateDoc, DocComment, PriceList, TypeSkln, filename, PriceList_Rcd, RcdDog, OrderPrice);
                    }
                }
            }
        }

        internal static void InsertTmpZkg(string IsPlatCd, string IsDelivCd, string DocDateExp, string IsArtCd, string IsArtEI, string ArtQuantity, string DocDateDoc, string DocComment, string PriceList, int TypeSkln, string filename, string PriceList_Rcd, string RcdDog = "0", string OrderPrice = "0")
        {
            //запись в TMPZKG
            string connString = Settings.Default.ConnStringISPRO;

            string Insert = "insert into U_Chtmpzkg (IsPlatRcd,IsDelivRcd,DocDateExp,IsArtCd,IsArtEI,ArtQuantity,DocDateDoc,DocComment,PriceList,TypeSkln,PriceList_RCD,RcdDog,Price) "
                + " values ('" + IsPlatCd + "','" + IsDelivCd + "', '" + Convert.ToDateTime(DocDateExp).ToString("yyyyMMdd") + "', '" + IsArtCd + "', " + IsArtEI + ", '" + ArtQuantity.Replace(",",".") + "', '" + Convert.ToDateTime(DocDateDoc).ToString("yyyyMMdd") + "', RTRIM('" + DocComment + " ' + (case when '" + RcdDog + "' <>'0' then (select Dog_OutNum from DOG where Dog_Rcd = "+ RcdDog + ") else '' end)) , '" + PriceList + "'," + TypeSkln + "," + PriceList_Rcd + "," + RcdDog + "," + OrderPrice + ")";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(Insert, conn);
            try
            {
                SqlDataReader dr = command.ExecuteReader();
            }
            catch (IOException e)
            {
                Program.WriteLine("---------------");
                Program.WriteLine("Ошибка записи U_Chtmpzkg.Ошибка в файле:" + filename);
                WriteErrorLog("Ошибка записи U_Chtmpzkg.Ошибка в файле:" + filename);
                WriteErrorLog(e.Message);
                Program.WriteLine(Insert);
                Program.WriteLine("---------------");
            };
            conn.Close();
        }

        internal static object[] ConvertEI(string Art, int CurrentEI, string quantity, string Price, string DateDeliv)//возвращает 1-количество 2-id ЕИ
        {
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "DECLARE @ZakNom VARCHAR(13) = (select top 1 skln_rcd from SKLN where SklN_Cd =  '" + Art + "')  "
                   + "DECLARE @ZakQt DECIMAL(12,4) = " + quantity + "  "
                   + "DECLARE @ZakPrc INT = "+Price+"  "
                   + "DECLARE @ZakDelvrDt DATE = '"+Convert.ToDateTime(DateDeliv).ToString("yyyyMMdd")+"'  "
                   + "  "
                   + "DECLARE @ResNm VARCHAR(255)  "
                   + "DECLARE @ZakEiOsn DECIMAL(12,4)  "
                   + "  "
                   + "SELECT TOP 1 @ResNm = SklN_NmAlt  "
                   + "     , @ZakEiOsn = ISNULL(SKLNOMEI.NmEi_QtOsn,1)  "
                   + "FROM SKLN  "
                   + "     LEFT JOIN SKLNOMEI ON SKLN.SklN_Rcd = SKLNOMEI.NmEi_RcdNom AND SKLNOMEI.NmEi_Osn <> 1 AND SKLNOMEI.NmEi_Cd = "+Convert.ToString(CurrentEI)+"  "
                   + "WHERE SklN_Rcd = @ZakNom  "
                   + "  "
                   + "DECLARE @PrcCn DECIMAL(16,4)  "
                   + "DECLARE @PrcEiOsn DECIMAL(12,4)  "
                   + "DECLARE @SklOpaRcd INT  "
                   + "DECLARE @SklPrcRcd INT  "
                   + "DECLARE @SklPrcEi VARCHAR(5)  "
                   + "SELECT   @PrcCn = ROUND(Arc.ArcCn,SVL.SVl_Acc)  "
                   + "     , @SklOpaRcd = SklOpa_Rcd       "
                   + "     , @SklPrcRcd = SklPrc_rcd       "
                   + "	 , @SklPrcEi = EI_ShNm  "
                   + "	 , @PrcEiOsn = CASE ISNULL(NmEi_QtOsn,1) WHEN 0 THEN 1 ELSE ISNULL(NmEi_QtOsn,1) END                         "
                   + "FROM SklPrc INNER JOIN SklPrcRst ON SklPrcRst_Ass = SklPrc_Cd  "
                   + "            INNER JOIN SVL ON SVL.SVL_RCD = SklPrcRst_Val      "
                   + "            INNER JOIN SklOpa ON SklPrc_RcdOpa = SklOpa_Rcd  "
                   + "            LEFT JOIN( SELECT CA.SklPrcArc_Prc AS SklPrcArc_Prc  "
                   + "                            , CA.sklprcarc_cn1b AS ArcCn  "
                   + "                            , CA.sklprcarc_dat AS sklprcarc_dat  "
                   + "                       FROM sklprcarc AS CA  "
                   + "                       WHERE CA.SklPrcArc_PrcR = @ZakPrc  "
                   + "                         AND CA.SklPrcArc_Dat = ( SELECT MAX( A1.SklPrcArc_Dat )  "
                   + "                                                  FROM SklPrcArc AS A1  "
                   + "                                                  WHERE A1.SklPrcArc_Dat <= @ZakDelvrDt      "
                   + "                                                    AND A1.SklPrcArc_PrcR = @ZakPrc  "
                   + "                                                    AND A1.SklPrcArc_Prc = CA.SklPrcArc_Prc  "
                   + "                                                  GROUP BY A1.SklPrcArc_Prc  "
                   + "                                                         , A1.SklPrcArc_PrcR ))AS Arc ON Arc.SklPrcArc_Prc = SklPrc_Rcd  "
                   + "            LEFT JOIN SKLNOMEI ON NmEi_RcdNom = SklOpa_RcdNom and NmEi_Cd = SklPrc_Ei   "
                   + "			LEFT JOIN EI ON EI_Rcd = SklPrc_Ei                                                              "
                   + "WHERE SklPrcRst_Rcd = @ZakPrc  "
                   + "  AND SklPrcRst_Val = 1   "
                   + "  AND SklOpa_RcdNom = @ZakNom  "
                   + "SELECT Convert(DECIMAL(12,4),(@ZakQt * @ZakEiOsn / @PrcEiOsn)) [Qt],(SELECT EI_Rcd FROM EI WHERE EI_ShNm = @SklPrcEi)[EI]";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())
            {
                dr.GetValues(result);
            }
            conn.Close();
            return result;
           
        }


        internal static int GetEIPrice(int PriceList,string Art )
        {
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "select sklprc_ei from SklPrc "
                        + "   left join SKLOPA  on SklPrc_RcdOpa =  SklOpa_Rcd "
                        + "   left join SKLN  on SklN_Rcd = SklOpa_RcdNom "
                        + "where SklPrc_Cd = " + PriceList + " and SklN_Cd  = '" + Art + "'";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())
            {
                dr.GetValues(result);
            }
            conn.Close();
            int Price_EI = Convert.ToInt32(result[0]);
            return Price_EI;
        }

        internal static void ClearTmpZkg()
        {
            string connString = Settings.Default.ConnStringISPRO;
            string Insert = "delete from U_Chtmpzkg";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(Insert, conn);
            SqlDataReader dr = command.ExecuteReader();
            conn.Close();
        }

        public static string Reserved_Rcd(string TableName, string ColumnName)
        {
            //string FirmRcd = DispOrders.GetValueOption("ОБЩИЕ.НОМЕР ПРЕДПРИЯТИЯ");  //не нужно так как подключение происходит в строке string connString = Settings.Default.ConnStringISPRO;
            //string DataBase_Name = DispOrders.GetValueOption("ОБЩИЕ.ИМЯ БД");
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "declare @RetRcd int  "
                                + "DECLARE @DB VARCHAR(40) = DB_NAME() "
                                + "DECLARE @FirmRcd BIGINT = (SELECT TOP 1 TrdPrm_CdFirm FROM TRDPRM) "
                                + " exec spSysGetRcd @DB,'" + TableName + "','" + ColumnName + "',@FirmRcd,4294967295,@RetRcd output  "
                                + " select @RetRcd";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            SqlCommand command = new SqlCommand(verification, conn);
            command.CommandTimeout = 60;

            string RCD = string.Empty;
            SqlDataReader dr = null;
            conn.Open();
            using (conn)
            {
                for (int tryNumber = 3; tryNumber > 0; tryNumber--)
                {
                    try
                    {
                        if (tryNumber < 3) Thread.Sleep(500);
                        if (conn.State == ConnectionState.Closed) conn.Open();
                        if (dr != null) dr.Close();
                        dr = command.ExecuteReader();//не работает!     
                    }
                    catch (SqlException e)
                    {
                        exceptionFlag = true;
                        Program.WriteLine("Ошибка генератора Rcd: " + e.Message + "; Source: " + e.Source);
                        Program.WriteLine("Осталось повторных попыток: " + tryNumber + " из 3.");
                        dr.Close();
                    }

                    if (!exceptionFlag) break;
                }

                if (!exceptionFlag)
                {
                    object[] result = new object[dr.VisibleFieldCount];
                    while (dr.Read())
                    {
                        dr.GetValues(result);
                    }
                    RCD = Convert.ToString(result[0]);
                }
            }              
            conn.Close();
            exceptionFlag = false;
            return RCD;
        }
        
        public static object[] GetArticulFromTMPZKG(int TypeSkln)
        {
            int i = 0;
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select IsArtCd from U_Chtmpzkg where TypeSkln = " + TypeSkln.ToString() + "";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            object[] result = new object[i];
            while (dr.Read())
            {
                Array.Resize(ref result, result.Length + 1);
                result[i] = dr.GetValue(0);
                i++;
            }
            conn.Close();
            return result;
            
        }

        public static string GetDocNumber(DateTime Date, int Jur)
        {
            string connString = Settings.Default.ConnStringISPRO;
            string OnlyDate = Date.ToString(@"yyyy-MM-dd");//дата доставки
            string verification = "SELECT RIGHT( '00000000' + CONVERT(varchar( 8 ), MAX( Nmr ) + 1), 8 )  "
                                + "FROM ( SELECT MAX( CONVERT(bigint, a.PRDZKG_Nmr))AS Nmr FROM PRDZKG a WITH (NOLOCK) "
                                + "LEFT JOIN DOCCFG WITH (NOLOCK)  on DocCfg_Rcd=134 WHERE ISNUMERIC( a.PRDZKG_Nmr ) = 1 AND YEAR( a.PRDZKG_Dt ) >= YEAR( CONVERT(date,'" + OnlyDate + "') ) AND ( DocCfg_Term = 1 AND a.PRDZKG_Dt = CONVERT(date, '" + OnlyDate + "') OR   "
                                + "( DocCfg_Term = 2  AND MONTH( a.PRDZKG_Dt ) = MONTH( CONVERT(date, '" + OnlyDate + "'))) OR ( YEAR( a.PRDZKG_Dt ) < YEAR( CONVERT(date, '" + OnlyDate + "') ) + 1 AND DocCfg_Term = 3 ) OR DocCfg_Term = 0 ) AND a.PRDZKG_JrnRcd = " + Jur + "    "
                                + "UNION   "
                                + "SELECT ISNULL( MAX( DocNmQ_Nmr ), 0 )AS Nmr FROM DOCNMRQ WITH (NOLOCK) WHERE DocNmr_Rcd = '134'   "
                                + "UNION  "
                                + "SELECT ISNULL( MAX( d.PRDZKG_Nmr ), 0 )AS Nmr FROM PRDZKG d)AS FindNmr"; 
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())         //deadlock!!!
            {
                dr.GetValues(result);
            }
            conn.Close();
            string number = Convert.ToString(result[0]);
            return number;
        }

        public static object[] GetDataFromTMPZKG(int TypeSkln)
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select top 1 DocDateDoc,PriceList_RCD from U_Chtmpzkg where TypeSkln = "+ TypeSkln.ToString() +"";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())
            {
                dr.GetValues(result);
            }
            conn.Close();
            return result; 
        }

        public static object[] GetSumOrder(string PrdZkg_Rcd)
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT  SUM(Trds_Sum),SUM(Trds_SumTax) FROM Trds   "
                                + "WHERE  Trds_RcdHdr = "+PrdZkg_Rcd+" AND Trds_TypHdr = 17  "
                                + "GROUP BY Trds_RcdHdr;"; 
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())
            {
                dr.GetValues(result);
            }
            conn.Close();
            return result;
        }

        public static string GetValueOption(string NameOption)
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select OptValue from U_CHZAKOPT where Upper(OptName) = '" + NameOption.ToUpper() + "'";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())
            {
                dr.GetValues(result);
            }
            conn.Close();
            string Value = Convert.ToString(result[0]);
            return Value;
        }

        public static int GetMarID(string Ptn_Cd,string TypeSkln)
        {
            string verification;
            int MarId = 199999;
            string NameTypeSkln = DispOrders.GetNameTypeSkln(TypeSkln);

            if (NameTypeSkln.ToUpper() == "МОЛОКО")
            {
                verification = " SELECT TrdRt_Rcd FROM TRDRT "
                               + " join UFPRV on UF_RkValS = TrdRt_cd  "
                               + " join UFRKV on UFRKV.UFR_RkRcd = UFPRV.UF_RkRcd AND UFPRV.UF_TblId = 1126 "
                               + " WHERE UFR_Id = 'U_MARSHRUT'  and UF_TblRcd =(select Ptn_Rcd from PTNRK where Ptn_Cd = '" + Ptn_Cd + "')";
            }
            else if(NameTypeSkln.ToUpper() == "МОРОЖЕНОЕ")
            {
                verification = " SELECT TrdRt_Rcd FROM TRDRT "
                               + " join UFPRV on UF_RkValS = TrdRt_cd  "
                               + " join UFRKV on UFRKV.UFR_RkRcd = UFPRV.UF_RkRcd AND UFPRV.UF_TblId = 1126 "
                               + " WHERE UFR_Id = 'U_MARSHRUT_MOR'  and UF_TblRcd =(select Ptn_Rcd from PTNRK where Ptn_Cd = '" + Ptn_Cd + "')";
            }
            else
            {
                MarId = 199999;
                return MarId;
            }

            if (MarId == 199999)
            {
                string connString = Settings.Default.ConnStringISPRO;
                SqlConnection conn = new SqlConnection();
                conn.ConnectionString = connString;
                conn.Open();
                SqlCommand command = new SqlCommand(verification, conn);
                Program.WriteLine("Метод взятия маршрута по умолчанию. (график маршрута не указан)");
                SqlDataReader dr = command.ExecuteReader();
                int i = 0;
                object[] result = new object[i];
                while (dr.Read())
                {
                    Array.Resize(ref result, result.Length + 1);
                    result[i] = dr.GetValue(0);
                    i++;
                }
                conn.Close();
                if (result.Length == 0)
                {
                    MarId = 199999;
                }
                else
                {
                    MarId = Convert.ToInt32(result[0]);
                }
            }
            
            return MarId;
        }


        internal static void CreateOrder(string PrdZkg_Rcd,string TypeSkln/*,string DocNumber*/, string DocSum, string DocSumTax, string PrcRcd,string Ptn_Cd,string PtnGroup)//создание заказа - создание записи в таблице prdzkg
        {
            string SkladId, UsID, Jur;
            int MarId = 199999;

            try
            {
                SkladId = DispOrders.GetSkladId(PtnGroup, TypeSkln);

                if (TypeSkln == "5") MarId = DispOrders.GetRouteSchedule(Ptn_Cd); // если у к/а есть график маршрутов, то MarId получит соответсвующий Rcd маршрута, иначе 199999

                if (MarId == 199999) MarId = DispOrders.GetMarID(Ptn_Cd, TypeSkln);

                Jur = Verifiacation.GetJurOrder(PtnGroup, TypeSkln);
            }
            catch (IOException e)
            {
                Program.WriteLine("Ошибка при взятии данных для записи заказа. Источник: " + e.Source + ".Сообщение: " + e.Message);
                DispOrders.WriteErrorLog(e.Message);

                return;
            }


            if (MarId == 199999)//ошибка - забыли указать маршрут
            {
                Program.WriteLine("У К/А " + Ptn_Cd + " не указан маршрут!");
                WriteOrderLog("ИС-ПРО", Convert.ToString(Ptn_Cd), Convert.ToString(Ptn_Cd), "ИС-ПРО", "", 99, "У контрагента " + Ptn_Cd + " не указан маршрут. Проставте маршрут в карточке к/а и в заказе!", DateTime.Today, DateTime.Now,Convert.ToInt32(Jur));
                MarId = 0;
            }
            if (MarId != 0)
            {
                    UsID = "1";
            }
            else
            {
                UsID = "0";
            }
            string connString = Settings.Default.ConnStringISPRO;
            decimal Sum, SumTax, DiffSum;
            Sum = Convert.ToDecimal(DocSum);
            SumTax = Convert.ToDecimal(DocSumTax);
            DiffSum = Sum - SumTax;
            Program.WriteLine("Запуск процедуры вставки заказа в постоянную таблицу (U_ChCreateOrderNew)...");
            string Insert = "exec U_ChCreateOrderNew '" + PrdZkg_Rcd + "','" + Jur + "','" + UsID + "','" + SkladId + "','" + Convert.ToString(MarId) + "','" + Convert.ToString(Sum).Replace(",", ".") + "'," + Convert.ToString(SumTax).Replace(",", ".") + ",'" + Convert.ToString(DiffSum).Replace(",", ".") + "','"+PrcRcd+"'";
         
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;    
            SqlCommand command = new SqlCommand(Insert, conn);
            command.CommandTimeout = 60;
            SqlDataReader dr = null;
            
            conn.Open();

            for (int tryNumber = 3; tryNumber > 0; tryNumber--)
            {
                try
                {
                    Program.WriteLine("Запуск SqlDataReader. - SqlCommand.ExecuteReader()");
                    if (conn.State == ConnectionState.Closed) conn.Open();
                    using (conn)
                    {
                        //if (tryNumber < 3) Thread.Sleep(500);
                        if (dr != null) dr.Close();
                        dr = command.ExecuteReader();
                    }  
                }
                catch (SqlException exception)
                {
                    exceptionFlag = true;
                    Program.WriteLine("Ошибка при записи в таблицу PRDZKG. Источник: " + exception.Source + ".Сообщение: " + exception.Message);
                    DispOrders.WriteErrorLog(exception.Message);
                    Program.WriteLine("Повторная попытка записи...");
                    Program.WriteLine("Осталось попыток: " + tryNumber + " из 3.");
                    if (dr != null) dr.Close();
                    //command.CommandTimeout = 60;
                }
                
                conn.Close();
                if (!exceptionFlag) break;
            }

            if (!exceptionFlag) Program.WriteLine("Процедура вставки заказа U_ChCreateOrderNew выполнена успешно.");
            else Program.WriteLine("Не удалось выполнить процедуру U_ChCreateOrderNew.");

            exceptionFlag = false;
        }

        internal static void CreateItemPosition(string TrdS_Rcd, string PrdZkg_Rcd,string Articul)//создание товарной позиции - создание записи в таблице TrdS
        {
            string connString = Settings.Default.ConnStringISPRO;
            string Insert = "exec U_ChCreateItemPosition '" + TrdS_Rcd + "', '" + PrdZkg_Rcd + "', '" + Articul + "' ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            SqlCommand command = new SqlCommand(Insert, conn);
            SqlDataReader dr = null;
            conn.Open();

            for (int tryNumber = 5; tryNumber > 0; tryNumber--)
            {
                try
                {
                    if (tryNumber < 5) Thread.Sleep(500);  // иначе генерируется такой же Rcd как предыдущий
                    if (conn.State == ConnectionState.Closed) conn.Open();
                    if (dr != null) dr.Close();
                    dr = command.ExecuteReader();
                }
                catch (SqlException e)
                {
                    exceptionFlag = true;
                    Program.WriteLine("Ошибка генератора Rcd: " + e.Message + "; Source: " + e.Source);
                    Program.WriteLine("Осталось повторных попыток: " + tryNumber + " из 5.");
                    if (dr != null) dr.Close();
                    command.CommandTimeout = 60;
                }

                if (!exceptionFlag) break;

            }
            dr.Close();
            conn.Close();
            exceptionFlag = false;
            }

        internal static void WriteOrderLog(string source, string plat, string deliv, string filename,string num_order,int error,string error_prop, DateTime date, DateTime time, int TypeSkln )//запись в лог приема заказа
        {
            string connString = Settings.Default.ConnStringISPRO;
            string Insert = "INSERT INTO U_CHLOGTAKEORD( SourceOrder, Plat, Deliv, NameFile, Num_order, Error, Error_Prop, Date, Time, TypeSkln ) "
                + " VALUES( '" + source + "', substring('" + plat + "',1,50), substring('" + deliv + "',1,50), '" + filename + "', '" + num_order + "', '" + Convert.ToString(error) + "', '" + error_prop + "', '" + date.ToString("yyyyMMdd") + "', '" + time.ToString("HH:mm:ss") + "','"+Convert.ToString(TypeSkln)+"')";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(Insert, conn);
            SqlDataReader dr = command.ExecuteReader();
            conn.Close();

        }

        internal static void TMPtoPrdZkg(string[] Buyer, string[] Deliv, string Name_ParseFile, string provider,string NumOreder, string sellerCodeByBuyer = "")//переносит заказ на постоянку
        {
            int[] DstSkln = DistinctTypeSkln();//проверка на TypeSkln
            string PtnGroup = Verifiacation.GetPtnGroup(Deliv[0]);
            Program.WriteLine(Name_ParseFile);
            foreach (int ts in DstSkln)
            {
                Program.WriteLine("Создание и резерв RCD для заказа " + NumOreder + "... ");
                string PrdZkg_Rcd = DispOrders.Reserved_Rcd("PrdZkg", "PrdZkg_Rcd");//резервируем rcd в таблице PrdZkg           
                if (PrdZkg_Rcd != null)
                {
                    Program.WriteLine("RCD: " + PrdZkg_Rcd);
                    object[] ArtList = DispOrders.GetArticulFromTMPZKG(ts);
                    int count = ArtList.Count();
                    Program.WriteLine("Создание и резерв RCD для позиций заказа " + NumOreder + "... ");
                    for (int i = 0; i < count; i++)
                    {
                        string TrdS_Rcd = DispOrders.Reserved_Rcd("TrdS", "TrdS_Rcd");//резервируем rcd в таблице TrdS
                        Program.WriteLine("[" + i + "] RCD: " + TrdS_Rcd);
                        DispOrders.CreateItemPosition(TrdS_Rcd, PrdZkg_Rcd, Convert.ToString(ArtList[i]));
                    }

                    object[] dTMPZKG = DispOrders.GetDataFromTMPZKG(ts);//0-дата документа 1 - rcd прайслиста
                    //string DocNumber = DispOrders.GetDocNumber(Convert.ToDateTime(dTMPZKG[0]), Convert.ToInt16(dTMPZKG[1]));
                    string number_jur = Verifiacation.GetJurOrder(PtnGroup, Convert.ToString(ts));
                    object[] Sum;
                    try
                    {
                        Sum = DispOrders.GetSumOrder(PrdZkg_Rcd);
                    }
                    catch
                    {
                        Sum = new object[] {0,0};
                    }

                    DispOrders.CreateOrder(PrdZkg_Rcd, Convert.ToString(ts), Convert.ToString(Sum[0]), Convert.ToString(Sum[1]), Convert.ToString(dTMPZKG[1]),Convert.ToString(Deliv[0]),PtnGroup);

                    //Запись в лог об удаче
                    if (Convert.ToString(Sum[0]) != "0")
                    {
                        DispOrders.WriteOrderLog(provider, Buyer[0] + " - " + Buyer[1], Deliv[0] + " - " + Deliv[1], Name_ParseFile, NumOreder, 0, "Ошибок нет. Заказ принят. Журнал " + number_jur, DateTime.Today, DateTime.Now, ts);
                    }
                    else
                    {
                        DispOrders.WriteOrderLog(provider, Buyer[0] + " - " + Buyer[1], Deliv[0] + " - " + Deliv[1], Name_ParseFile, NumOreder, 50, "Нулевая сумма заказа! Журнал " + number_jur, DateTime.Today, DateTime.Now, ts);
                    }
                    Program.WriteLine("Заказ принят. " + provider + ". Журнал " + number_jur);
                    Program.WriteLine("----------------");
                    
                    //Только для эдисофт и скб-контур

                    switch (provider)
                    {
                        case "СКБ-Контур":
                            DispOrders.WriteProtocolEDI("Заказ", Name_ParseFile, Buyer[0] + " - " + Buyer[1], 0, Deliv[0] + " - " + Deliv[1], "Заказ принят", DateTime.Now, NumOreder, provider);
                            DispOrders.WriteEDIExchRecord(NumOreder, Convert.ToInt64(PrdZkg_Rcd), Convert.ToInt64(Buyer[3]), Convert.ToInt64(Deliv[3]), DateTime.Now, Name_ParseFile, 1, sellerCodeByBuyer);
                            break;
                        case "EDI-Софт":
                            DispOrders.WriteProtocolEDI("Заказ", Name_ParseFile, Buyer[0] + " - " + Buyer[1], 0, Deliv[0] + " - " + Deliv[1], "Заказ принят", DateTime.Now, NumOreder, provider);
                            DispOrders.WriteEDIExchRecord(NumOreder, Convert.ToInt64(PrdZkg_Rcd), Convert.ToInt64(Buyer[3]), Convert.ToInt64(Deliv[3]), DateTime.Now, Name_ParseFile, 1, sellerCodeByBuyer);
                            break;
                        default: break;
                    }

                    //ПРОВЕРКА НА ОТПРАВКУ ORDERSP
                    
                    string enbl = Verifiacation.CheckEnabledSentEdiDoc(Convert.ToString(Deliv[0]), "Rsp");//Rsp-orderSp
                    if (enbl == "1")
                    {
                        if (provider == "СКБ-Контур") 
                        {
                            EDIXMLCreation.CreateKonturOrderSp(PrdZkg_Rcd);//отправляем ordersp СКБ-Контур - передать Prdzkg_rcd
                        }
                        if (provider == "EDI-Софт") 
                        {
                            EDIXMLCreation.CreateEdiOrderSp(PrdZkg_Rcd);//отправляем ordersp EDI-Софт - передать Prdzkg_rcd
                        }
                    }
                    else
                    {
                        continue;
                    }

                }
                else
                {
                    Program.WriteLine("Ошибка записи в базу. Не могу зарезервировать номер заказа!");
                    DispOrders.WriteErrorLog("Ошибка записи в базу. Не могу зарезервировать номер заказа! Файл " + Name_ParseFile);
                }
            }
        }

        internal static object[,] GetListSFOld(int count)//получение списка СФ на отправку с учетом времени отсрочки [14]-признак ксф, если !=0 то ксф
        {
            int i = 0;
            string connString = Settings.Default.ConnStringISPRO;
            string CMDGetSF = " select * from U_vwChListSfForSent "//сделал вьюшку
                           + " where  IsProDoc is NULL and "
                           + " ((ISNULL(SklSfA_RcdCor,0)=0 and SklSf_Dt between DATEADD(dd,-(ISNULL((select top 1 [Dn_Vz] from U_CHEDINSTSF),10)),CONVERT(date,getdate())) and CONVERT(date,getdate())) "
                           + " or "
                           + " (ISNULL(SklSfA_RcdCor,0)!=0 and SklSf_Dt between DATEADD(dd,-(ISNULL((select top 1 [Dn_Vz_K] from U_CHEDINSTSF),10)),CONVERT(date,getdate())) and CONVERT(date,getdate()))) "
                           + " and ((ISNULL(SklSfA_RcdCor,0)=0 and   dateadd(dd,ISNULL((select top 1 [Dn_Zd] from U_CHEDINSTSF),0),SklSf_Dt)<=CONVERT(date,getdate())) or (ISNULL(SklSfA_RcdCor,0)!=0 and  dateadd(dd,ISNULL((select top 1 [Dn_Zd_K] from U_CHEDINSTSF),0),SklSf_Dt)<=CONVERT(date,getdate()))) ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(CMDGetSF, conn);
            SqlDataReader dr = command.ExecuteReader();
            object[,] result = new object[count,21];
            while (dr.Read())
            {
                result[i, 0] = dr.GetValue(0);
                result[i, 1] = dr.GetValue(1);
                result[i, 2] = dr.GetValue(2);
                result[i, 3] = dr.GetValue(3);
                result[i, 4] = dr.GetValue(4);
                result[i, 5] = dr.GetValue(5);
                result[i, 6] = dr.GetValue(6);
                result[i, 7] = dr.GetValue(7);
                result[i, 8] = dr.GetValue(8);
                result[i, 9] = dr.GetValue(9);
                result[i, 10] = dr.GetValue(10);
                result[i, 11] = dr.GetValue(11);
                result[i, 12] = dr.GetValue(12);
                result[i, 13] = dr.GetValue(13);
                result[i, 14] = dr.GetValue(14);
                result[i, 15] = dr.GetValue(15);
                result[i, 16] = dr.GetValue(16);
                result[i, 17] = dr.GetValue(17);
                result[i, 18] = dr.GetValue(18);
                result[i, 19] = dr.GetValue(19);
                result[i, 20] = dr.GetValue(20);
                i++;
            }
            conn.Close();
            return result;
        }

        internal static object[,] GetListSF()//получение списка СФ на отправку с учетом времени отсрочки [14]-признак ксф, если !=0 то ксф [21] = номер RECADV [22] = дата
        {
            int i = 0;
            string connString = Settings.Default.ConnStringISPRO;

            string CMDGetSF = " select * from U_vwChListSfForSentPlat WHERE SklSf_TpOtg = 0 ";  //Ограничения по периоду заложила в саму вьюху SklSf_TpOtg = 0 --счет фактура

            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(CMDGetSF, conn);
            SqlDataReader dr = command.ExecuteReader();

            int recordCount = 0;
            while (dr.Read()) recordCount++;
            object[,] result = new object[recordCount, dr.FieldCount];

            dr.Close();

            dr = command.ExecuteReader();

            while (dr.Read())
            {
                for (int j = 0; j < dr.FieldCount; j++) result[i, j] = dr.GetValue(j);
                i++;
            }
            conn.Close();
            return result;
        }


        internal static int CountSF()
        {
            int i=0,count;
            string connString = Settings.Default.ConnStringISPRO;
            string CMDGetSF = "select COUNT (*) from U_vwChListSfForSent " //сделал вьюшку
                           + " where  IsProDoc is NULL and "
                           + " ((ISNULL(SklSfA_RcdCor,0)=0 and SklSf_Dt between DATEADD(dd,-(ISNULL((select top 1 [Dn_Vz] from U_CHEDINSTSF),10)),CONVERT(date,getdate())) and CONVERT(date,getdate())) "
                           + " or "
                           + " (ISNULL(SklSfA_RcdCor,0)!=0 and SklSf_Dt between DATEADD(dd,-(ISNULL((select top 1 [Dn_Vz_K] from U_CHEDINSTSF),10)),CONVERT(date,getdate())) and CONVERT(date,getdate()))) "
                           + " and ((ISNULL(SklSfA_RcdCor,0)=0 and   dateadd(dd,ISNULL((select top 1 [Dn_Zd] from U_CHEDINSTSF),0),SklSf_Dt)<=CONVERT(date,getdate())) or (ISNULL(SklSfA_RcdCor,0)!=0 and  dateadd(dd,ISNULL((select top 1 [Dn_Zd_K] from U_CHEDINSTSF),0),SklSf_Dt)<=CONVERT(date,getdate()))) ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(CMDGetSF, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())
            {
                result[i] = dr.GetValue(0);
                i++;
            }
            conn.Close();
            count = Convert.ToInt32(result[0]);
            return count;
        }

        public static object[,] GetItemsFromTrdS(string DocRcd, int cnt, int typHdr, bool PCE = false)//rcd документа, количество позиций
        {
            int i = 0;
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "";
            if (PCE == true)
            {
                verification = "select  LEFT(BarCode_Code, 13) BarCode_Code, " //есть позиции, использующие одинаковый штрихкод, но ИСПРО не позволяет вводить 2 одинаковых штрихкода, в качестве 14 символа используем любое число
                                   + "     case when LEFT( SklGr_CD, 3 )IN( '039', '139' ) then SUBSTRING(skln_nmAlt,1,6) else SUBSTRING(skln_nmAlt,1,4) end as Code, "
                                   + "	   SklN_Cd, "
                                   + "	   SklN_NmAlt, "
                                   + "	   Convert(decimal(18,3),TrdS_Qt * (TrdS_QtOsn/TrdS_Qt)/EISht.NmEi_QtOsn), "
                                   + "	   CONVERT( DECIMAL(18, 2), (TrdS_Cn * EISht.NmEi_QtOsn / TrdS_QtOsn * TrdS_Qt) / CONVERT( DECIMAL(18, 2), (TrdS_Cn * EISht.NmEi_QtOsn / TrdS_QtOsn * TrdS_Qt) / CASE WHEN ISNULL(Trx.TaxRate_Val,'0') = '0' THEN 1 ELSE 1 + CAST( RTRIM( LTRIM( Trx.TaxRate_Val ))AS FLOAT ) / 100  END)), "
                                   + "	   CONVERT( DECIMAL(18, 2), (TrdS_Cn * EISht.NmEi_QtOsn / TrdS_QtOsn * TrdS_Qt)), "
                                   + "	   'PCE', "
                                   + "	   EI_OKEI, "
                                   + "	   Convert(decimal(18,2),TaxRate_Val), "
                                   + "	   'S', "
                                   + "	   Convert(decimal(18,2),TrdS_SumTax), "
                                   + "     CONVERT( DECIMAL(18, 2), TrdS_SumOpl - TrdS_SumTax), "
                                   + "	   CONVERT( DECIMAL(18, 2), TrdS_SumOpl ),  "
                                   + "	   mVR_uuid vsd  "
                                   + "from TRDS  "
                                   + "        LEFT JOIN SKLN AS Nom ON Nom.SklN_Rcd = TrdS_RcdNom "
                                   + "        LEFT JOIN BarCode bc on bc.BarCode_RcdNom = Nom.SklN_Rcd AND BARCODE_Base = 1 "
                                   + "        LEFT JOIN EI ON EI_Rcd = 5  "
                                   + "        LEFT JOIN SKLGR on SklN_RcdGrp = SklGr_Rcd "
                                   + "        LEFT JOIN DBO.ATRDSTAX AS Tax ON Tax.ATrdSTax_RcdS = TRDS.TrdS_Rcd AND Tax.ATrdSTax_RcdAs = 0 "
                                   + "        LEFT JOIN DBO.TAXRATE AS Trx ON Trx.TaxRate_Rcd = Tax.TrdSTax_RateCd AND Trx.TaxRate_RcdTax = Tax.TrdSTax_Cd "
                                   + "        LEFT JOIN SKLNOMEI AS EISht ON Nom.SklN_Rcd = EISht.NmEi_RcdNom AND EISht.NmEi_Cd = 5 "
                                   + "        LEFT JOIN U_M_sprnom ON mNom_ISPRO_Rcd=trds_rcdnom "
                                   + "        LEFT JOIN U_M_VSDREAL ON mVR_NomGuid=mNom_guid and mVR_RcdSklnk=trds_rcdhdr "
                                   + " where TrdS_TypHdr = " + typHdr + " and trds_rcdhdr = '" + DocRcd + "'";
            }
            else
            {
               verification = "select  LEFT(BarCode_Code, 13) BarCode_Code, " //есть позиции, использующие одинаковый штрихкод, но ИСПРО не позволяет вводить 2 одинаковых штрихкода, в качестве 14 символа используем любое число
                                   + "     case when LEFT( SklGr_CD, 3 )IN( '039', '139' ) then SUBSTRING(skln_nmAlt,1,6) else SUBSTRING(skln_nmAlt,1,4) end as Code, "
                                   + "	   SklN_Cd, "
                                   + "	   SklN_NmAlt, "
                                   + "	   Convert(decimal(18,3),TrdS_Qt), "
                                   + "	   Convert(decimal(18,2),((trds_sumopl-trds_sumtax)/TrdS_QtCn)), "
                                   + "	   Convert(decimal(18,2),trds_sumopl/TrdS_Qt), "
                                   + "	   case when EI_Rcd=1 then 'KG' when EI_Rcd=5 then 'PCE' when EI_Rcd=39 then 'CT' end, "
                                   + "	   EI_OKEI, "
                                   + "	   Convert(decimal(18,2),TaxRate_Val), "
                                   + "	   'S', "
                                   + "	   Convert(decimal(18,2),TrdS_SumTax), "
                                   + "     CONVERT( DECIMAL(18, 2), (trds_sumopl-trds_sumtax)/TrdS_Qt ) * CONVERT( DECIMAL(18, 3), TrdS_Qt ), "
                                   + "	   CONVERT( DECIMAL(18, 2), TrdS_SumExt )*CONVERT( DECIMAL(18, 3), TrdS_Qt ), "
                                   + "	   mVR_uuid vsd  "
                                   + "from TRDS  "
                                   + "        LEFT JOIN SKLN AS Nom ON Nom.SklN_Rcd = TrdS_RcdNom "
                                   + "        LEFT JOIN BarCode bc on bc.BarCode_RcdNom = Nom.SklN_Rcd AND BARCODE_Base = 1 "
                                   + "        LEFT JOIN EI on TrdS_EiQt = ei_rcd "
                                   + "        LEFT JOIN SKLGR on SklN_RcdGrp = SklGr_Rcd "
                                   + "        LEFT JOIN (ATRDSTAX ATT1 left join TAXRATE TR1 on TR1.TaxRate_Rcd = ATT1.TrdSTax_RateCd) on TrdS_Rcd = ATT1.ATrdSTax_RcdS and ATT1.TrdSTax_Cd = 1 "
                                    + "       LEFT JOIN U_M_sprnom ON mNom_ISPRO_Rcd=trds_rcdnom "
                                   + "        LEFT JOIN U_M_VSDREAL ON mVR_NomGuid=mNom_guid and mVR_RcdSklnk=trds_rcdhdr "
                                   + " where TrdS_TypHdr = " + typHdr + " and trds_rcdhdr = '" + DocRcd + "'";
            }
            
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[,] result = new object[cnt, 15];
            dr.Close();
            dr = command.ExecuteReader();
            while (dr.Read())
            {
                try
                {
                    result[i, 0] = dr.GetValue(0);
                    result[i, 1] = dr.GetValue(1);
                    result[i, 2] = dr.GetValue(2);
                    result[i, 3] = dr.GetValue(3);
                    result[i, 4] = dr.GetValue(4);
                    result[i, 5] = dr.GetValue(5);
                    result[i, 6] = dr.GetValue(6);
                    result[i, 7] = dr.GetValue(7);
                    result[i, 8] = dr.GetValue(8);
                    result[i, 9] = dr.GetValue(9);
                    result[i, 10] = dr.GetValue(10);
                    result[i, 11] = dr.GetValue(11);
                    result[i, 12] = dr.GetValue(12);
                    result[i, 13] = dr.GetValue(13);
                    result[i, 14] = dr.GetValue(14);
                    i++;
                }
                catch(IOException e)
                {
                    string error = "Ошибка в процедуре GetItemsFromTrdS. Ошибка получении товарных позиций. TRDS-RCDHDR: " + DocRcd;
                    Program.WriteLine(error);
                    WriteErrorLog(error);
                    WriteErrorLog(e.Message);
                    WriteErrorLog(e.Source);
                }
               
            }
            conn.Close();
            return result;
        }

        public static object[] GetTotal(string NumDoc, int typHdr)//вход - номер сф или заказа. выход - [0]количество всех товарных позиций, [1]сумма ндс, [2]сумма с ндс, [3]сумма без ндс
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification  = "select Convert(decimal(18,2),SUM(TrdS_Qt)) as volume, "
                                   + "       Convert(decimal(18,2),SUM((trds_sumopl-trds_sumtax)/TrdS_QtCn)), "
                                   + "       Convert(decimal(18,2),SUM(trds_sumopl/TrdS_Qt)) as withnds, "
                                   + "       Convert(decimal(18,2),SUM(trds_sumopl/TrdS_Qt))-Convert(decimal(18,2),SUM((trds_sumopl-trds_sumtax)/TrdS_QtCn)), "
                                   +"        SUM(trds_sumopl) as TotalAmount, "
                                   +"        Convert(decimal(18,2),SUM(trds_sumopl-trds_sumtax)) "
                                   + " from TRDS "
                                   + " where trds_typHdr = "+ typHdr +" and trds_rcdhdr = '" + NumDoc + "'";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())
            {
                dr.GetValues(result);
            }
            conn.Close();
            return result;
        }

        internal static void WriteInvoiceLog(string plat, string deliv, string filename, string num_order, int error, string error_prop, DateTime date)//запись в лог сф
        {
            string connString = Settings.Default.ConnStringISPRO;
            if (error_prop.Length > 249)
            {
                error_prop = error_prop.Substring(1, 249);
            }
            string Insert = "INSERT INTO U_CHLOGSENTSF( Plat, Deliv, NameFile, Num_order, Error, Error_Prop, Date) "
                + " VALUES( '" + plat + "', '" + deliv + "', '" + filename + "', '" + num_order + "', '" + Convert.ToString(error) + "', '" + error_prop + "', '" + date.ToString("yyyyMMdd HH:mm:ss") + "')";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(Insert, conn);
            SqlDataReader dr = command.ExecuteReader();
            conn.Close();

        }

        internal static void WriteEDiSentDoc(string Doc, string FileName, string IsproDocRcd,string NmrDocIspro,string Sdoc,string SmDoc,string PrdzkgTxt, int flag, string dopTxt = "")//запись в лог отправленных документов EDI
        {
            string connString = Settings.Default.ConnStringISPRO;
            string Insert = "exec U_MgEDISentDocNew " + Doc + ",'" + (DateTime.Now).ToString("yyyyMMdd HH:mm:ss") + "', '" + FileName + "', " + IsproDocRcd + ", '" + NmrDocIspro + "','robot' ," + Sdoc + ", " + SmDoc.Replace(",",".") + ",'" + PrdzkgTxt + "'" + "," + flag + ",'" + dopTxt + "'";           
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(Insert, conn);
            SqlDataReader dr = command.ExecuteReader();
            conn.Close();

        }

        internal static void WriteProtocolEDI(string typedoc, string filename, string plat, int error, string deliv, string status, DateTime DT, string num_order,string Provider)//запись в лог приема заказа
        {
            string connString = Settings.Default.ConnStringISPRO;
            string Insert = "INSERT INTO U_CHProtocolED( TypeDoc, NameFile, Plat, Error, Deliv, Status, DT, NumOrderEDI,Provider ) "
                + " VALUES( '" + typedoc + "', '" + filename + "', '" + plat + "', '" + Convert.ToString(error) + "', '" + deliv + "', '" + status+ "', '" + DT.ToString("yyyyMMdd HH:mm:ss") + "', '" + num_order + "','"+Provider+"')";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(Insert, conn);
            SqlDataReader dr = command.ExecuteReader();
            conn.Close();

        }

        internal static void WriteEDIExchRecord(string orderNmrExt, Int64 orderZkgRcd, Int64 kagRcd, Int64 gplRcd, /*string provider,*/ DateTime getDate, string orderFile, int stt, string sellerCodeByBuyer = "")//запись в таблицу документооборота EDI
        {
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "INSERT INTO U_CHEDIEXCH (Exch_OrdNmrExt, Exch_ZkgRcd, Exch_RcdKag, Exch_RcdGpl, Exch_OrdPrdr, Exch_OrdDat, Exch_OrdTim, Exch_OrdFile, Exch_OrdStt, Exch_USelCdByer, Exch_OrdFmt)\n"
                       + "SELECT '" + orderNmrExt + "', " + orderZkgRcd.ToString() + ", " + kagRcd.ToString() + ", " + gplRcd.ToString() + ",(SELECT TOP 1 Provdr FROM U_CHEDINASTDOC WHERE Gpl_Rcd = " + gplRcd.ToString() + "), '" + getDate.ToString("yyyyMMdd") + "', '1900-01-01 " + getDate.ToString("HH:mm:ss") + "','" + orderFile + "', " + stt.ToString() + ",'" + sellerCodeByBuyer + "', (SELECT TOP 1 NastDoc_Fmt FROM U_CHEDINASTDOC WHERE Gpl_Rcd = " + gplRcd.ToString() + ")";

            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            conn.Close();
        }

        internal static void WriteEDIExchDetail(Int64 orderZkgRcd, int docTyp, int stt, DateTime getDate, string file, string cmt)//запись в таблицу детализации документооборота EDI
        {
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "INSERT INTO dbo.U_CHEDIEXCHDET (Det_ZkgRcd, Det_DocTyp, Det_Dat, Det_Tim, Det_Stt, Det_File, Det_Cmt)\n"
           + "VALUES(" + orderZkgRcd.ToString() + "," + docTyp.ToString() + ",'" + getDate.ToString("yyyy-MM-dd") + "', '1900-01-01 " + getDate.ToString("HH:mm:ss") + "', " + stt.ToString() + ",'" + file + "','" + cmt + "')";

            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            conn.Close();
        }

        internal static object[,] GetListDesadv(/*int count*/)//получение списка Desadv на отправку
        {
            int i = 0;
            string connString = Settings.Default.ConnStringISPRO;
            string CMDGetSF = " select Provdr,SklNk_Nmr,SklNk_Rcd,SklNk_RcdZkg,SklNk_KAgId,SklNk_GplRcd,PrdZkg_NmrExt,PrdZkg_DtOtg,SklNk_Dat,PrdZkg_Dt,SklNk_TAut1,SG_Nm,SklNk_TDrvNm, SklNk_GplAdr from U_vwChListDesadvForSent ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            /*conn.Open();
            SqlCommand command = new SqlCommand(CMDGetSF, conn);
            SqlDataReader dr = command.ExecuteReader();
            object[,] result = new object[count, 14];
            while (dr.Read())
            {
                result[i, 0] = dr.GetValue(0);
                result[i, 1] = dr.GetValue(1);
                result[i, 2] = dr.GetValue(2);
                result[i, 3] = dr.GetValue(3);
                result[i, 4] = dr.GetValue(4);
                result[i, 5] = dr.GetValue(5);
                result[i, 6] = dr.GetValue(6);
                result[i, 7] = dr.GetValue(7);
                result[i, 8] = dr.GetValue(8);
                result[i, 9] = dr.GetValue(9);
                result[i, 10] = dr.GetValue(10);
                result[i, 11] = dr.GetValue(11);
                result[i, 12] = dr.GetValue(12);
                result[i, 13] = dr.GetValue(13);
                i++;
            }
            conn.Close();
            return result;*/
            object[] resultRow = null;
            List<object[]> resultList = new List<object[]>();
            object[,] result = null;
            int fieldCount = 1;
            try
            {
                conn.Open();
            }
            catch (Exception e)
            {
                Program.WriteLine("Ошибка соединения с БД " + Convert.ToString(e));
                if (conn.State == ConnectionState.Open) conn.Close();
            }
            SqlCommand command = new SqlCommand(CMDGetSF, conn);
            SqlDataReader dr = command.ExecuteReader();

            //dr.Close();
            //dr = command.ExecuteReader();
            
            if (dr.HasRows) fieldCount = dr.FieldCount;

            //int recordCount = 0;

            while (dr.Read())
            {
                resultRow = new object[fieldCount];
                for (int j = 0; j < fieldCount; j++) resultRow[j] = dr.GetValue(j);
                resultList.Add(resultRow);
            }

            if (resultList.Count > 0)
            {
                result = new object[resultList.Count, fieldCount];
                for (i = 0; i < resultList.Count; i++)
                {
                    for (int j = 0; j < fieldCount; j++) result[i, j] = resultList[i][j];
                }
            }
            else result = new object[0, 0];
            /*result = new object[recordCount, dr.FieldCount];

            dr.Close();

            dr = command.ExecuteReader();

            while (dr.Read())
            {
                for (int j = 0; j < dr.FieldCount; j++) result[i, j] = dr.GetValue(j);
                i++;
            }*/
            conn.Close();
            return result;

        }

        internal static int CountDesadv()
        {
            int i = 0, count;
            string connString = Settings.Default.ConnStringISPRO;
            string CMDGetDV = " select count(*) from U_vwChListDesadvForSent ";//сделал вьюшку
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(CMDGetDV, conn);
            command.CommandTimeout = 0;
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())
            {
                result[i] = dr.GetValue(0);
                i++;
            }
            conn.Close();
            count = Convert.ToInt32(result[0]);
            return count;
        }

        internal static void WriteErrorLog(string error)//запись в лог ошибок программы
        {
            string connString = Settings.Default.ConnStringISPRO;
            string DT = DateTime.Now.ToString("yyyyMMdd hh:mm:ss");
            string Insert = "INSERT INTO U_CHLOGERRTAKE( DT, Error) VALUES ( '" + DT + "' , Substring('"+error+"',1,300))";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(Insert, conn);
            SqlDataReader dr = command.ExecuteReader();
            conn.Close();

        }

        internal static object[,] GetOrderMilkFromOptimum(int n)//получение заказов на молоко из оптимум
        {
            int i = 0;
            string date = DateTime.Today.ToString("yyyyMMdd");
            string connString = Settings.Default.ConnStringOptimum;
            string sql = "SELECT  "
                       + "     'KPK'+CONVERT( VARCHAR,ord.orNumber)+CONVERT( VARCHAR,ord.MasterFID)+CONVERT( VARCHAR,ord.orID) AS 'Number', "
                       + "	   CONVERT( DATE,ord.orDate) AS 'DateDoc', "
                       + "	   CONVERT( DATE,ord.orShippingDate) AS 'DateExp', "
                       + "	   fcs.exID AS 'PtnCd', "
                       + "	   fcs.fShortName AS 'PtnName', "
                       + "	   fcs.fAddress AS 'PtnAddress', "
                       + "	   ord.orComment AS 'Comment', "
                       + "	   CONVERT(VARCHAR,ord.orNumber)+CONVERT(VARCHAR,ord.MasterFID)+CONVERT( VARCHAR,ord.orID) AS FileId, "
                       + "	   ord.orDate, "
                       + "	   ord.orID, "
                       + "	   ord.MasterFID "
                       + "FROM DS_Orders AS ord "
                       + "	   LEFT JOIN DS_Faces AS fcs ON fcs.fID = ord.mfID   "
                       + "WHERE ord.OrType = 0 "
                       + "AND ord.orDate BETWEEN DATEADD( DD,-1,'" + date + "') AND DATEADD( DD,1,'" + date+"' ) "
                       + "AND ord.Condition = 1 "
                       + "AND ord.fState = 0 "
                       + "AND LEFT( "
                       + "( SELECT TOP 1 igp.IgIdText "
                       + "  FROM DS_Orders_Items AS ordit "
                       + "       JOIN DS_Items AS itm ON ordit.iID = itm.iID "
                       + "       JOIN DS_UnitTypes AS utp ON ordit.unitID = utp.utID "
                       + "       JOIN DS_IGROUPS AS igp ON igp.igid = itm.it2ID "
                       + "  WHERE ordit.orItemsDate = ord.orDate "
                       + "        AND ordit.OrID = ord.OrId "
                       + "        AND ordit.Masterfid = ord.Masterfid),3 ) not in ('039','139') ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            //int n = dr.VisibleFieldCount;
            object[,] result = new object[n, 11];
            while (dr.Read())
            {
                result[i, 0] = dr.GetValue(0);
                result[i, 1] = dr.GetValue(1);
                result[i, 2] = dr.GetValue(2);
                result[i, 3] = dr.GetValue(3);
                result[i, 4] = dr.GetValue(4);
                result[i, 5] = dr.GetValue(5);
                result[i, 6] = dr.GetValue(6);
                result[i, 7] = dr.GetValue(7);
                result[i, 8] = dr.GetValue(8);
                result[i, 9] = dr.GetValue(9);
                result[i, 10] = dr.GetValue(10);
                i++;
            }
            conn.Close();
            return result;
        }

        internal static object[,] GetOrderIcecreamFromOptimum(int n)//получение заказов на мороженное из оптимум
        {
            int i = 0;
            string date = DateTime.Today.ToString("yyyyMMdd");
            string connString = Settings.Default.ConnStringOptimum;
            string sql = "SELECT  "
                        + "    'KPK'+CONVERT( VARCHAR,ord.orNumber)+CONVERT( VARCHAR,ord.MasterFID)+CONVERT( VARCHAR,ord.orID) AS 'Number', "
                        + "	   CONVERT( DATE,ord.orDate) AS 'DateDoc', "
                        + "	   CONVERT( DATE,ord.orShippingDate) AS 'DateExp', "
                        + "	   fcs.exID AS 'PtnCd', "
                        + "	   fcs.fShortName AS 'PtnName', "
                        + "	   fcs.fAddress AS 'PtnAddress', "
                        + "	   ord.orComment AS 'Comment', "
                        + "	   CONVERT(VARCHAR,ord.orNumber)+CONVERT(VARCHAR,ord.MasterFID)+CONVERT( VARCHAR,ord.orID) AS FileId, "
                        + "	   ord.orDate, "
                        + "	   ord.orID, "
                        + "	   ord.MasterFID "
                        + "FROM DS_Orders AS ord "
                        + "	   LEFT JOIN DS_Faces AS fcs ON fcs.fID = ord.mfID   "
                        + "WHERE ord.OrType = 0 "
                        + "AND ord.orDate BETWEEN DATEADD( DD,-1,'" + date + "') AND DATEADD( DD,1,'" + date + "' ) "
                        + "AND ord.Condition = 1 "
                        + "AND ord.fState = 0 "
                        + "AND LEFT( "
                        + "( SELECT TOP 1 igp.IgIdText "
                        + "  FROM DS_Orders_Items AS ordit "
                        + "       JOIN DS_Items AS itm ON ordit.iID = itm.iID "
                        + "       JOIN DS_UnitTypes AS utp ON ordit.unitID = utp.utID "
                        + "       JOIN DS_IGROUPS AS igp ON igp.igid = itm.it2ID "
                        + "  WHERE ordit.orItemsDate = ord.orDate "
                        + "        AND ordit.OrID = ord.OrId "
                        + "        AND ordit.Masterfid = ord.Masterfid),3 ) IN ('039','139') ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            //int n = dr.VisibleFieldCount;
            object[,] result = new object[n, 11];
            while (dr.Read())
            {
                result[i, 0] = dr.GetValue(0);
                result[i, 1] = dr.GetValue(1);
                result[i, 2] = dr.GetValue(2);
                result[i, 3] = dr.GetValue(3);
                result[i, 4] = dr.GetValue(4);
                result[i, 5] = dr.GetValue(5);
                result[i, 6] = dr.GetValue(6);
                result[i, 7] = dr.GetValue(7);
                result[i, 8] = dr.GetValue(8);
                result[i, 9] = dr.GetValue(9);
                result[i, 10] = dr.GetValue(10);
                i++;
            }
            conn.Close();
            return result;
        }

        internal static int CountMilk()//количество заказов на молоко
        {
            int i = 0, count;
            string date = DateTime.Today.ToString("yyyyMMdd");
            string connString = Settings.Default.ConnStringOptimum;
            string sql = "SELECT  count('KPK'+CONVERT( VARCHAR,ord.orNumber)+CONVERT( VARCHAR,ord.MasterFID)+CONVERT( VARCHAR,ord.orID)) AS 'Number' "
                        + "FROM DS_Orders AS ord "
                        + "	   LEFT JOIN DS_Faces AS fcs ON fcs.fID = ord.mfID   "
                        + "WHERE ord.OrType = 0 "
                        + "AND ord.orDate BETWEEN DATEADD( DD,-1,'" + date+"') AND DATEADD( DD,1,'"+date+"' ) "
                        + "AND ord.Condition = 1 "
                        + "AND ord.fState=0 "
                        + "AND LEFT( "
                        + "( SELECT TOP 1 igp.IgIdText "
                        + "  FROM DS_Orders_Items AS ordit "
                        + "       JOIN DS_Items AS itm ON ordit.iID = itm.iID "
                        + "       JOIN DS_UnitTypes AS utp ON ordit.unitID = utp.utID "
                        + "       JOIN DS_IGROUPS AS igp ON igp.igid = itm.it2ID "
                        + "  WHERE ordit.orItemsDate = ord.orDate "
                        + "        AND ordit.OrID = ord.OrId "
                        + "        AND ordit.Masterfid = ord.Masterfid),3 ) not in ('039', '139') ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())
            {
                result[i] = dr.GetValue(0);
                i++;
            }
            conn.Close();
            count = Convert.ToInt32(result[0]);
            return count;
        }

        internal static int CountIceCream()//количество заказов на мороженное
        {
            int i = 0, count;
            string date = DateTime.Today.ToString("yyyyMMdd");
            string connString = Settings.Default.ConnStringOptimum;
            string sql = "SELECT  count('KPK'+CONVERT( VARCHAR,ord.orNumber)+CONVERT( VARCHAR,ord.MasterFID)+CONVERT( VARCHAR,ord.orID)) AS 'Number' "
                        + "FROM DS_Orders AS ord "
                        + "	   LEFT JOIN DS_Faces AS fcs ON fcs.fID = ord.mfID   "
                        + "WHERE ord.OrType = 0 "
                        + "AND ord.orDate BETWEEN DATEADD( DD,-1,'" + date + "') AND DATEADD( DD,1,'" + date + "' ) "
                        + "AND ord.Condition = 1 "
                        + "AND ord.fState = 0 "
                        + "AND LEFT( "
                        + "( SELECT TOP 1 igp.IgIdText "
                        + "  FROM DS_Orders_Items AS ordit "
                        + "       JOIN DS_Items AS itm ON ordit.iID = itm.iID "
                        + "       JOIN DS_UnitTypes AS utp ON ordit.unitID = utp.utID "
                        + "       JOIN DS_IGROUPS AS igp ON igp.igid = itm.it2ID "
                        + "  WHERE ordit.orItemsDate = ord.orDate "
                        + "        AND ordit.OrID = ord.OrId "
                        + "        AND ordit.Masterfid = ord.Masterfid),3 ) IN ('039','139') ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())
            {
                result[i] = dr.GetValue(0);
                i++;
            }
            conn.Close();
            count = Convert.ToInt32(result[0]);
            return count;
        }

        internal static int CountItemsInOrderOptimum(string orItemsDate, string OrID, string Masterfid)//количество позиций в заказе из Optimum
        {
            int i = 0, count;
            string date = DateTime.Today.ToString("yyyyMMdd");
            string connString = Settings.Default.ConnStringOptimum;
            string sql = "select count(itm.iidText) "
                      + " from DS_Orders_Items AS ordit "
                      + "      JOIN DS_Items AS itm ON ordit.iID = itm.iID "
                      + "      JOIN DS_UnitTypes AS utp ON ordit.unitID = utp.utID "
                      + " where Convert(date,ordit.orItemsDate)= Convert(date,'" + orItemsDate + "') "
                      + "      and ordit.OrID = '" + OrID + "' "
                      + "      and ordit.Masterfid = '" + Masterfid + "' ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())
            {
                result[i] = dr.GetValue(0);
                i++;
            }
            conn.Close();
            count = Convert.ToInt32(result[0]);
            return count;
        }

        internal static object[,] GetitemsOptimum(string orItemsDate, string OrID, string Masterfid)//получение товарных позиций из Optimum
        {
            int i = 0;
            string date = DateTime.Today.ToString("yyyyMMdd");
            string connString = Settings.Default.ConnStringOptimum;
            string sql = "select itm.iidText AS 'ArtCd', "
                       + "	  itm.iName AS 'ArtName', "
                       + "	  case when utp.UnitName = 'кор' then 39 when utp.UnitName = 'шт' then 5 when utp.UnitName = 'кг' then 1 end AS 'EI', "
                       + "	  ordit.Amount AS 'Quantity' "
                       + "from DS_Orders_Items AS ordit "
                       + "      JOIN DS_Items AS itm ON ordit.iID = itm.iID "
                       + "      JOIN DS_UnitTypes AS utp ON ordit.unitID = utp.utID "
                       + "where Convert(date,ordit.orItemsDate)= Convert(date,'"+orItemsDate+"') "
                       + "      and ordit.OrID = '"+OrID+"' "
                       + "      and ordit.Masterfid = '"+Masterfid+"' ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            int m = CountItemsInOrderOptimum(orItemsDate, OrID, Masterfid);
            object[,] result = new object[m, n];
            while (dr.Read())
            {
                result[i, 0] = dr.GetValue(0);
                result[i, 1] = dr.GetValue(1);
                result[i, 2] = dr.GetValue(2);
                result[i, 3] = dr.GetValue(3);
                i++;
            }
            conn.Close();
            return result;
        }

        internal static int[] DistinctTypeSkln()//Проверка временной таблицы заказов на типы заказов (молоко, мороженной или оба) возвращает 5,6 или 5-6
        {
            string connString = Settings.Default.ConnStringISPRO;
            string CMDGetSF = "select distinct(TypeSkln) from U_Chtmpzkg ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(CMDGetSF, conn);
            SqlDataReader dr = command.ExecuteReader();
            int i = 0;
            int[] result = new int[i];
            while (dr.Read())
            {
                Array.Resize(ref result, result.Length + 1);
                result[i] = Convert.ToInt32(dr.GetValue(0));
                i++;
            }
            conn.Close();
            return result;
        }

        public static bool DeleteOrder(string NumberEDI)//возвращает true - удалено, false - не удалено, есть ордер.
        {
            int i = 0;
            bool result = false;
            string CheckconnString = Settings.Default.ConnStringISPRO;
            string Checksql = "select SklNk_Rcd from SKLNK where SklNk_RcdZkg = (SELECT PrdZkg_Rcd from PRDZKG where prdzkg_txt = '" + NumberEDI + "') and SklNk_Mov = 0 and SklNk_CdDoc = 46";
            SqlConnection Checkconn = new SqlConnection();
            Checkconn.ConnectionString = CheckconnString;
            Checkconn.Open();
            SqlCommand Checkcommand = new SqlCommand(Checksql, Checkconn);
            SqlDataReader Checkdr = Checkcommand.ExecuteReader();
            int n = Checkdr.VisibleFieldCount;
            object[] Checkresult = new object[n];
            while (Checkdr.Read())
            {
                Checkresult[i] = Checkdr.GetValue(0);
                i++;
            }
            Checkconn.Close();

            if ((Checkresult.Length == 0) || (Checkresult[0] == null))//нет накладной
            {
                string connString = Settings.Default.ConnStringISPRO;
                string sql = " DECLARE @number varchar(30) = '" + NumberEDI + "' "
                               + " if EXISTS(select * from PRDZKG where PrdZkg_Txt = @number) "
                               + " begin "
                               + " delete from ATrdsTax where ATrdSTax_RcdS in (select TrdS_Rcd from TRDS where TrdS_TypHdr=17 and TrdS_RcdHdr = (SELECT PrdZkg_Rcd from PRDZKG where prdzkg_txt = '" + NumberEDI + "'))  "
                               + " delete from TRDS where TrdS_TypHdr=17 and trds_mov=0 and  TrdS_RcdHdr = (SELECT PrdZkg_Rcd from PRDZKG where prdzkg_txt = '" + NumberEDI + "')  "
                               + " delete from PRDZKG where PrdZkg_Txt = '" + NumberEDI + "' "
                               + " end ";
                SqlConnection conn = new SqlConnection();
                conn.ConnectionString = connString;
                conn.Open();
                SqlCommand command = new SqlCommand(sql, conn);
                SqlDataReader dr = command.ExecuteReader();
                conn.Close();
                result = true;
            }
            return result;
        }

        public static string GetSkladId(string PtnGroup, string TypeSkln)
        {
            int i = 0;
            string SkladId;
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "  select Sklad_rcd from U_CHSETTJURORD "
                        + " left join JR on JR_Rcd = Jur_rcd "
                        + " where Code_GrPtnrk ='" + PtnGroup + "' and TypeSkln = " + TypeSkln + "";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())
            {
                result[i] = dr.GetValue(0);
                i++;
            }
            conn.Close();

            if (result[0] != null)
            {
                SkladId = Convert.ToString(result[0]);
            }
            else
            {
                int j = 0;
                string default_sql =  "select SkladId from U_CHSPTYPESKLN where TypeSkln = " + TypeSkln + "";
                SqlConnection default_conn = new SqlConnection();
                default_conn.ConnectionString = connString;
                conn.Open();
                SqlCommand default_command = new SqlCommand(default_sql, conn);
                SqlDataReader default_dr = default_command.ExecuteReader();
                int m = default_dr.VisibleFieldCount;
                object[] default_result = new object[m];
                while (default_dr.Read())
                {
                    default_result[j] = default_dr.GetValue(0);
                    j++;
                }
                conn.Close();
                SkladId = Convert.ToString(default_result[0]);
            }
            return SkladId;


        }

        public static string GetNameTypeSkln(string TypeSkln)
        {
            int i = 0;
            string Name;
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "select TypeDesc from U_CHSPTYPESKLN where TypeSkln = " + TypeSkln + "";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())
            {
                result[i] = dr.GetValue(0);
                i++;
            }
            conn.Close();

            if (result.Length > 0)
            {
                Name = Convert.ToString(result[0]);
            }
            else
            {
                Name = "NULL";
            }
            return Name;
        }

        public static object[,] GetItemFromPrevDoc(string DocRcd, string Barcode)//rcd документа, баркод продукции
        {
            int i = 0;
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select  BarCode_Code, "
                                   + "     case when LEFT( SklGr_CD, 3 )IN( '039', '139' ) then SUBSTRING(SklN_NmAlt,1,6) else SUBSTRING(SklN_NmAlt,1,4) end as Code, "
                                   + "	   SklN_Cd, "
                                   + "	   SklN_NmAlt, "
                                   + "	   Convert(decimal(18,3),TrdS_Qt), "
                                   + "	   Convert(decimal(18,2),((trds_sumopl-trds_sumtax)/TrdS_QtCn)), "
                                   + "	   Convert(decimal(18,2),trds_sumopl/TrdS_Qt), "
                                   + "	   case when EI_Rcd=1 then 'KG' when EI_Rcd=5 then 'PCE' when EI_Rcd=39 then 'CT' end, "
                                   + "	   EI_OKEI, "
                                   + "	   Convert(decimal(18,2),TaxRate_Val), "
                                   + "	   'S', "
                                   + "	   Convert(decimal(18,2),TrdS_SumTax), "
                                   + "     CONVERT( DECIMAL(18, 2), (trds_sumopl-trds_sumtax)/TrdS_Qt ) * CONVERT( DECIMAL(18, 3), TrdS_Qt ), "
                                   + "	   CONVERT( DECIMAL(18, 2), trds_sumopl ) "
                                   + "from TRDS  "
                                   + "        LEFT JOIN SKLN AS Nom ON Nom.SklN_Rcd = TrdS_RcdNom "
                                   + "        LEFT JOIN BarCode bc on bc.BarCode_RcdNom = Nom.SklN_Rcd AND BARCODE_Base = 1 "
                                   + "        LEFT JOIN EI on TrdS_EiQt = ei_rcd "
                                   + "        LEFT JOIN SKLGR on SklN_RcdGrp = SklGr_Rcd "
                                   + "        LEFT JOIN (ATRDSTAX ATT1 left join TAXRATE TR1 on TR1.TaxRate_Rcd = ATT1.TrdSTax_RateCd) on TrdS_Rcd = ATT1.ATrdSTax_RcdS and ATT1.TrdSTax_Cd = 1 "
                                   + " where trds_rcdhdr = '" + DocRcd + "'  and BarCode_Code = '"+Barcode+"'";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[,] result = new object[1, 14];
            while (dr.Read())
            {
                try
                {
                    result[i, 0] = dr.GetValue(0);
                    result[i, 1] = dr.GetValue(1);
                    result[i, 2] = dr.GetValue(2);
                    result[i, 3] = dr.GetValue(3);
                    result[i, 4] = dr.GetValue(4);
                    result[i, 5] = dr.GetValue(5);
                    result[i, 6] = dr.GetValue(6);
                    result[i, 7] = dr.GetValue(7);
                    result[i, 8] = dr.GetValue(8);
                    result[i, 9] = dr.GetValue(9);
                    result[i, 10] = dr.GetValue(10);
                    result[i, 11] = dr.GetValue(11);
                    result[i, 12] = dr.GetValue(12);
                    result[i, 13] = dr.GetValue(13);
                    //i++;
                }
                catch (IOException e)
                {
                    string error = "Ошибка в процедуре GetItemsFromTrdS. Ошибка получении товарных позиций. TRDS-RCDHDR: " + DocRcd;
                    Program.WriteLine(error);
                    WriteErrorLog(error);
                    WriteErrorLog(e.Message);
                    WriteErrorLog(e.Source);
                }
            }
            conn.Close();
            return result;
        }

        public static void CheckBuyerCode(string BuyerCode,string RosMolCode,string PtnCdBuyer)
        {
            object[] PtnInfo = Verifiacation.GetDataFromPtnCD(PtnCdBuyer);
            string CdSpr = Convert.ToString(PtnInfo[5]);
            string Code = RosMolCode.Substring(0, RosMolCode.IndexOf(" "));
            object[] BICode = Verifiacation.GetBuyerItemCode(CdSpr, Code);
            
            string CurrentValueCode = Convert.ToString(BICode[0]);
            if (BuyerCode != CurrentValueCode) //если код продукции контрагента в xml другой, то обновляем значение в базе
            {
                string connString = Settings.Default.ConnStringISPRO;
                string sql = "begin tran "
                           + " IF Exists (select * from UFSpr where UFS_Rcd = (SELECT top 1 UFLstSpr.UFS_Rcd FROM UFLstSpr JOIN UFSpr ON UFSpr.UFS_Rcd = UFLstSpr.UFS_Rcd WHERE UFS_CdSpr = '" + CdSpr + "') and UFS_CdS = '" + Code + "') "
                           + " begin "
                           + "    update UFSpr set UFS_NmK='" + BuyerCode + "',UFS_Nm='" + BuyerCode + "' where UFS_CdS = '" + Code + "' and UFS_Rcd = (SELECT top 1 UFLstSpr.UFS_Rcd FROM UFLstSpr JOIN UFSpr ON UFSpr.UFS_Rcd = UFLstSpr.UFS_Rcd WHERE UFS_CdSpr = '" + CdSpr + "' )"
                           + " end "
                           + " else "
                           + " begin "
                           + "    insert into UFSpr (UFS_Rcd,UFS_CdS,UFS_NmK,UFS_Nm) values ((SELECT top 1 UFLstSpr.UFS_Rcd FROM UFLstSpr WHERE UFS_CdSpr = '" + CdSpr + "'),'" + Code + "','" + BuyerCode + "','" + BuyerCode + "') "      // убрал JOIN UFSpr ON UFSpr.UFS_Rcd = UFLstSpr.UFS_Rcd (при первой вставке не сработает)
                           + " end "
                           + " commit tran ";
                SqlConnection conn = new SqlConnection();
                conn.ConnectionString = connString;
                conn.Open();
                SqlCommand command = new SqlCommand(sql, conn);
                SqlDataReader dr = command.ExecuteReader();
                conn.Close();
            }


        }

        internal static void ClearTmpDbf()
        {
            string connString = Settings.Default.ConnStringISPRO;
            string Insert = "delete from U_Chtmpdbf";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(Insert, conn);
            SqlDataReader dr = command.ExecuteReader();
            conn.Close();
        }

        public static string[] GetNumZakFromDbf()
        {
            int i = 0;
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select distinct(NUMZAK) from U_CHTMPDBF";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            string[] result = new string[i];
            while (dr.Read())
            {
                Array.Resize(ref result, result.Length + 1);
                result[i] = Convert.ToString(dr.GetValue(0));
                i++;
            }
            conn.Close();
            return result;
        }

        public static object[,] GetItemsFromTMPDBF(string NumZak)
        {
            int i = 0;
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "select GplCode,Art,ArtName,EI,QT,DateZak,DateOtg,DogRcd /*рсд договора*/ from U_CHTMPDBF where NUMZAK = " + NumZak + "";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            int m = CountItemsInOrderDBF(NumZak);
            object[,] result = new object[m, 8];
            while (dr.Read())
            {
                try
                {
                    result[i, 0] = dr.GetValue(0);
                    result[i, 1] = dr.GetValue(1);
                    result[i, 2] = dr.GetValue(2);
                    result[i, 3] = dr.GetValue(3);
                    result[i, 4] = dr.GetValue(4);
                    result[i, 5] = dr.GetValue(5);
                    result[i, 6] = dr.GetValue(6);
                    result[i, 7] = dr.GetValue(7);
                    i++;
                }
                catch (IOException e)
                {
                    string error = "Ошибка в процедуре GetItemFromTMPDBF";
                    Program.WriteLine(error);
                    WriteErrorLog(error);
                    WriteErrorLog(e.Message);
                    WriteErrorLog(e.Source);
                }
            }
            conn.Close();
            return result;
        }

        internal static int CountItemsInOrderDBF(string NumZak)//количество позиций в заказе из DBF
        {
            int i = 0, count;
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "select count(*) from U_CHTMPDBF where NUMZAK = " + NumZak + "";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())
            {
                result[i] = dr.GetValue(0);
                i++;
            }
            conn.Close();
            count = Convert.ToInt32(result[0]);
            return count;
        }

        internal static int CountUPD()
        {
            int i = 0, count;
            string connString = Settings.Default.ConnStringISPRO;
            string CMDGetSF = "select COUNT (*) from U_vwChListUPDForSent " //сделал вьюшку
                           + " where  IsProDoc is NULL and "
                           + " ((SklSfA_RcdCor=0 and SklSf_Dt between DATEADD(dd,-(ISNULL((select top 1 [Dn_Vz] from U_CHEDINSTSF),10)),CONVERT(date,getdate())) and CONVERT(date,getdate())) "
                           + " or "
                           + " (SklSfA_RcdCor!=0 and SklSf_Dt between DATEADD(dd,-(ISNULL((select top 1 [Dn_Vz_K] from U_CHEDINSTSF),10)),CONVERT(date,getdate())) and CONVERT(date,getdate()))) "
                           + " and ((SklSfA_RcdCor=0 and   dateadd(dd,ISNULL((select top 1 [Dn_Zd] from U_CHEDINSTSF),0),SklSf_Dt)<=CONVERT(date,getdate())) or (SklSfA_RcdCor!=0 and  dateadd(dd,ISNULL((select top 1 [Dn_Zd_K] from U_CHEDINSTSF),0),SklSf_Dt)<=CONVERT(date,getdate()))) ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(CMDGetSF, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())
            {
                result[i] = dr.GetValue(0);
                i++;
            }
            conn.Close();
            count = Convert.ToInt32(result[0]);
            return count;
        }

        internal static object[,] GetListUPD(int count)//получение списка УПД/УКД на отправку с учетом времени отсрочки [14]-признак ксф, если !=0 то ксф
        {
            int i = 0;
            string connString = Settings.Default.ConnStringISPRO;
            string CMDGetSF = " select * from U_vwChListUPDForSent "//сделал вьюшку
                            + " where  IsProDoc is NULL and "
                            + " ((SklSfA_RcdCor=0 and SklSf_Dt between DATEADD(dd,-(ISNULL((select top 1 [Dn_Vz] from U_CHEDINSTSF),10)),CONVERT(date,getdate())) and CONVERT(date,getdate())) "
                            + " or "
                            + " (SklSfA_RcdCor!=0 and SklSf_Dt between DATEADD(dd,-(ISNULL((select top 1 [Dn_Vz_K] from U_CHEDINSTSF),10)),CONVERT(date,getdate())) and CONVERT(date,getdate()))) "
                            + " and ((SklSfA_RcdCor=0 and   dateadd(dd,ISNULL((select top 1 [Dn_Zd] from U_CHEDINSTSF),0),SklSf_Dt)<=CONVERT(date,getdate())) or (SklSfA_RcdCor!=0 and  dateadd(dd,ISNULL((select top 1 [Dn_Zd_K] from U_CHEDINSTSF),0),SklSf_Dt)<=CONVERT(date,getdate()))) "
                            +"  order by SklSf_Dt";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(CMDGetSF, conn);
            SqlDataReader dr = command.ExecuteReader();
            object[,] result = new object[count, 23];
            while (dr.Read())
            {
                result[i, 0] = dr.GetValue(0);
                result[i, 1] = dr.GetValue(1);
                result[i, 2] = dr.GetValue(2);
                result[i, 3] = dr.GetValue(3);
                result[i, 4] = dr.GetValue(4);
                result[i, 5] = dr.GetValue(5);
                result[i, 6] = dr.GetValue(6);
                result[i, 7] = dr.GetValue(7);
                result[i, 8] = dr.GetValue(8);
                result[i, 9] = dr.GetValue(9);
                result[i, 10] = dr.GetValue(10);
                result[i, 11] = dr.GetValue(11);
                result[i, 12] = dr.GetValue(12);
                result[i, 13] = dr.GetValue(13);
                result[i, 14] = dr.GetValue(14);
                result[i, 15] = dr.GetValue(15);
                result[i, 16] = dr.GetValue(16);
                result[i, 17] = dr.GetValue(17);
                result[i, 18] = dr.GetValue(18);
                result[i, 19] = dr.GetValue(19);
                result[i, 20] = dr.GetValue(20);
                result[i, 21] = dr.GetValue(21);
                result[i, 22] = dr.GetValue(22);
                i++;
            }
            conn.Close();
            return result;
        }

        /*
         * typeFunc = "" - Это УПД с функциями СЧФДОП и СЧФ
         * typeFunc = "ДОП" - Это УПД с функцией ДОП
         * */
        internal static object[,] GetListUPDN(string typeFunc = "")//получение списка УПД/УКД на отправку с учетом времени отсрочки
        {
            int i = 0;
            //count = 2;//test
            string connString = Settings.Default.ConnStringISPRO;
            string CMDGetSF;
            object[,] result = null;
            object[] resultRow = null;
            List<object[]> resultList = new List<object[]>();

            switch (typeFunc)
            {
                case "SVOD": CMDGetSF = "SELECT ProviderOpt, ProviderZkg, NastDoc_Fmt, SklSf_Rcd, SklSf_TpOtg, SklSfA_RcdCor, Expr1, typeSf FROM U_vwChListUPDForSentNSvod WHERE ISNULL(ProviderZkg,'') <> '' AND ISNULL(NastDoc_Fmt,'') <> ''";
                    break;
                case "ДОП": CMDGetSF = "SELECT ProviderOpt, ProviderZkg, NastDoc_Fmt, SklSf_Rcd, SklSf_TpOtg, SklSfA_RcdCor, PrdZkg_NmrExt, PrdZkg_Rcd, PrdZkg_Dt, SklNk_TDrvNm, typeSf, NISF, sklnkDat, sklnkNmr, dtOtgr FROM U_vwChListUPDDOPForSent WHERE ISNULL(ProviderZkg,'') <> '' AND ISNULL(NastDoc_Fmt,'') <> ''";
                    break;
                default: CMDGetSF = "SELECT ProviderOpt, ProviderZkg, NastDoc_Fmt, SklSf_Rcd, SklSf_TpOtg, SklSfA_RcdCor, PrdZkg_NmrExt, PrdZkg_Rcd, PrdZkg_Dt, SklNk_TDrvNm, typeSf, NISF, sklnkDat, sklnkNmr, dtOtgr FROM U_vwChListUPDForSentN WHERE ISNULL(ProviderZkg,'') <> '' AND ISNULL(NastDoc_Fmt,'') <> ''";
                    break;
            }

            /*if (typeFunc == "")
                CMDGetSF = "SELECT ProviderOpt, ProviderZkg, NastDoc_Fmt, SklSf_Rcd, SklSf_TpOtg, SklSfA_RcdCor, PrdZkg_NmrExt, PrdZkg_Rcd, PrdZkg_Dt, SklNk_TDrvNm, typeSf, NISF, sklnkDat, sklnkNmr, dtOtgr FROM U_vwChListUPDForSentN WHERE ISNULL(ProviderZkg,'') <> '' AND ISNULL(NastDoc_Fmt,'') <> ''";
            else
                CMDGetSF = "SELECT ProviderOpt, ProviderZkg, NastDoc_Fmt, SklSf_Rcd, SklSf_TpOtg, SklSfA_RcdCor, PrdZkg_NmrExt, PrdZkg_Rcd, PrdZkg_Dt, SklNk_TDrvNm, typeSf, NISF, sklnkDat, sklnkNmr, dtOtgr FROM U_vwChListUPDDOPForSent WHERE ISNULL(ProviderZkg,'') <> '' AND ISNULL(NastDoc_Fmt,'') <> ''";
            */

            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            try
            {
                conn.Open();

                SqlCommand command = new SqlCommand(CMDGetSF, conn);
                SqlDataReader dr = command.ExecuteReader();

                //int recordCount = 0;
                int fieldCount = dr.FieldCount;

                while (dr.Read())
                {
                    resultRow = new object[fieldCount];
                    for (int j = 0; j < fieldCount; j++) resultRow[j] = dr.GetValue(j);
                    resultList.Add(resultRow);
                }

                if (resultList.Count > 0)
                {
                    result = new object[resultList.Count, fieldCount];
                    for (i = 0; i < resultList.Count; i++)
                    {
                        for (int j = 0; j < fieldCount; j++) result[i, j] = resultList[i][j];
                    }
                }
                else result = new object[0,0];

                
                
                /*if (dr.Read())
                {
                    recordCount = dr.VisibleFieldCount;
                    result = new object[recordCount, dr.FieldCount];
                    dr.GetValues(result);
                    while (dr.Read())
                    {
                        for (int j = 0; j < dr.FieldCount; j++) result[i, j] = dr.GetValue(j);
                        i++;
                    }
                }*/

                /*while (dr.Read()) recordCount++;
                result = new object[recordCount, dr.FieldCount];

                dr.Close();
                dr = command.ExecuteReader();
                

                while (dr.Read())
                {
                    for (int j = 0; j < dr.FieldCount; j++) result[i, j] = dr.GetValue(j);
                    i++;
                }*/


                conn.Close();
            }
            catch (Exception e)
            {
                Program.WriteLine("Ошибка соединения с БД " + Convert.ToString(e));
                if (conn.State == ConnectionState.Open) conn.Close();
                result = new object[0, 0];
            }
           
            return result;
        }

        /*
           Метод заносит информацию об обработанном файле
           string TypeDoc - тип документа - ORDERS, ORDERSSP, DESADV, RECADV, INVOICE, ON_SCHFDOPPR, ON_KORSCHFDOPPR 
           datetime RecDate - дата обработки файла 
           string NameF - наименование файла
           string DNumber - номер документа
           string OrdNumber - номер заказа
           string Cmt - комментарий
           string SenderILN - Gln отправителя
           string SenderName - наименование отправителя
           string BuyerGLN - GLN плательщика
           string BuyerName - наименование плательщика
           string DeliveryGLN - GLN грузополучателя
           string DeliveryName - наименование грузополучателя
           string DeliveryAddress - адрес грузополучателя
           string Provider - провайдер
           int Success - обработан файл или прервался на ошибке (1 - успех/ 0 - провален)
        */
        internal static void WriteEDIProcessedFile(string TypeDoc, DateTime RecDate, string DNumber, string OrdNumber, string NameF, string Cmt, string SenderILN, string SenderName, string BuyerGLN, string BuyerName, string DeliveryGLN, string DeliveryName, string DeliveryAddress, string Provider, int Success)
        {
            string connString = Settings.Default.ConnStringISPRO;
            string sql = " INSERT INTO dbo.U_MgEdiPrFile "
                       + " (ProcesF_TypeDoc, ProcesF_RecDate, ProcesF_DNumber, ProcesF_OrdNumber, ProcesF_NameF, ProcesF_Cmt, ProcesF_SenderILN, "
                       + " ProcesF_SenderName, ProcesF_BuyerGLN, ProcesF_BuyerName, ProcesF_DeliveryGLN, ProcesF_DeliveryName, "
                       + " ProcesF_DeliveryAddress, ProcesF_Provider, ProcesF_Success) "
                       + " VALUES('" + TypeDoc + "','" + RecDate.ToString("yyyyMMdd HH:mm:ss") + "','" + DNumber + "', '" + OrdNumber + "', '"+NameF.Substring(0, Math.Min(NameF.Length,199)) + "', '" + Cmt.Substring(0, Math.Min(Cmt.Length, 99)) + "', " + SenderILN
                       + " , '" + SenderName.Substring(0, Math.Min(SenderName.Length, 149)) + "'," + BuyerGLN + ", '" + BuyerName.Substring(0, Math.Min(BuyerName.Length, 149)) + "', " + DeliveryGLN + ", '" + DeliveryName.Substring(0, Math.Min(DeliveryName.Length, 149)) 
                       + "', '" + DeliveryAddress.Substring(0, Math.Min(DeliveryAddress.Length, 199)) + "' ,'" + Provider.Substring(0, Math.Min(Provider.Length, 20)) + "', " + Success.ToString() + ")";

            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            try
            {
                SqlDataReader dr = command.ExecuteReader();
            }
            catch (IOException e)
            {
                string error = "Ошибка в процедуре WriteEDIProcessedFile. Добавление записи в таблицу U_MgEdiPrFile";
                Program.WriteLine(error);
                WriteErrorLog(error);
                WriteErrorLog(e.Message);
                WriteErrorLog(e.Source);
            }

            conn.Close();
        }

        /*
           Метод перемещает файл из одной папки в другую
           string file - файл, который нужно переместить
           string NewFolder - куда поместить  
        */
        internal static bool MoveToFolder(string  parsefile, string NewFolder)
        {
            //Проверим существует ли файл и новая директория
            if (File.Exists(parsefile) && Directory.Exists(NewFolder))
            {
                //Получим полный путь до файла
                string oldPatch = Path.GetFullPath(parsefile);
                //Последним символов в NewFolder должен быть \
                if(NewFolder.Substring(NewFolder.Length - 1, 1) != "\\")
                    NewFolder += "\\";
                string newPatch = NewFolder + Path.GetFileName(parsefile);
                try
                {
                    Directory.Move(oldPatch, newPatch);
                }
                catch //не смогли переместить, возможно уже есть с таким именем файл в данной директории
                {
                    string id = Convert.ToString(Guid.NewGuid());
                    string ReserveNewP = NewFolder + DateTime.Now.ToString("@yyyyMMdd_HHmmss_") + id + "_" + Path.GetFileName(parsefile);
                    Directory.Move(oldPatch, ReserveNewP);
                }

                return true;
            }
            else return false;

        }

        public static object[,] GetItemsFromInvoiceOld(string DocRcd, int cnt, bool PCE)//rcd документа, количество позиций, ,bool PCE - маркер ЕИ штука.
        {
            int i = 0;
            string verification;
            string connString = Settings.Default.ConnStringISPRO;
            if (PCE == true) //надо все в штуках
            {
                verification = "select  BarCode_Code, "
                                + "case when LEFT( SklGr_CD, 3 )IN( '039', '139' ) then SUBSTRING(SklN_NmAlt,1,6) else SUBSTRING(SklN_NmAlt,1,4) end as Code, "
                                + "SklN_Cd, "
                                + "SklN_NmAlt, "
                                + "case when EI_Rcd=39 then Convert(int,EIBox.NmEi_QtNett/EISht.NmEi_QtNett)*TrdS_Qt when EI_Rcd=5 then Convert(decimal(18,3),TrdS_Qt) when EI_Rcd=1 then Convert(int,EIKG.NmEi_QtNett/EISht.NmEi_QtNett) end,"
                                + "Convert(decimal(18,2),((trds_sumopl-trds_sumtax)/(case when EI_Rcd=39 then Convert(int,EIBox.NmEi_QtNett/EISht.NmEi_QtNett)*TrdS_Qt when EI_Rcd=5 then Convert(decimal(18,3),TrdS_Qt) when EI_Rcd=1 then Convert(int,EIKG.NmEi_QtNett/EISht.NmEi_QtNett) end))), "
                                + "Convert(decimal(18,2),trds_sumopl/(case when EI_Rcd=39 then Convert(int,EIBox.NmEi_QtNett/EISht.NmEi_QtNett)*TrdS_Qt when EI_Rcd=5 then Convert(decimal(18,3),TrdS_Qt) when EI_Rcd=1 then Convert(int,EIKG.NmEi_QtNett/EISht.NmEi_QtNett) end)), "
                                + "'PCE', "
                                + "796, "
                                + " 'шт', "
                                + "Convert(decimal(18,2),TaxRate_Val), "
                                + "'S', "
                                + "Convert(decimal(18,2),TrdS_SumTax), "
                                + "CONVERT( DECIMAL(18, 2), (trds_sumopl-trds_sumtax)/TrdS_Qt ) * CONVERT( DECIMAL(18, 3), TrdS_Qt ), "
                                + "CONVERT( DECIMAL(18, 2), trds_sumopl ) "
                                + "from TRDS  "
                                + "LEFT JOIN SKLN AS Nom ON Nom.SklN_Rcd = TrdS_RcdNom "
                                + "LEFT JOIN BarCode bc on bc.BarCode_RcdNom = Nom.SklN_Rcd AND BARCODE_Base = 1 "
                                + "LEFT JOIN EI on TrdS_EiQt = ei_rcd "
                                + "LEFT JOIN SKLGR on SklN_RcdGrp = SklGr_Rcd "
                               // + "LEFT JOIN SKLNOMTAX AS TAX ON TAX.NmTax_RcdPar = Nom.SklN_Rcd "
                               // + "LEFT JOIN TAXRATE AS NDS ON NDS.TaxRate_Rcd = TAX.NmTax_CdRate "
                                + "LEFT JOIN (ATRDSTAX ATT1 left join TAXRATE TR1 on TR1.TaxRate_Rcd = ATT1.TrdSTax_RateCd) on TrdS_Rcd = ATT1.ATrdSTax_RcdS and ATT1.TrdSTax_Cd = 1 "
                                + "LEFT JOIN SKLNOMEI AS EISht ON Nom.SklN_Rcd = EISht.NmEi_RcdNom AND EISht.NmEi_Cd = 5 AND EISht.NmEi_Osn<>1"
                                + "LEFT JOIN SKLNOMEI AS EIBox ON Nom.SklN_Rcd = EIBox.NmEi_RcdNom AND EIBox.NmEi_Cd = 39 "
                                + "LEFT JOIN SKLNOMEI AS EIKG ON Nom.SklN_Rcd = EIBox.NmEi_RcdNom AND EIBox.NmEi_Cd = 1  "
                                + " where trds_rcdhdr = '" + DocRcd + "' and TrdS_TypHdr=5 ";
            }
            else
                verification = "select  BarCode_Code, "
                                       + "     case when LEFT( SklGr_CD, 3 )IN( '039', '139' ) then SUBSTRING(SklN_NmAlt,1,6) else SUBSTRING(SklN_NmAlt,1,4) end as Code, "
                                       + "	   SklN_Cd, "
                                       + "	   SklN_NmAlt, "
                                       + "	   Convert(decimal(18,3),TrdS_Qt), "
                                       + "	   Convert(decimal(18,2),((trds_sumopl-trds_sumtax)/TrdS_QtCn)), "
                                       + "	   Convert(decimal(18,2),trds_sumopl/TrdS_Qt), "
                                       + "	   case when EI_Rcd=1 then 'KG' when EI_Rcd=5 then 'PCE' when EI_Rcd=39 then 'CT' end, "
                                       + "	   EI_OKEI, "
                                       + "     EI_ShNm, "
                                       + "	   Convert(decimal(18,2),TaxRate_Val), "
                                       + "	   'S', "
                                       + "	   Convert(decimal(18,2),TrdS_SumTax), "
                                       + "     CONVERT( DECIMAL(18, 2), (trds_sumopl-trds_sumtax)/TrdS_Qt ) * CONVERT( DECIMAL(18, 3), TrdS_Qt ), "
                                       + "	   CONVERT( DECIMAL(18, 2), trds_sumopl ) "
                                       + "from TRDS  "
                                       + "        LEFT JOIN SKLN AS Nom ON Nom.SklN_Rcd = TrdS_RcdNom "
                                       + "        LEFT JOIN BarCode bc on bc.BarCode_RcdNom = Nom.SklN_Rcd AND BARCODE_Base = 1 "
                                       + "        LEFT JOIN EI on TrdS_EiQt = ei_rcd "
                                       + "        LEFT JOIN SKLGR on SklN_RcdGrp = SklGr_Rcd "
                                       + "        LEFT JOIN (ATRDSTAX ATT1 left join TAXRATE TR1 on TR1.TaxRate_Rcd = ATT1.TrdSTax_RateCd) on TrdS_Rcd = ATT1.ATrdSTax_RcdS and ATT1.TrdSTax_Cd = 1 "
                                       + " where trds_rcdhdr = '" + DocRcd + "' and TrdS_TypHdr=5";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[,] result = new object[cnt, 15];
            while (dr.Read())
            {
                try
                {
                    result[i, 0] = dr.GetValue(0);
                    result[i, 1] = dr.GetValue(1);
                    result[i, 2] = dr.GetValue(2);
                    result[i, 3] = dr.GetValue(3);
                    result[i, 4] = dr.GetValue(4);
                    result[i, 5] = dr.GetValue(5);
                    result[i, 6] = dr.GetValue(6);
                    result[i, 7] = dr.GetValue(7);
                    result[i, 8] = dr.GetValue(8);
                    result[i, 9] = dr.GetValue(9);
                    result[i, 10] = dr.GetValue(10);
                    result[i, 11] = dr.GetValue(11);
                    result[i, 12] = dr.GetValue(12);
                    result[i, 13] = dr.GetValue(13);
                    result[i, 14] = dr.GetValue(14);
                    i++;
                }
                catch (IOException e)
                {
                    string error = "Ошибка в процедуре GetItemsFromTrdS. Ошибка получении товарных позиций. TRDS-RCDHDR: " + DocRcd;
                    Program.WriteLine(error);
                    WriteErrorLog(error);
                    WriteErrorLog(e.Message);
                    WriteErrorLog(e.Source);
                }

            }
            conn.Close();
            return result;
        }

        public static object[,] GetItemsFromInvoice(string DocRcd, int cnt, bool PCE)//rcd документа, количество позиций, ,bool PCE - маркер ЕИ штука.
        {
            int i = 0;
            string verification;
            string connString = Settings.Default.ConnStringISPRO;
            if (PCE == true) //надо все в штуках
            {
                verification = "SELECT ISNULL((SELECT TOP 1 BarCode_Code FROM BarCode WHERE BarCode_RcdNom = Nom.SklN_Rcd ORDER BY BARCODE_Base DESC),(SELECT TOP 1 BR.TrdS_BarCode FROM TRDS BR WITH (NOLOCK) WHERE BR.TrdS_BarCode <> '' AND BR.TrdS_TypHdr = 1  AND BR.TrdS_RcdNom = SklN_Rcd AND BR.TrdS_TypPar IN (0,17))) BarCode_Code --0\n"
                                + "     , CASE WHEN LEFT(SklGr_CD, 3) IN ( '039', '139' ) THEN SUBSTRING(skln_nm, 1, 6) ELSE SUBSTRING(skln_nm, 1, 4) END AS Cod --1\n"
                                + "     , SklN_Cd --2\n"
                                + "     , SklN_Nm --3\n"
                                + "	 , TrdS_Qt * (TrdS_QtOsn/TrdS_Qt)/EISht.NmEi_QtOsn  --4 кол-во\n"
                                + "     , CONVERT( DECIMAL(18, 2), (TrdS_Cn * EISht.NmEi_QtOsn / TrdS_QtOsn * TrdS_Qt) / ( 1 + CAST( RTRIM( LTRIM( NDS.TaxRate_Val ))AS FLOAT ) / 100 )) --5 Цена без НДС\n"
                                + "     , CONVERT( DECIMAL(18, 2), (TrdS_Cn * EISht.NmEi_QtOsn / TrdS_QtOsn * TrdS_Qt))  --6 Цена с НДС\n"
                                + "     , 'PCE' --7\n"
                                + "     , EI_OKEI --8\n"
                                + "     , CONVERT( DECIMAL(18, 2), TaxRate_Val) --9\n"
                                + "     , 'S' --10\n"
                                + "     , CONVERT( DECIMAL(18, 2), TrdS_SumTax) --11\n"
                                + "     , CONVERT( DECIMAL(18, 2), (trds_sumopl - trds_sumtax)) --12\n"
                                + "     , /*CONVERT( DECIMAL(18, 2), TrdS_SumExt) * CONVERT(DECIMAL(18, 3), TrdS_Qt)*/ 0 --13\n"
                                + "     , TrdS_QtOsn --14\n"        // количество в основной ЕИ, (КГ наверное) для садиков...
                                + "FROM TRDS\n"
                                + "     JOIN SKLN AS Nom ON Nom.SklN_Rcd = TrdS_RcdNom\n"
                                //+ "     LEFT JOIN BarCode AS bc ON bc.BarCode_RcdNom = Nom.SklN_Rcd AND BARCODE_Base = 1\n"
                                + "     LEFT JOIN EI ON EI_Rcd = 5\n"
                                + "     LEFT JOIN SKLGR ON SklN_RcdGrp = SklGr_Rcd\n"
                                + "     LEFT JOIN ATRDSTAX AS TAX ON Tax.ATrdSTax_RcdS = TRDS.TrdS_Rcd AND Tax.ATrdSTax_RcdAs = 0\n"
                                + "     LEFT JOIN DBO.TAXRATE AS NDS ON NDS.TaxRate_Rcd = Tax.TrdSTax_RateCd AND NDS.TaxRate_RcdTax = Tax.TrdSTax_Cd\n"
                                + "     LEFT JOIN SKLNOMEI AS EISht ON Nom.SklN_Rcd = EISht.NmEi_RcdNom AND EISht.NmEi_Cd = 5\n"
                                + "     --LEFT JOIN SKLNOMEI AS EIBox ON Nom.SklN_Rcd = EIBox.NmEi_RcdNom AND EIBox.NmEi_Cd = 39\n"
                                + "     --LEFT JOIN SKLNOMEI AS EIKG ON Nom.SklN_Rcd = EIBox.NmEi_RcdNom AND EIBox.NmEi_Cd = 1\n"
                                + "WHERE trds_rcdhdr = " + DocRcd + "\n"
                                + "    AND TrdS_TypHdr = 5;";
            }
            else
                verification = "SELECT ISNULL((SELECT TOP 1 BarCode_Code FROM BarCode WHERE BarCode_RcdNom = Nom.SklN_Rcd ORDER BY BARCODE_Base DESC),(SELECT TOP 1 BR.TrdS_BarCode FROM TRDS BR WITH (NOLOCK) WHERE BR.TrdS_BarCode <> '' AND BR.TrdS_TypHdr = 1  AND BR.TrdS_RcdNom = SklN_Rcd AND BR.TrdS_TypPar IN (0,17))) BarCode_Code --0\n"
                                + "     , CASE WHEN LEFT(SklGr_CD, 3) IN ( '039', '139' ) THEN SUBSTRING(skln_nm, 1, 6) ELSE SUBSTRING(skln_nm, 1, 4)  END AS Code --1\n"
                                + "     , SklN_Cd --2\n"
                                + "     , SklN_Nm --3\n"
                                + "     , CONVERT( DECIMAL(18, 3), TrdS_Qt) --4\n"
                                + "	 , CONVERT( DECIMAL(18, 2), TrdS_Cn / ( 1 + CAST( RTRIM( LTRIM( NDS.TaxRate_Val ))AS FLOAT ) / 100 )) --5 Цена без НДС\n"
                                + "     , CONVERT( DECIMAL(18, 2), TrdS_Cn) --6\n"
                                + "     , CASE WHEN EI_Rcd = 1 THEN 'KG' WHEN EI_Rcd = 5 THEN 'PCE' WHEN EI_Rcd = 39 THEN 'CT' END --7\n"
                                + "     , EI_OKEI --8\n"
                                + "     , CONVERT( DECIMAL(18, 2), TaxRate_Val) --9\n"
                                + "     , 'S' --10\n"
                                + "     , CONVERT( DECIMAL(18, 2), TrdS_SumTax) --1\n"
                                + "     , CONVERT( DECIMAL(18, 2), trds_sumopl - trds_sumtax) --12\n"
                                + "     , 0 /*CONVERT( DECIMAL(18, 2), TrdS_SumExt) * CONVERT(DECIMAL(18, 3), TrdS_Qt)*/ --13\n"
                                + "     , TrdS_QtOsn --14\n"        // количество в основной ЕИ, (КГ наверное) для садиков...
                                + "FROM TRDS\n"
                                + "     JOIN SKLN AS Nom ON Nom.SklN_Rcd = TrdS_RcdNom\n"
                                //+ "     LEFT JOIN BarCode AS bc ON bc.BarCode_RcdNom = Nom.SklN_Rcd AND BARCODE_Base = 1\n"
                                + "     LEFT JOIN EI ON TrdS_EiQt = ei_rcd\n"
                                + "     LEFT JOIN SKLGR ON SklN_RcdGrp = SklGr_Rcd\n"
                                + "	    LEFT JOIN DBO.ATRDSTAX AS Tax ON Tax.ATrdSTax_RcdS = TRDS.TrdS_Rcd AND Tax.ATrdSTax_RcdAs = 0\n"
                                + "     LEFT JOIN DBO.TAXRATE AS NDS ON NDS.TaxRate_Rcd = Tax.TrdSTax_RateCd AND NDS.TaxRate_RcdTax = Tax.TrdSTax_Cd \n"
                                + "WHERE trds_rcdhdr = " + DocRcd + "\n"
                                + "      AND TrdS_TypHdr = 5;";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[,] result = new object[cnt, 15];
            while (dr.Read())
            {
                try
                {
                    result[i, 0] = dr.GetValue(0);
                    if (Convert.ToString(result[i, 0]).Length == 0)//баркода нет - ищеи альтернативу
                    {
                        result[i, 0] = DispOrders.GetAltBarCode(Convert.ToString(dr.GetValue(1)));
                    }
                    result[i, 1] = dr.GetValue(1);
                    result[i, 2] = dr.GetValue(2);
                    result[i, 3] = dr.GetValue(3);
                    result[i, 4] = dr.GetValue(4);
                    result[i, 5] = dr.GetValue(5);
                    result[i, 6] = dr.GetValue(6);
                    result[i, 7] = dr.GetValue(7);
                    result[i, 8] = dr.GetValue(8);
                    result[i, 9] = dr.GetValue(9);
                    result[i, 10] = dr.GetValue(10);
                    result[i, 11] = dr.GetValue(11);
                    result[i, 12] = dr.GetValue(12);
                    result[i, 13] = dr.GetValue(13);
                    result[i, 14] = dr.GetValue(14);
                    i++;
                }
                catch (Exception e)
                {
                    string error = "Ошибка в процедуре GetItemsFromInvoice. Ошибка получении товарных позиций. TRDS-RCDHDR: " + DocRcd;
                    Program.WriteLine(error);
                    WriteErrorLog(error);
                    WriteErrorLog(e.Message);
                    WriteErrorLog(e.Source);
                }

            }
            conn.Close();
            return result;
        }

        /*
          Метод вставляет значения во таблицу для временного хранения данных
        */
        internal static void InsertTmpD(ArrayList list)
        {
            //соберем строку, для выполнения запроса
            string insertColumn = " insert into U_MGEDITMPDOC ( ";
            string insertValues = " values ( ";
            int countList = list.Count;
            foreach (ArrayList obj in list)
            {
                for (int i = 0; i < obj.Count; i++)
                {
                    insertColumn = insertColumn + "TmpD_Str" + (i + 1).ToString() + ", ";
                    insertValues = insertValues + "'" + list[i].ToString() + "', ";
                }
            }
            //удалим последние заяптые
            insertColumn = insertColumn.Substring(0, insertColumn.Length - 2) + " )";
            insertValues = insertValues.Substring(0, insertValues.Length - 2) + " )";
            string insert = insertColumn + insertValues;
            //запись в U_MgEDITmpDoc
            string connString = Settings.Default.ConnStringISPRO;
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(insert, conn);
            try
            {
                SqlDataReader dr = command.ExecuteReader();
            }
            catch (IOException e)
            {
                Program.WriteLine("---------------");
                Program.WriteLine("Ошибка записи U_MgEDITmpDoc.");
                WriteErrorLog("Ошибка записи U_MgEDITmpDoc.");
                WriteErrorLog(e.Message);
                Program.WriteLine(insert);
                Program.WriteLine("---------------");
            };
            conn.Close();
        }

        /*
          Метод очищает временную таблицу от всех значений
        */
        internal static void DeleteTmpDet()
        {
            //соберем строку, для выполнения запроса
            string sql_as_string = " DELETE FROM U_MGEDITMPDOC ";

            //запись в U_MgEDITmpDoc
            string connString = Settings.Default.ConnStringISPRO;
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql_as_string, conn);
            try
            {
                SqlDataReader dr = command.ExecuteReader();
            }
            catch (IOException e)
            {
                Program.WriteLine("---------------");
                Program.WriteLine("Ошибка удаления U_MgEDITmpDoc.");
                WriteErrorLog("Ошибка удаления U_MgEDITmpDoc.");
                WriteErrorLog(e.Message);
                Program.WriteLine(sql_as_string);
                Program.WriteLine("---------------");
            };
            conn.Close();
        }

        /*
          Метод вставляет значения в таблицу для временного хранения данных
        */
        internal static void InsertTmpDet(List<List<string>> table)
        {
            string connString = Settings.Default.ConnStringISPRO; //настрйки доступа к базе данных
            SqlConnection connection = new SqlConnection();
            connection.ConnectionString = connString;
            connection.Open();

            SqlCommand command = connection.CreateCommand();
            SqlTransaction transaction;

            // Start a local transaction.
            transaction = connection.BeginTransaction("SampleTransaction");

            // Must assign both transaction object and connection
            // to Command object for a pending local transaction
            command.Connection = connection;
            command.Transaction = transaction;

            try
            {

                foreach (List<string> row in table)
                {
                    string insertColumn = " insert into U_MGEDITMPDOC ( ";
                    string insertValues = " values ( ";
                    for (int i = 0; i < row.Count; i++)
                    {
                        //Program.WriteLine(row[i].ToString());
                        insertColumn = insertColumn + "TmpD_Str" + (i + 1).ToString() + ", ";
                        insertValues = insertValues + "'" + row[i].ToString() + "', ";
                    }
                    //удалим последние заяптые
                    insertColumn = insertColumn.Substring(0, insertColumn.Length - 2) + " )";
                    insertValues = insertValues.Substring(0, insertValues.Length - 2) + " )";
                    command.CommandText = insertColumn + insertValues;
                    command.ExecuteNonQuery();
                }

                // Attempt to commit the transaction.
                transaction.Commit();
                Console.WriteLine("Both records are written to database.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Commit Exception Type: {0}", ex.GetType());
                Console.WriteLine("  Message: {0}", ex.Message);

                // Attempt to roll back the transaction.
                try
                {
                    transaction.Rollback();
                }
                catch (Exception ex2)
                {
                    // This catch block will handle any errors that may have occurred
                    // on the server that would cause the rollback to fail, such as
                    // a closed connection.
                    Console.WriteLine("Rollback Exception Type: {0}", ex2.GetType());
                    Console.WriteLine("  Message: {0}", ex2.Message);
                }
            }

        }

        /*
           Создание документа RecAdv 
        */
        internal static void CreateRecAdv(string ADate, string ANumber, Int64 BuyerILN, Int64 DeliveryILN, string DespNumber, string GrossAmount, int LineQt, string NetAmount, string OrdDate, string OrdNumber, string Qt, string RecDate, string Cmt = "")
        {
            //соберем строку, для выполнения запроса
            string insert = " DECLARE @p1 decimal(21,4); DECLARE @p2 decimal(21,4); " +
                            " SET @p1 = CAST('" + GrossAmount.Replace(',','.') + "' as decimal(21,4)); " +
                            " SET @p2 = CAST('" + NetAmount.Replace(',', '.') + "' as decimal(21,4)); " +
                            " EXECUTE [dbo].[U_MGEDICreateRecAdv] '" + Convert.ToDateTime(ADate).ToString("yyyyMMdd") + "', '" + ANumber + "', " + BuyerILN.ToString() + ", " + DeliveryILN.ToString() + ", '" + DespNumber + "', @p1, "
                                                                   +  LineQt.ToString() + ", @p2, '" + Convert.ToDateTime(OrdDate).ToString("yyyyMMdd") + "', '" + OrdNumber + "', '" + DespNumber + "', " + Qt.ToString().Replace(',', '.') + ", '" + Convert.ToDateTime(RecDate).ToString("yyyyMMdd") + "'" + ",'" + Cmt +  "'";
            //запись в U_MGRECADV
            string connString = Settings.Default.ConnStringISPRO;
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(insert, conn);
            try
            {
                SqlDataReader dr = command.ExecuteReader();
            }
            catch (IOException e)
            {
                Program.WriteLine("---------------");
                Program.WriteLine("Ошибка записи U_MGRECADV.");
                WriteErrorLog("Ошибка записи U_MGRECADV.");
                WriteErrorLog(e.Message);
                Program.WriteLine(insert);
                Program.WriteLine("---------------");
            };
            conn.Close();
        }

        /*
          Проверить не загружали ли мы ранее этот документ
        */
        public static bool checkExistsRecAdv(string ANumber, string OrdNumber)
        {
            //соберем строку, для выполнения запроса
            string select = " SELECT count(*) FROM U_MGRecAdv WHERE RecAdv_ANumber = '" + ANumber + "' and RecAdv_OrdNumber = '" + OrdNumber + "'";
            bool result = false;
            string connString = Settings.Default.ConnStringISPRO;
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(select, conn);
            try
            {
                result = ((int) command.ExecuteScalar() > 0)? true: false;
            }
            catch (IOException e)
            {
                Program.WriteLine("---------------");
                Program.WriteLine("Ошибка выполнения запроса." + select);
                WriteErrorLog("Ошибка выполнения запроса." + select);
                WriteErrorLog(e.Message);
                Program.WriteLine(select);
                Program.WriteLine("---------------");
            };
            conn.Close();

            return result;
        }

        /*
          Сравнить расходную накладную (за минусом возврата если он есть) с уведомлением о приемке, разницу оставить во врменной таблице
       */
        internal static int CompareActAndInvoice(string OrdNumber, string DespNumber, ref string ERROR)
        {
            string connString = Settings.Default.ConnStringISPRO;
            int status = 99999;
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            try
            {
                SqlCommand command = new SqlCommand("U_MGCompareActAndInvoice", conn);
                command.CommandType = CommandType.StoredProcedure;
                // set up the parameters
                command.Parameters.Add("@ordNumber", SqlDbType.VarChar, 35);
                command.Parameters.Add("@despatchNumber", SqlDbType.VarChar, 35);
                command.Parameters.Add("@RESULT", SqlDbType.Int).Direction = ParameterDirection.Output;

                // set parameter values
                command.Parameters["@ordNumber"].Value = OrdNumber;
                command.Parameters["@despatchNumber"].Value = DespNumber;

                // open connection and execute stored procedure
                command.ExecuteNonQuery();

                // read output value from @NewId
                status = Convert.ToInt32(command.Parameters["@RESULT"].Value);
                if (status == 2) ERROR = "отсутсвует накладная по номеру заказа (или номеру расходной накладной)";
                if (status == 0) ERROR = "ошибка при разборе акта";
            }
            catch(Exception e)
            {
                status = 0;
                ERROR = e.Message;
            }
            conn.Close();

            return status;
        }

        internal static void RenumberOrdersTmpDbf()
        {
            string connString = Settings.Default.ConnStringISPRO;
            string Insert = " UPDATE U_CHTMPDBF \n"
                          + " SET NUMZAK = q.nmzk, \n"
                          + "     DogRcd = q.Dog_Rcd \n"
                          + " FROM ( \n"
                          + "        SELECT \n"
                          + "          rank() OVER(ORDER BY [GplCode], [DateOtg], Dog_Rcd) nmzk, \n"
                          + "          GplCode gpl, \n"
                          + "          Art nom, \n"
                          + "          DateOtg dtotg, \n"
                          + "          ISNULL(Dog_Rcd,0) Dog_Rcd \n"
                          + "        FROM \n"
                          + "          U_CHTMPDBF AS A \n"
                          + "        INNER JOIN PTNRK on Ptn_Cd = GplCode \n"
                          + "        INNER JOIN SKLN on SklN_Cd = Art \n"
                          + "        LEFT JOIN U_MGDOGNOMGPL on Pl_Rcd = Ptn_RcdPlat and Nomen_Rcd = Skln_Rcd and DateOtg between StDAte and EndDate \n"
                          + "      ) q \n"
                          + " WHERE \n"
                          + " q.gpl = GplCode and q.dtotg = DateOtg and q.nom = Art \n";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(Insert, conn);
            try
            {
                SqlDataReader dr = command.ExecuteReader();
            }
            catch (IOException e)
            {
                Program.WriteLine("---------------");
                Program.WriteLine("Ошибка обновления U_CHTMPDBF.");
                WriteErrorLog("Ошибка обновления U_CHTMPDBF.");
                WriteErrorLog(e.Message);
                Program.WriteLine("---------------");
            };
            conn.Close();
        }

        /*
         Вернуть цену с налогом
         Функция проверяет переданные ноды на наличие значений, в зависимости от результата возвращает преобразованное значение
       */
        public static string getPriceWithNds(XmlNode PriceWithNds, XmlNode PriceWithoutNds, string TaxRate)
        {
            string OrderPrice = "0"; //переменная для будущей цены с налогом
            XmlNode PriceOrderNode;

            PriceOrderNode = PriceWithNds;   //цена с НДС
            if (PriceOrderNode != null) //если что-то вернули
                OrderPrice = PriceOrderNode.InnerText;
            else
            {
                PriceOrderNode = PriceWithoutNds;  // есть только без НДС, надо считать
                if (PriceOrderNode != null)
                    OrderPrice = PriceOrderNode.InnerText;
                try  // точка или запятая в системе? а ХЗ!!! в хмле идёт точка
                {
                    OrderPrice = Convert.ToString(Math.Round(Convert.ToDecimal(OrderPrice) * Convert.ToDecimal("1." + TaxRate), 2)); 
                }
                catch
                {
                    OrderPrice = Convert.ToString(Math.Round(Convert.ToDecimal(OrderPrice.Replace(".", ",")) * Convert.ToDecimal("1," + TaxRate), 2));
                    OrderPrice = OrderPrice.Replace(",", "."); // Возвращаю опять точку, чтобы было как везде
                }
            }
            return OrderPrice;
        }

        internal static string GetISOCode(string PtnCd)
        {
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "SELECT SAdrReg_Cd FROM PTNRK \n"
                        + "LEFT JOIN PTNFILK on PTNFILK .Ptn_Rcd=PTNRK.Ptn_Rcd \n"
                        + "LEFT JOIN [ISPRO_SYS].[dbo].[SADRSREG] REGION on PTNFILK.Filia_Rgn=REGION.SAdrReg_Rcd \n"
                        + " WHERE PTNRK.Ptn_Cd = '" + Convert.ToString(PtnCd) + "'";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            string ISO;
            int RegCd;
            object[] result = new object[n];
            while (dr.Read())
            {
                dr.GetValues(result);
            }
            conn.Close();
            if (result.Length == 0 || Convert.ToString(result[0]) == "")
            {
                ISO = "empty";
            }
            else
            {
                RegCd = Convert.ToInt32(result[0]);
                switch (RegCd)
                {
                    case 1: ISO = "RU-AD"; break;
                    case 2: ISO = "RU-BA"; break;
                    case 3: ISO = "RU-BU"; break;
                    case 4: ISO = "RU-AL"; break;
                    case 5: ISO = "RU-DA"; break;
                    case 6: ISO = "RU-IN"; break;
                    case 7: ISO = "RU-KB"; break;
                    case 8: ISO = "RU-KL"; break;
                    case 9: ISO = "RU-KC"; break;
                    case 10: ISO = "RU-KR"; break;
                    case 11: ISO = "RU-KO"; break;
                    case 12: ISO = "RU-ME"; break;
                    case 13: ISO = "RU-MO"; break;
                    case 14: ISO = "RU-SA"; break;
                    case 15: ISO = "RU-SE"; break;
                    case 16: ISO = "RU-TA"; break;
                    case 17: ISO = "RU-TY"; break;
                    case 18: ISO = "RU-UD"; break;
                    case 19: ISO = "RU-KK"; break;
                    case 20: ISO = "RU-CE"; break;
                    case 21: ISO = "RU-CU"; break;
                    case 22: ISO = "RU-ALT"; break;
                    case 23: ISO = "RU-KDA"; break;
                    case 24: ISO = "RU-KYA"; break;
                    case 25: ISO = "RU-PRI"; break;
                    case 26: ISO = "RU-STA"; break;
                    case 27: ISO = "RU-KHA"; break;
                    case 28: ISO = "RU-AMU"; break;
                    case 29: ISO = "RU-ARK"; break;
                    case 30: ISO = "RU-AST"; break;
                    case 31: ISO = "RU-BEL"; break;
                    case 32: ISO = "RU-BRY"; break;
                    case 33: ISO = "RU-VLA"; break;
                    case 34: ISO = "RU-VGG"; break;
                    case 35: ISO = "RU-VLG"; break;
                    case 36: ISO = "RU-VOR"; break;
                    case 37: ISO = "RU-IVA"; break;
                    case 38: ISO = "RU-IRK"; break;
                    case 39: ISO = "RU-KGD"; break;
                    case 40: ISO = "RU-KLU"; break;
                    case 41: ISO = "RU-KAM"; break;
                    case 42: ISO = "RU-KEM"; break;
                    case 43: ISO = "RU-KIR"; break;
                    case 44: ISO = "RU-KOS"; break;
                    case 45: ISO = "RU-KGN"; break;
                    case 46: ISO = "RU-KRS"; break;
                    case 47: ISO = "RU-LEN"; break;
                    case 48: ISO = "RU-LIP"; break;
                    case 49: ISO = "RU-MAG"; break;
                    case 50: ISO = "RU-MOS"; break;
                    case 51: ISO = "RU-MUR"; break;
                    case 52: ISO = "RU-NIZ"; break;
                    case 53: ISO = "RU-NGR"; break;
                    case 54: ISO = "RU-NVS"; break;
                    case 55: ISO = "RU-OMS"; break;
                    case 56: ISO = "RU-ORE"; break;
                    case 57: ISO = "RU-ORL"; break;
                    case 58: ISO = "RU-PNZ"; break;
                    case 59: ISO = "RU-PER"; break;
                    case 60: ISO = "RU-PSK"; break;
                    case 61: ISO = "RU-ROS"; break;
                    case 62: ISO = "RU-RYA"; break;
                    case 63: ISO = "RU-SAM"; break;
                    case 64: ISO = "RU-SAR"; break;
                    case 65: ISO = "RU-SAK"; break;
                    case 66: ISO = "RU-SVE"; break;
                    case 67: ISO = "RU-SMO"; break;
                    case 68: ISO = "RU-TAM"; break;
                    case 69: ISO = "RU-TVE"; break;
                    case 70: ISO = "RU-TOM"; break;
                    case 71: ISO = "RU-TUL"; break;
                    case 72: ISO = "RU-TYU"; break;
                    case 73: ISO = "RU-ULY"; break;
                    case 74: ISO = "RU-CHE"; break;
                    case 75: ISO = "RU-ZAB"; break;
                    case 76: ISO = "RU-YAR"; break;
                    case 77: ISO = "RU-MOW"; break;
                    case 78: ISO = "RU-SPE"; break;
                    case 79: ISO = "RU-YEV"; break;
                    case 80: ISO = "RU-ZAB"; break;
                    case 81: ISO = "RU-KO"; break;
                    case 82: ISO = "RU-KAM"; break;
                    case 83: ISO = "RU-NEN"; break;
                    case 84: ISO = "RU-KYA"; break;
                    case 85: ISO = "RU-IRK"; break;
                    case 86: ISO = "RU-KHM"; break;
                    case 87: ISO = "RU-CHU"; break;
                    case 88: ISO = "RU-KYA"; break;
                    case 89: ISO = "RU-YAN"; break;
                    case 90: ISO = "empty"; break;
                    case 91: ISO = "RU-CR"; break;
                    case 92: ISO = "RU-SEV"; break;
                    case 93: ISO = "empty"; break;
                    case 94: ISO = "empty"; break;
                    case 95: ISO = "empty"; break;
                    case 96: ISO = "empty"; break;
                    case 97: ISO = "empty"; break;
                    case 98: ISO = "empty"; break;
                    case 99: ISO = "empty"; break;
                    default: ISO = "RU-CHE"; break;
                }

            }
            return ISO;
        }

        internal static object[,] GetListCOInvoice()//получение списка COINVOICE на отправку с учетом времени отсрочки
        {
            int i = 0;
            string connString = Settings.Default.ConnStringISPRO;
            string CMDGetSF = "SELECT  ProviderOpt, ProviderZkg, NastDoc_Fmt, SklSf_Rcd, SklSf_TpOtg, SklSfA_RcdCor, PrdZkg_NmrExt, PrdZkg_Rcd, PrdZkg_Dt, SklNk_TDrvNm FROM U_vwChListInvForSentNPlat WHERE SklSf_TpOtg=3 AND ISNULL(ProviderZkg,'') <> '' AND ISNULL(NastDoc_Fmt,'') <> ''";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            try
            {
                conn.Open();
            }
            catch (Exception e)
            {
                Program.WriteLine("Ошибка соединения с БД " + Convert.ToString(e));
                if (conn.State == ConnectionState.Open) conn.Close();
            }
            SqlCommand command = new SqlCommand(CMDGetSF, conn);
            SqlDataReader dr = command.ExecuteReader();

            int recordCount = 0;
            while (dr.Read()) recordCount++;
            object[,] result = new object[recordCount, dr.FieldCount];

            dr.Close();

            dr = command.ExecuteReader();

            while (dr.Read())
            {
                for (int j = 0; j < dr.FieldCount; j++) result[i, j] = dr.GetValue(j);
                i++;
            }
            conn.Close();
            return result;
        }

        public static object[,] GetItemFromInvoice(string DocRcd, string SklN_Cd, bool PCE)//rcd документа, артикул покупателя, bool PCE - маркер ЕИ штука.
        {
            //Program.WriteLine("процедура GetItemFromInvoice Блок1");
            int i = 0;
            string verification;
            string connString = Settings.Default.ConnStringISPRO;
            //Program.WriteLine("процедура GetItemFromInvoice Блок2 " + connString);
            if (PCE == true) //надо все в штуках
            {
                verification = "SELECT ISNULL((SELECT TOP 1 BarCode_Code FROM BarCode WHERE BarCode_RcdNom = Nom.SklN_Rcd ORDER BY BARCODE_Base DESC),(SELECT TOP 1 BR.TrdS_BarCode FROM TRDS BR WITH (NOLOCK) WHERE BR.TrdS_BarCode <> '' AND BR.TrdS_TypHdr = 1  AND BR.TrdS_RcdNom = SklN_Rcd AND BR.TrdS_TypPar IN (0,17))) BarCode_Code --0\n"
                                + "     , CASE WHEN LEFT(SklGr_CD, 3) IN ( '039', '139' ) THEN SUBSTRING(skln_nm, 1, 6) ELSE SUBSTRING(skln_nm, 1, 4) END AS Cod --1\n"
                                + "     , SklN_Cd --2\n"
                                + "     , SklN_Nm --3\n"
                                + "	 , TrdS_Qt * (TrdS_QtOsn/TrdS_Qt)/EISht.NmEi_QtOsn  --4 кол-во\n"
                                + "     , CONVERT( DECIMAL(18, 2), (TrdS_Cn * EISht.NmEi_QtOsn / TrdS_QtOsn * TrdS_Qt) / ( 1 + CAST( RTRIM( LTRIM( NDS.TaxRate_Val ))AS FLOAT ) / 100 )) --5 Цена без НДС\n"
                                + "     , CONVERT( DECIMAL(18, 2), (TrdS_Cn * EISht.NmEi_QtOsn / TrdS_QtOsn * TrdS_Qt))  --6 Цена с НДС\n"
                                + "     , 'PCE' --7\n"
                                + "     , EI_OKEI --8\n"
                                + "     , CONVERT( DECIMAL(18, 2), TaxRate_Val) --9\n"
                                + "     , 'S' --10\n"
                                + "     , CONVERT( DECIMAL(18, 2), TrdS_SumTax) --11\n"
                                + "     , CONVERT( DECIMAL(18, 2), (trds_sumopl - trds_sumtax)) --12\n"
                                + "     , /*CONVERT( DECIMAL(18, 2), TrdS_SumExt) * CONVERT(DECIMAL(18, 3), TrdS_Qt)*/ 0 --13\n"
                                + "     , TrdS_QtOsn --14\n"        // количество в основной ЕИ, (КГ наверное) для садиков...
                                + "FROM TRDS\n"
                                + "     JOIN SKLN AS Nom ON Nom.SklN_Rcd = TrdS_RcdNom\n"
                                //+ "     LEFT JOIN BarCode AS bc ON bc.BarCode_RcdNom = Nom.SklN_Rcd AND BARCODE_Base = 1\n"
                                + "     LEFT JOIN EI ON EI_Rcd = 5\n"
                                + "     LEFT JOIN SKLGR ON SklN_RcdGrp = SklGr_Rcd\n"
                                + "     LEFT JOIN SKLNOMTAX AS TAX ON TAX.NmTax_RcdPar = Nom.SklN_Rcd\n"
                                + "     LEFT JOIN TAXRATE AS NDS ON NDS.TaxRate_Rcd = TAX.NmTax_CdRate\n"
                                + "     LEFT JOIN SKLNOMEI AS EISht ON Nom.SklN_Rcd = EISht.NmEi_RcdNom AND EISht.NmEi_Cd = 5\n"
                                + "     --LEFT JOIN SKLNOMEI AS EIBox ON Nom.SklN_Rcd = EIBox.NmEi_RcdNom AND EIBox.NmEi_Cd = 39\n"
                                + "     --LEFT JOIN SKLNOMEI AS EIKG ON Nom.SklN_Rcd = EIBox.NmEi_RcdNom AND EIBox.NmEi_Cd = 1\n"
                                + "WHERE trds_rcdhdr = " + DocRcd + "\n"
                                + "AND SklN_Cd = '" + SklN_Cd + "'\n"
                                + "      AND TrdS_TypHdr = 5;";
            }
            else
                verification = "SELECT ISNULL((SELECT TOP 1 BarCode_Code FROM BarCode WHERE BarCode_RcdNom = Nom.SklN_Rcd ORDER BY BARCODE_Base DESC),(SELECT TOP 1 BR.TrdS_BarCode FROM TRDS BR WITH (NOLOCK) WHERE BR.TrdS_BarCode <> '' AND BR.TrdS_TypHdr = 1  AND BR.TrdS_RcdNom = SklN_Rcd AND BR.TrdS_TypPar IN (0,17))) BarCode_Code --0\n"
                                + "     , CASE WHEN LEFT(SklGr_CD, 3) IN ( '039', '139' ) THEN SUBSTRING(skln_nm, 1, 6) ELSE SUBSTRING(skln_nm, 1, 4)  END AS Code --1\n"
                                + "     , SklN_Cd --2\n"
                                + "     , SklN_Nm --3\n"
                                + "     , CONVERT( DECIMAL(18, 3), TrdS_Qt) --4\n"
                                + "	 , CONVERT( DECIMAL(18, 2), TrdS_Cn / ( 1 + CAST( RTRIM( LTRIM( NDS.TaxRate_Val ))AS FLOAT ) / 100 )) --5 Цена без НДС\n"
                                + "     , CONVERT( DECIMAL(18, 2), TrdS_Cn) --6\n"
                                + "     , CASE WHEN EI_Rcd = 1 THEN 'KG' WHEN EI_Rcd = 5 THEN 'PCE' WHEN EI_Rcd = 39 THEN 'CT' END --7\n"
                                + "     , EI_OKEI --8\n"
                                + "     , CONVERT( DECIMAL(18, 2), TaxRate_Val) --9\n"
                                + "     , 'S' --10\n"
                                + "     , CONVERT( DECIMAL(18, 2), TrdS_SumTax) --1\n"
                                + "     , CONVERT( DECIMAL(18, 2), trds_sumopl - trds_sumtax) --12\n"
                                + "     , 0 /*CONVERT( DECIMAL(18, 2), TrdS_SumExt) * CONVERT(DECIMAL(18, 3), TrdS_Qt)*/ --13\n"
                                + "     , TrdS_QtOsn --14\n"        // количество в основной ЕИ, (КГ наверное) для садиков...
                                + "FROM TRDS\n"
                                + "     JOIN SKLN AS Nom ON Nom.SklN_Rcd = TrdS_RcdNom\n"
                                + "     LEFT JOIN EI ON TrdS_EiQt = ei_rcd\n"
                                + "     LEFT JOIN SKLGR ON SklN_RcdGrp = SklGr_Rcd\n"
                                + "     LEFT JOIN SKLNOMTAX AS TAX ON TAX.NmTax_RcdPar = Nom.SklN_Rcd\n"
                                + "     LEFT JOIN TAXRATE AS NDS ON NDS.TaxRate_Rcd = TAX.NmTax_CdRate\n"
                                + "WHERE trds_rcdhdr = " + DocRcd + "\n"
                                + "AND SklN_Cd = '" + SklN_Cd + "'\n"
                                + "      AND TrdS_TypHdr = 5;";
            //Program.WriteLine("процедура GetItemFromInvoice Блок3 " + verification);
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            //Program.WriteLine("процедура GetItemFromInvoice Блок4 " + Convert.ToString(n));
            object[,] result = new object[1, 15];
            while (dr.Read())
            {
                try
                {
                    result[i, 0] = dr.GetValue(0);
                    if (Convert.ToString(result[i, 0]).Length == 0)//баркода нет - ищеи альтернативу
                    {
                        result[i, 0] = DispOrders.GetAltBarCode(Convert.ToString(dr.GetValue(1)));
                    }
                    result[i, 1] = dr.GetValue(1);
                    result[i, 2] = dr.GetValue(2);
                    result[i, 3] = dr.GetValue(3);
                    result[i, 4] = dr.GetValue(4);
                    result[i, 5] = dr.GetValue(5);
                    result[i, 6] = dr.GetValue(6);
                    result[i, 7] = dr.GetValue(7);
                    result[i, 8] = dr.GetValue(8);
                    result[i, 9] = dr.GetValue(9);
                    result[i, 10] = dr.GetValue(10);
                    result[i, 11] = dr.GetValue(11);
                    result[i, 12] = dr.GetValue(12);
                    result[i, 13] = dr.GetValue(13);
                    result[i, 14] = dr.GetValue(14);
                    //Program.WriteLine("процедура GetItemFromInvoice Блок5 " + Convert.ToString(result[i, 4]));
                    i++;
                }
                catch (Exception e)
                {
                    string error = "Ошибка в процедуре GetItemFromInvoice. Ошибка получения товарных позиций. TRDS-RCDHDR: " + DocRcd;
                    Program.WriteLine(error);
                    WriteErrorLog(error);
                    WriteErrorLog(e.Message);
                    WriteErrorLog(e.Source);
                }

            }
            conn.Close();
            return result;
        }

        public static object GetAltBarCode(string Code) //Возвращает баркод 
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select BarCode_Code from SKLN JOIN BarCode bc on bc.BarCode_RcdNom = SklN_Rcd AND BARCODE_Base = 1 where skln_nm like '" + Code + "%' ";

            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();

            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read()) dr.GetValues(result);
            conn.Close();
            return result[0];
        }


        public static int GetRouteSchedule(string PtnCd)  // возвращает номер текущего маршрута согласно графику маршрутов в карточке контрагента
        {
            string connString = Settings.Default.ConnStringISPRO;      // запрос на график маршрутов
            string schedulesQuery = "SELECT prv.UF_RkValS "
                                  + "FROM UFPRV prv "
                                  + "LEFT JOIN UFRKV rkv ON prv.UF_RkRcd = rkv.UFR_RkRcd "
                                  + "LEFT JOIN PTNRK ptn ON prv.UF_TblRcd = ptn.Ptn_Rcd "
                                  + "WHERE prv.UF_RkRcd = rkv.UFR_RkRcd AND prv.UF_TblId = 1126 AND rkv.UFR_Id = 'U_GR_MAR' "
                                  + "AND ptn.Ptn_Cd = '" + PtnCd + "' ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(schedulesQuery, conn);
            SqlDataReader dataReader = command.ExecuteReader();
            int result = 199999;
            DateTime DateExp = new DateTime();

            if (dataReader.Read())
            {
                object[] scheduleResult = new object[dataReader.VisibleFieldCount];
                dataReader.GetValues(scheduleResult);

                string routeScheduleString = scheduleResult[0].ToString();     // график маршрутов - строка в которой ч/з ';' указаны номера маршрутов на каждый день недели

                dataReader.Close();
                try
                {
                    string getDocDateExp = "SELECT TOP 1 DocDateExp FROM U_CHTMPZKG";   // запрос на дату отгрузки заказа
                    command = new SqlCommand(getDocDateExp, conn);
                    SqlDataReader dateExpReader = command.ExecuteReader();
                    if (dateExpReader.Read())
                    {
                        object[] DocDateExp = new object[dateExpReader.VisibleFieldCount];
                        dateExpReader.GetValues(DocDateExp);
                        DateExp = (DateTime)DocDateExp[0];
                        dateExpReader.Close();
                    }
                    else DateExp = DateTime.Now;     // на случай, если вдруг не получилось достать дату отгрузки
                }
                catch (Exception ex)
                {
                    Program.WriteLine(ex.Message);
                    DateExp = DateTime.Now;
                }

                int weekDay = (int)DateExp.DayOfWeek;      // получаем день недели
                string[] routes = routeScheduleString.Split(';');     // разделние строки маршрутов на массив
                string Route;
                if (weekDay == 0)                  // в америке неделя начинается с воскресенья, DayOfWeek не содержит 7-ки, первый день = 0
                    Route = routes[6];
                else
                    Route = routes[weekDay - 1];

                
                try
                {
                    Program.WriteLine("Запрос на Rcd маршрута, т.к. в графике указан Cd.");
                    string getTrdRcd = "SELECT TrdRt_Rcd FROM TRDRT WHERE TrdRt_Cd = '" + Route + "' ";   // запрос на Rcd маршрута
                    command = new SqlCommand(getTrdRcd, conn);
                    SqlDataReader trdReader = command.ExecuteReader();
                    if (trdReader.Read())
                    {
                        object[] routeCode = new object[trdReader.VisibleFieldCount];
                        trdReader.GetValues(routeCode);
                        result = Convert.ToInt32(routeCode[0]);
                    }
                }catch (Exception ex)
                {
                    result = 199999;
                }
                

            }
            return result;
        }




    }
}   
    
