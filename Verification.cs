using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.Data.SqlClient;
using System.Data;
//using Microsoft.Office.Core;
using System.IO;
using System.Xml;
using System.Text.RegularExpressions;

// проверка данных на корректность - к/а, номенклатура, адреса доставки и прочего

namespace AutoOrdersIntake
{
    class Verifiacation
    {
        public static object[] Verification_Delivery_Proviant(string PtnAccord_Nm, string ufs_jur)//верификация адреса доставки для Провианта-возвращает массив: 1-Ptn_Rcd 2-Ptn_Cd
        {
            char ch = ':';
            int IndexOfChar = PtnAccord_Nm.IndexOf(ch);
            string cut_PtnAccordNm = PtnAccord_Nm.Remove(IndexOfChar);
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT Ptn_Cd, Ptn_Rcd FROM UFLstSpr "
                                + " left join UFSpr ON UFSpr.UFS_Rcd = UFLstSpr.UFS_Rcd "
                                + " left join PTNRK on ptnrk.Ptn_Cd = UFS_CdS "
                                + " WHERE UFS_Nm = '" + cut_PtnAccordNm + "' and UFS_CdSpr = '" + ufs_jur + "' ";
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

        public static object[] Verification_Plat_Proviant(string Ptn_Cd)//верификация плательщика для Провианта-возвращает массив: 1 - ptn_cd  2 - ptn_NmSh 3- Filia_Adr
        {
            //Ptn_Cd = "0" + Ptn_Cd;
            string connString = Settings.Default.ConnStringISPRO;
            string verification = " SELECT Ptn_Cd,Ptn_NmSh, Filia_Adr FROM PTNRK  "
                                 + " LEFT JOIN PTNFILK on PTNFILK.Ptn_Rcd = PTNRK.Ptn_Rcd "
                                 + " WHERE PTNRK.Ptn_Rcd = "
                                 + " (SELECT Ptn_RcdPlat FROM PTNRK "
                                 + " WHERE Ptn_Rcd = '" + Ptn_Cd + "')";
                                // + " WHERE Ptn_Cd = '" + Ptn_Cd + "')";
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

        public static string[] GetBuyerOptimum(string Ptn_Cd)//верификация плательщика для Оптимум -возвращает массив: 1 - ptn_cd  2 - ptn_NmSh 3- Filia_Adr
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = " SELECT Ptn_Cd,Ptn_NmSh, Filia_Adr FROM PTNRK  "
                                 + " LEFT JOIN PTNFILK on PTNFILK.Ptn_Rcd = PTNRK.Ptn_Rcd "
                                 + " WHERE PTNRK.Ptn_Rcd = "
                                 + " (SELECT Ptn_RcdPlat FROM PTNRK "
                                 + " WHERE Ptn_Cd = '" + Ptn_Cd + "')";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            string[] result = new string [n];
            while (dr.Read())
            {
                dr.GetValues(result);
            }
            conn.Close();
            return result;

        }

        public static string[] Verification_gln(string gln)//верификация gln доставки -возвращает массив строк(ptn_cd - ptn_NmSh - Filia_Adr) либо null-null-null
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT top 1 Ptn_Cd, Ptn_NmSh, Filia_Adr, CONVERT(VARCHAR(10),PTNRK.Ptn_Rcd) FROM PTNRK"
                                + "     JOIN UFPRV ON UFPRV.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRV.UF_TblId = 1126"
                                + "     JOIN UFRKV ON UFRKV.UFR_DbRcd = UFPRV.UF_TblId AND UFRKV.UFR_RkRcd = UFPRV.UF_RkRcd AND UFRKV.UFR_Id = 'U_GLN'"
                                + "     JOIN PTNFILK on PTNFILK.Ptn_Rcd = PTNRK.Ptn_Rcd"
                                + "     WHERE UFPRV.UF_RkValS = '" + gln + "'";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            string[] result = new string[n];
            while (dr.Read())
            {
                dr.GetValues(result);
            }
            conn.Close();
            return result;
        }

        public static string[] Verification_gln_buyer(string gln)//верификация gln доставки -возвращает массив строк(ptn_cd - ptn_NmSh - Filia_Adr) либо null-null-null
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT top 1 Ptn_Cd, Ptn_NmSh, Filia_Adr, CONVERT(VARCHAR(10),PTNRK.Ptn_Rcd) FROM PTNRK"
                                + "     JOIN UFPRV ON UFPRV.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRV.UF_TblId = 1126"
                                + "     JOIN UFRKV ON UFRKV.UFR_DbRcd = UFPRV.UF_TblId AND UFRKV.UFR_RkRcd = UFPRV.UF_RkRcd" // AND UFRKV.UFR_Id = 'U_GLNGR'"
                                + "     JOIN PTNFILK on PTNFILK.Ptn_Rcd = PTNRK.Ptn_Rcd"
                                + "     WHERE UFPRV.UF_RkValS = '" + gln + "'";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            string[] result = new string[n];
            while (dr.Read())
            {
                dr.GetValues(result);
            }
            conn.Close();
            return result;
        }

        public static string[] Verification_Tander_Buyer(string PtnCd)//верификация адреса доставки по Ptn_Cd для тандера-возвращает массив строк(ptn_cd - ptn_NmSh - Filia_Adr) либо null-null-null
        {
            //string connString = Settings.Default.ConnStringISPRO;
            //string verification = "SELECT Ptn_Cd, Ptn_NmSh, Filia_Adr FROM PTNRK "
            //                     + "LEFT JOIN PTNFILK on PTNFILK.Ptn_Rcd = PTNRK.Ptn_Rcd "
            //                     + "WHERE Ptn_cd = '" + PtnCd + "'";
            //SqlConnection conn = new SqlConnection();
            //conn.ConnectionString = connString;
            //conn.Open();
            //SqlCommand command = new SqlCommand(verification, conn);
            //SqlDataReader dr = command.ExecuteReader();
            //int n = dr.VisibleFieldCount;
            //string[] result = new string[n];
            //while (dr.Read())
            //{
            //    dr.GetValues(result);
            //}
            //conn.Close();
            //return result;
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT Ptn_Cd, Ptn_NmSh, Filia_Adr FROM PTNRK "
                                 + "LEFT JOIN PTNFILK on PTNFILK.Ptn_Rcd = PTNRK.Ptn_Rcd "
                                 + "WHERE Ptn_cd = '" + PtnCd + "'";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            string[] result = new string[n];
            while (dr.Read())
            {
                dr.GetValues(result);
            }
            conn.Close();
            return result;
        }
        

        public static string[] Verification_Tander(string PtnCd, string JurSootvKA)//верификация адреса доставки по Ptn_Cd для тандера-возвращает массив строк(ptn_cd - ptn_NmSh - Filia_Adr) либо null-null-null
        {
            string connString = Settings.Default.ConnStringISPRO;
            //string JurSootvKA = DispOrders.GetValueOption("ТАНДЕР.ЖУРНАЛ КА");
            string verification = "SELECT Ptn_Cd, Ptn_NmSh, Filia_Adr, case when PTNRK.Ptn_MainCd = 0 then PTNRK.Ptn_Cd else (SELECT Ptn_Cd from ptnrk ptn WHERE ptn.pTN_rCD = PTNRK.Ptn_MainCd) END PlcD FROM PTNRK PTNRK "
                                 + "LEFT JOIN PTNFILK on PTNFILK.Ptn_Rcd = PTNRK.Ptn_Rcd and PTNFILK.Filia_Flg in (0,1)"
                                 + "WHERE Ptn_cd = (SELECT UFS_CdS  FROM UFLstSpr "
                                 + "			 JOIN UFSpr ON UFSpr.UFS_Rcd = UFLstSpr.UFS_Rcd WHERE UFS_CdSpr = '" + JurSootvKA + "'  and UFS_Nm = '" + PtnCd + "')";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            string[] result = new string[n];
            while (dr.Read())
            {
                dr.GetValues(result);
            }
            conn.Close();
            return result;
        }

        public static string[] Verification_Xls_Buyer(string PtnCd)//верификация адреса доставки по Ptn_RCd для тандера-возвращает массив строк(ptn_cd - ptn_NmSh - Filia_Adr) либо null-null-null
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT Ptn_Cd, Ptn_NmSh, Filia_Adr FROM PTNRK "
                                 + "LEFT JOIN PTNFILK on PTNFILK.Ptn_Rcd = PTNRK.Ptn_Rcd and PTNFILK.Filia_Flg in (0,2)"
                                 + "WHERE Ptn_cd = '" + PtnCd + "'";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            string[] result = new string[n];
            while (dr.Read())
            {
                dr.GetValues(result);
            }
            conn.Close();
            return result;
        }

        public static object[] Verification_gtin(string gtin)//верификация штрих кода товара - возвращает массив данных 0-баркод 1-артикул 2-название 3-ед.изм. 4-rcd ед.изм. 5-тип номенклатуры либо массив null'ов 6 - ставку налога
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select TOP 1 BarCode_Code \n"
                                + "      ,SklN_Cd \n"
                                + "      ,SklN_NmAlt \n"
                                + "      ,EI_Nm \n"
                                + "      ,EI_Rcd \n "
                                + "      ,ISNULL(TypeSkln,5) \n "
                                + "      ,TAXRATE.TaxRate_Val \n "
                                + " from BARCODE bc \n"
                                + "        left join skln sn on bc.BarCode_RcdNom = sn.SklN_Rcd \n"
                                + "        left join EI on bc.BarCode_EiCd=ei.EI_Rcd \n"
                                + "        left join SKLGR on SklN_RcdGrp = SklGr_Rcd \n"
                                + "        left join U_ChSpGrType on SklGr_CD=Code_GrSkln \n"
                                + "        left join Sklnomtax on SKLNOMTAX.NmTax_RcdPar = sn.SklN_Rcd \n"
                                + "        Left join TAXRATE on TAXRATE.TaxRate_Rcd = SKLNOMTAX.NmTax_CdRate \n"
                                + "        where Convert(varchar(30),BarCode_Code) = '" + gtin + "' \n " //убрала and BarCode_Base = 1
                                +" ORDER BY BarCode_Base DESC "; 
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
            //на случай, если не определен тип номенклатуры, будет использоваться, как молоко
            if (result[0] != null)
            {
                if (result[5] == null)
                {
                    result[5] = GetDefaultTypeId();
                }
            }
            return result;
        }

        public static object[] Verification_gtin_xls(string gtin, string ufs_jur)//возвращает 0-skln_rcd, 1-skln_cd, 2-SklN_NmAlt, 3-ei_nm,4-ei_rcd, 1- мороженное или молоко
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select  skln_rcd, SklN_Cd, SklN_NmAlt, EI_Nm, EI_Rcd,  ISNULL(TypeSkln,5)  "
                                + " from UFSpr "
                                + " left join SKLN sn on SklN_Cd=UFS_CdS "
                                + " left join UFLstSpr on UFSpr.UFS_Rcd = UFLstSpr.UFS_Rcd "
                                + " left join BARCODE bc on bc.BarCode_RcdNom = sn.SklN_Rcd and bc.BarCode_Base = 1"
                                + " left join EI on bc.BarCode_EiCd=ei.EI_Rcd "
                                + " left join SKLGR on SklN_RcdGrp = SklGr_Rcd "
                                + " left join U_ChSpGrType on SklGr_CD=Code_GrSkln "
                                + " where UFS_Nm = '" + gtin + "' and UFS_CdSpr = '" + ufs_jur + "'";
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

        public static object[] GetPriceList(string PtnCd, int TypeSkln)//[0]-прайс [1]-рсд прайса
        {
            string connString = Settings.Default.ConnStringISPRO;
            string SysbaseName = DispOrders.GetValueOption("ОБЩИЕ.ИМЯ СИСТЕМНОЙ БД");
            //int price, price_rcd;
            string verification;
            string Name = DispOrders.GetNameTypeSkln(Convert.ToString(TypeSkln));
            if (Name.ToUpper() == "МОЛОКО")//молоко
            {
                verification =  "select ISNULL(SklPrcRst_Ass,0) from SklPrcRst where SklPrcRst_Rcd =  "
                               + "                                    (select Ptn_Prc from ptnrk   "
                               + "                                     LEFT JOIN UFPRV ON UFPRV.UF_TblId = ( SELECT RstDbId_Rst FROM " + SysbaseName + ".dbo.RSTDBID WHERE RstDbId_Id = 'PtnRk')    "
                               + "                                     AND UFPRV.UF_TblRcd = ptnrk.Ptn_Rcd                        "
                               + "                                     AND UFPRV.UF_TblId = ( SELECT UFRKV.UFR_DbRcd FROM UFRKV WHERE UFRKV.UFR_RkRcd = UFPRV.UF_RkRcd AND UFRKV.UFR_Id = 'U_MOR_PREISK')   "
                               + "                                     LEFT JOIN SklPrcRst ON SklPrcRst.SklPrcRst_Sh = UF_RkValS   "
                               + "                                     where Ptn_Cd = '"+PtnCd+"') "
                               + " union all "
                               + " select Ptn_Prc from ptnrk   "
                               + " LEFT JOIN UFPRV ON UFPRV.UF_TblId = ( SELECT RstDbId_Rst FROM " + SysbaseName + ".dbo.RSTDBID WHERE RstDbId_Id = 'PtnRk')    "
                               + " AND UFPRV.UF_TblRcd = ptnrk.Ptn_Rcd                        "
                               + " AND UFPRV.UF_TblId = ( SELECT UFRKV.UFR_DbRcd FROM UFRKV WHERE UFRKV.UFR_RkRcd = UFPRV.UF_RkRcd AND UFRKV.UFR_Id = 'U_MOR_PREISK')   "
                               + " LEFT JOIN SklPrcRst ON SklPrcRst.SklPrcRst_Sh = UF_RkValS   "
                               + " where Ptn_Cd = '"+PtnCd+"'";
            }
            else//мороженное
            {
                verification = "select ISNULL(SklPrcRst_Ass,159) from SklPrcRst where SklPrcRst_Rcd =  "
                               + "                                    (select SklPrcRst_Rcd from ptnrk   "
                               + "                                     LEFT JOIN UFPRV ON UFPRV.UF_TblId = ( SELECT RstDbId_Rst FROM " + SysbaseName + ".dbo.RSTDBID WHERE RstDbId_Id = 'PtnRk')    "
                               + "                                     AND UFPRV.UF_TblRcd = ptnrk.Ptn_Rcd                        "
                               + "                                     AND UFPRV.UF_TblId = ( SELECT UFRKV.UFR_DbRcd FROM UFRKV WHERE UFRKV.UFR_RkRcd = UFPRV.UF_RkRcd AND UFRKV.UFR_Id = 'U_MOR_PREISK')   "
                               + "                                     LEFT JOIN SklPrcRst ON SklPrcRst.SklPrcRst_Sh = UF_RkValS   "
                               + "                                     where Ptn_Cd = '" + PtnCd + "') "
                               + " union all "
                               + " select ISNULL(SklPrcRst_Rcd,249) from ptnrk   "
                               + " LEFT JOIN UFPRV ON UFPRV.UF_TblId = ( SELECT RstDbId_Rst FROM " + SysbaseName + ".dbo.RSTDBID WHERE RstDbId_Id = 'PtnRk') "
                               + " AND UFPRV.UF_TblRcd = ptnrk.Ptn_Rcd "
                               + " AND UFPRV.UF_TblId = ( SELECT UFRKV.UFR_DbRcd FROM UFRKV WHERE UFRKV.UFR_RkRcd = UFPRV.UF_RkRcd AND UFRKV.UFR_Id = 'U_MOR_PREISK') "
                               + " LEFT JOIN SklPrcRst ON SklPrcRst.SklPrcRst_Sh = UF_RkValS   "
                               + " where Ptn_Cd = '" + PtnCd + "'";
            }
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
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
            
            if ((i==0) || (i==1)) //если не указаны прайсы - используем по умолчанию.
            {
                switch (Name.ToUpper())
                {
                    case "МОЛОКО":
                        Array.Resize(ref result, 2);
                        result[0] = 22;
                        result[1] = 572;
                        break;
                    case "МОРОЖЕНОЕ":
                        Array.Resize(ref result, 2);
                        result[0] = 56;
                        result[1] = 226;
                        break;
                }
            }
            return result;
        }

        public static int CountRecords(string TableName)
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = " select COUNT(*) from " + TableName + "  ";
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
            int count = Convert.ToInt32(result[0]);
            return count;
        }

        public static object [] GetDataOrderFromArt(string Articul)
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select BarCode_Code "
                                + "      ,SklN_Cd "
                                + "      ,SklN_NmAlt "
                                + "      ,EI_Nm  "
                                + "      ,EI_Rcd  "
                                + "      ,case when LEFT( SklGr_CD, 3 )IN( '039', '139' ) then 6 else 5  end as TypeSkln  "
                                + " from BARCODE bc "
                                + "        left join skln sn on bc.BarCode_RcdNom = sn.SklN_Rcd "
                                + "        left join EI on bc.BarCode_EiCd=ei.EI_Rcd "
                                + "        left join SKLGR on SklN_RcdGrp = SklGr_Rcd"
                                + "        where bc.BarCode_Base = 1 and skln_cd =  '" + Articul + "'";
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

        public static object[] GetDataFromPtnCD(string Ptn_Cd)//--5 элемент ILN плательщика
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification =  "SELECT Ptn_Cd "
                                   + "     , Ptn_NmSh "
                                   + "     , UFPRV.UF_RkValS "
                                   + "     , Ptn_Inn "
                                   + "     , Ptn_KPP "
                                   + "     , UFPRV2.UF_RkValS "
                                   + "     , Filia_Adr "
                                   + "     , LEFT(Filia_Adr,6) "
                                   + "     , PTNRK.Ptn_Rcd "
                                   + "     , ISNULL(FILIALFROM.UF_RkValD,0) as FILIALFROM\n"  //9
                                   + "     , EdiNast.NastDoc_Fmt "  //10
                                   + " FROM PTNRK "
                                   + "     JOIN UFPRV ON UFPRV.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRV.UF_TblId = 1126 "
                                   + "     LEFT JOIN ufprv AS FILIALFROM ON FILIALFROM.UF_TblRcd = PTNRK.Ptn_Rcd AND FILIALFROM.UF_TblId = 1126 AND  FILIALFROM.UF_RkRcd = ( SELECT UFR_RkRcd FROM ufrkv WHERE UFR_Id = 'U_FILIAL_FROM' )"
                                   + "     JOIN UFRKV ON UFRKV.UFR_DbRcd = UFPRV.UF_TblId AND UFRKV.UFR_RkRcd = UFPRV.UF_RkRcd  AND UFRKV.UFR_Id = 'U_GLN'  "
                                   + "     LEFT JOIN UFPRV as UFPRV2 WITH (NOLOCK) ON UFPRV2.UF_TblId = 1126 AND UFPRV2.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRV2.UF_RkRcd = ( SELECT UFR_RkRcd FROM UFRKV WHERE UFR_DbRcd = 1126 AND UFR_Id = 'U_PRODCODE' ) "
                                   + "     LEFT JOIN PTNFILK on PTNFILK .Ptn_Rcd=PTNRK.Ptn_Rcd"
                                   + "     left join U_CHEDINASTDOC edinast on edinast.Gpl_Rcd = PTNRK.Ptn_Rcd"
                                   + " WHERE PTNRK.Ptn_Cd = '" + Ptn_Cd + "'";
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

        public static object[] GetContractInfo(string Ptn_Cd)//--1 номер контракта 2 - дата
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT \n"
                                + "ISNULL(SUBSTRING(UF_RkValS,1,charindex(' ',UF_RkValS)-1),'нет данных') as contract\n"
                                + ",ISNULL(SUBSTRING(UF_RkValS,LEN(UF_RkValS)-9,11),'2000-01-01') as dc  \n"
                                + "FROM PTNRK \n"
                                + "left JOIN UFPRV ON UFPRV.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRV.UF_TblId = 1126 and UFPRV.UF_RkRcd = ( SELECT UFR_RkRcd FROM UFRKV WHERE UFR_DbRcd = 1126 AND UFR_Id = 'U_CONTRACT_ID' ) \n"
                                + " WHERE PTNRK.Ptn_Cd = '" + Ptn_Cd + "'";
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

        public static object[] GetRecAdvInfo(Int64 prdZkgRcd)
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT Exch_RecAdvNmr, Exch_RecAdvDt FROM [U_CHEDIEXCH] WHERE Exch_ZkgRcd = " + prdZkgRcd.ToString();
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            object[] result = new object[2];
            while (dr.Read())
            {
                dr.GetValues(result);
            }
            conn.Close();
            return result;
        }

        public static object[] GetFirmInfo(string DateSf = "no")
        {
            if (DateSf == "no")
                DateSf = DateTime.Today.ToString("yyyyMMdd");

            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT TOP 1 Firm_Nm, rtrim(Firm_INN), rtrim(Firm_KPP), Firm_OKPO AS OKPO FROM U_vwChFirm WHERE Firm_Dat <= '" + DateSf + "' ORDER BY Firm_Dat DESC";
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

        public static object[] GetFirmAdr()
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select CrtAdr_StrNm+','+CrtAdr_House,CrtAdr_TowNm, CrtAdr_RegNm,CrtAdr_Ind, ISNULL(NULLIF(CrtAdr_RegCd,''),'74'), CrtAdr_StrNm, CrtAdr_House  from CRTADR WHERE CrtAdr_Cd = 1";
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

        public static object[] GetMasterFirmInfo()
        {
            int i = 0;
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select OptValue from U_CHZAKOPT where OptName ='ОБЩИЕ.ГЛАВНЫЙ НАЗВАНИЕ' "
                                +" union all "
                                +" select OptValue from U_CHZAKOPT where OptName = 'ОБЩИЕ.ГЛАВНЫЙ ИНН' "
                                +" union all "
                                +" select OptValue from U_CHZAKOPT where OptName = 'ОБЩИЕ.ГЛАВНЫЙ КПП' ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            object[] result = new object[3];
            while (dr.Read())
            {
                result[i] = dr.GetValue(0);
                i++;
            }
            conn.Close();
            return result;
        }

        public static object[] GetMasterFirmAdr()
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT ISNULL((SELECT OptValue FROM U_CHZAKOPT WHERE OptName ='ОБЩИЕ.ГЛАВНЫЙ АДРЕС'),''), ISNULL((SELECT OptValue FROM U_CHZAKOPT WHERE OptName ='ОБЩИЕ.ГЛАВНЫЙ ГОРОД'),''), ISNULL((SELECT OptValue FROM U_CHZAKOPT WHERE OptName ='ОБЩИЕ.ГЛАВНЫЙ ОБЛАСТЬ'),''), ISNULL((SELECT OptValue FROM U_CHZAKOPT WHERE OptName ='ОБЩИЕ.ГЛАВНЫЙ ИНДЕКС'),'')   , ISNULL((SELECT OptValue FROM U_CHZAKOPT WHERE OptName ='ОБЩИЕ.ГЛАВНЫЙ КОД РЕГИОНА'),'00') ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.FieldCount;
            object[] result = new object[n];
            while (dr.Read()) dr.GetValues(result);
            conn.Close();
            return result;
        }

        public static Boolean GetUseMasterGln(string PtnRcd)
        {
            //int i = 0;
            bool UseMG;
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "select RCD from U_CHGLNUSE where Ptn_rcd = "+PtnRcd+"";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            if (dr.Read())
            {
                //result[i] = dr.GetValue(0);
                //i++;
                dr.GetValues(result);
            }
            conn.Close();

            if (result[0] != null)
            {
                UseMG = true;
            }
            else
            {
                UseMG = false;
            }
            return UseMG;
        }

        public static object[] GetSigner()
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select VL_FIRSTNAMNE, "
                                   + "       VL_LASTNAME, "
                                   + "       VL_PATRONYMICNAME "
                                   + " from U_CHEDIVLPODP";
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

        public static int CountItemsInOrder(string DocRcd, int TypeHdr)//количество позиций в заказе
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select count(*) from  TRDS where trds_rcdhdr = '" + DocRcd + "' and TrdS_TypHdr = " + TypeHdr;
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
            int count = Convert.ToInt32(result[0]);
            return count;
        }

        public static object[] GetBuyerItemCode(string CdSpr, string Code)
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT ISNULL(UFS_Nm, 0) FROM UFLstSpr JOIN UFSpr ON UFSpr.UFS_Rcd = UFLstSpr.UFS_Rcd WHERE UFS_CdSpr = '"+CdSpr+"' and UFS_CdS = '"+Code+"' ";
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

        public static string CheckEnabledSentEdiDoc(string ptn_cd, string NameDoc)//ord-order, rst-ordrsp, des-desadv, rec-recavd,inv-invoice,pricat-pricelist
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = " select "+NameDoc+" from U_CHEDINASTDOC where Gpl_Rcd = (select Ptn_Rcd from PTNRK where Ptn_Cd = '"+ptn_cd+"')";
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
            string resultCheck = Convert.ToString(result[0]);
            return resultCheck;
        }

        public static object[] GetDataFromPtnRCD(Int64 ptnRcd, Int64 filiaRcd) // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование, 12 Полное наименование, 13 Признак отправки ВСД
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT Ptn_Cd\n"
           + "     , Ptn_NmSh\n"
           + "     , ISNULL(NULLIF(Filia_GLN,''), UFPRV1.UF_RkValS)\n"
           + "     , ISNULL(NULLIF(LTRIM(Ptn_Inn), ''), Filia_Fax) AS Ptn_Inn\n"
           + "     , Ptn_KPP\n"
           + "     , UFPRV2.UF_RkValS AS ProdCode\n"
           + "     , Filia_Adr\n"
           + "     , Filia_Index\n"
           + "     , ISNULL(NULLIF((SELECT TOP 1 SAdrReg_Cd FROM vwSAdrReg WHERE SAdrReg_CntRcd = Filia_Cnt AND SAdrReg_Rcd =  Filia_Rgn),''),'00') Filia_Rgn\n"
           + "     , '' City\n"
           + "     , '' Street\n"
           + "     , '' House\n"
           + "     , Ptn_Nm\n"
           + "     , ISNULL(DesVSD,0) DesVSD\n"
           + "     , (Select UF_RkValS From UFPRV where UF_TblId = 1126 AND UF_TblRcd = PTNRK.Ptn_Rcd AND UF_RkRcd = ( SELECT UFR_RkRcd FROM UFRKV WHERE UFR_DbRcd = 1126 AND UFR_Id = 'U_DOG_EDI')) NmrDog\n"
           + "     , (Select UF_RkValS From UFPRV where UF_TblId = 1126 AND UF_TblRcd = PTNRK.Ptn_Rcd AND UF_RkRcd = ( SELECT UFR_RkRcd FROM UFRKV WHERE UFR_DbRcd = 1126 AND UFR_Id = 'U_DATE_DOG_EDI')) DtDog\n"
           + "     , ISNULL(UFPRVGR.UF_RkValS, '')\n"
           + "FROM PTNRK\n"
           + "     JOIN PTNFILK ON PTNFILK.Ptn_Rcd = PTNRK.Ptn_Rcd\n"
           + "     LEFT JOIN U_CHEDINASTDOC AS sett ON PTNRK.Ptn_Rcd = sett.Gpl_Rcd\n"
           + "     INNER JOIN UFPRV AS UFPRV1 ON UFPRV1.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRV1.UF_TblId = 1126 AND UFPRV1.UF_RkRcd = ( SELECT UFR_RkRcd FROM UFRKV WHERE UFR_DbRcd = 1126 AND UFR_Id = 'U_GLN') "
           + "     LEFT JOIN UFPRV AS UFPRVGR ON UFPRVGR.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRVGR.UF_TblId = 1126 AND UFPRVGR.UF_RkRcd = ( SELECT UFR_RkRcd FROM UFRKV WHERE UFR_DbRcd = 1126 AND UFR_Id = 'U_GLNGR') "
           + "     LEFT JOIN UFPRV AS UFPRV2 WITH (NOLOCK) ON UFPRV2.UF_TblId = 1126 AND UFPRV2.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRV2.UF_RkRcd = ( SELECT UFR_RkRcd FROM UFRKV WHERE UFR_DbRcd = 1126 AND UFR_Id = 'U_PRODCODE')\n"
           + "WHERE PTNRK.Ptn_Rcd = " + ptnRcd.ToString() + "\n"
           + "  AND PTNFILK.Filia_Rcd = " + filiaRcd.ToString();

            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read()) dr.GetValues(result);
            string strAdr = result[6].ToString().Trim();

            if (strAdr != "")
            {
                //Разбивам адрес на массив строк используя разделитель "запятую"
                string[] arrAdr = strAdr.Split(new char[] { ',' }, 10, StringSplitOptions.RemoveEmptyEntries);
                string strCity = "";
                string strStreet = "";
                string strHouse = "";

                int param = 0;

                foreach (string s in arrAdr)
                {


                    if (s.Trim() == String.Empty)
                    {
                        continue;
                    }
                    if (param == 2)
                    {
                        switch (s.Trim().Substring(0, (s.Trim().Length < 2 ?s.Trim().Length: 2)).ToUpper())
                        {
                            case "Д.":
                                strHouse = s.Trim();
                                param = 3;
                                break;
                        }
                        if (strHouse.Length > 20) strHouse = strHouse.Substring(1, 20);
                    }
                    if (param == 1)
                    {
                        switch (s.Trim().Substring(0, (s.Trim().Length < 2 ? s.Trim().Length : 2)).ToUpper())
                        {
                            case "Ш.":
                                strStreet = s.Trim();
                                param = 2;
                                break;
                            case "УЛ":
                                strStreet = s.Trim();
                                param = 2;
                                break;
                            case "ПР":
                                strStreet = s.Trim();
                                param = 2;
                                break;
                            case "ПЛ":
                                strStreet = s.Trim();
                                param = 2;
                                break;
                        }
                        if (strStreet.Length > 50) strStreet = strStreet.Substring(1, 50);
                    }
                    if (param == 0)
                    {
                        switch (s.Trim().Substring(0, (s.Trim().Length < 2 ? s.Trim().Length : 2)).ToUpper())
                        {
                            case "Г.":
                                strCity = s.Trim();
                                param = 1;
                                break;
                            case "С.":
                                strCity = s.Trim();
                                param = 1;
                                break;
                            case "Н.":
                                strCity = s.Trim();
                                param = 1;
                                break;
                            case "П.":
                                strCity = s.Trim();
                                param = 1;
                                break;
                            case "Д.":
                                strCity = s.Trim();
                                param = 1;
                                break;
                        }
                        if (strCity.Length > 50) strCity = strCity.Substring(1, 50);
                    }
                }
                result[9] = strCity;
                result[10] = strStreet;
                result[11] = strHouse;
            }
            conn.Close();
            return result;
        }

        public static object[] GetDataFromPtnRCD_IP(Int64 ptnRcd, Int64 filiaRcd) // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование, 14 Корпус 15 квартира
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT Ptn_Cd\n"
           + "     , Ptn_NmSh\n"
           + "     , Filia_GLN\n"
           + "     , ISNULL(NULLIF(LTRIM(Ptn_Inn), ''), Filia_Fax) AS Ptn_Inn\n"
           + "     , Ptn_KPP\n"
           + "     , UFPRV2.UF_RkValS AS ProdCode\n"
           + "     , Filia_Adr\n"
           + "     , Filia_Index\n"
           + "     , (SELECT TOP 1 SAdrReg_Cd FROM vwSAdrReg WHERE SAdrReg_CntRcd = Filia_Cnt AND SAdrReg_Rcd =  Filia_Rgn) Filia_Rgn\n"
           + "     , '' City\n"
           + "     , '' Street\n"
           + "     , '' House\n"
           + "     , Ptn_Nm\n"
           + "     , ISNULL(FILIALFROM.UF_RkValD,0) as FILIALFROM\n"  //13
           + "     , '' Corpus\n"
           + "     , '' Flat\n"
           + "FROM PTNRK\n"
           + "     JOIN PTNFILK ON PTNFILK.Ptn_Rcd = PTNRK.Ptn_Rcd\n"
           + "     LEFT JOIN ufprv AS FILIALFROM ON FILIALFROM.UF_TblRcd = PTNRK.Ptn_Rcd AND FILIALFROM.UF_TblId = 1126 AND  FILIALFROM.UF_RkRcd = ( SELECT UFR_RkRcd FROM ufrkv WHERE UFR_Id = 'U_FILIAL_FROM' )"
           + "     LEFT JOIN UFPRV AS UFPRV2 WITH (NOLOCK) ON UFPRV2.UF_TblId = 1126 AND UFPRV2.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRV2.UF_RkRcd = ( SELECT UFR_RkRcd FROM UFRKV WHERE UFR_DbRcd = 1126 AND UFR_Id = 'U_PRODCODE')\n"
           + "WHERE PTNRK.Ptn_Rcd = " + ptnRcd.ToString() + "\n"
           + "  AND PTNFILK.Filia_Rcd = " + filiaRcd.ToString();

            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read()) dr.GetValues(result);
            string strAdr = result[6].ToString().Trim();

            if (strAdr != "")
            {
                //Разбивам адрес на массив строк используя разделитель "запятую"
                string[] arrAdr = strAdr.Split(new char[] { ',' }, 10, StringSplitOptions.RemoveEmptyEntries);
                string strCity = "";
                string strStreet = "";
                string strHouse = "";
                string strCorpus = "";
                string strFlat = "";

                int param = 0;

                foreach (string s in arrAdr)
                {
                    if (param == 4)
                    {
                        if (s.Length > 1)
                            switch (s.Substring(0, 3).ToUpper())
                            {
                                case "КВ.":
                                    strFlat = s;
                                    param = 5;
                                    break;
                                case "ЛИТ":
                                    strCorpus = strCorpus + " " + s;
                                    param = 5;
                                    break;
                            }
                        if (strCorpus.Length > 20) strCorpus = strCorpus.Substring(1, 20);
                        if (strFlat.Length > 20) strFlat = strFlat.Substring(1, 20);
                    }
                    if (param == 3)
                    {
                        if (s.Length > 1)
                            switch (s.Substring(0, 3).ToUpper())
                            {
                                case "КОР":
                                    strCorpus = s;
                                    param = 4;
                                    break;
                                case "КВ.":
                                    strFlat = s;
                                    param = 4;
                                    break;
                                case "ЛИТ":
                                    strCorpus = s;
                                    param = 4;
                                    break;
                                case "СТР":
                                    strHouse = strHouse + " " + s;
                                    param = 4;
                                    break;
                            }
                        if (strCorpus.Length > 20) strCorpus = strCorpus.Substring(1, 20);
                        if (strHouse.Length > 20) strHouse = strHouse.Substring(1, 20);
                        if (strFlat.Length > 20) strFlat = strFlat.Substring(1, 20);
                    }
                    if (param == 2)
                    {
                        if (s.Length > 1)         // ух ты жжжж   тут пришло без "Д." ... костыль точу.
                            switch (s.Substring(0, 2).ToUpper())
                            {
                                case "Д.":
                                    strHouse = s;
                                    param = 3;
                                    break;
                                case "СТ":
                                    strHouse = s;
                                    param = 3;
                                    break;
                            }
                        else
                        {
                            strHouse = s;
                            param = 3;
                            break;
                        }
                        if (strHouse.Length > 20) strHouse = strHouse.Substring(1, 20);
                    }
                    if (param == 1)
                    {
                        switch (s.Substring(0, 2).ToUpper())
                        {
                            case "Ш.":
                                strStreet = s;
                                param = 2;
                                break;
                            case "УЛ":
                                strStreet = s;
                                param = 2;
                                break;
                            case "ПР":
                                strStreet = s;
                                param = 2;
                                break;
                            case "ПЛ":
                                strStreet = s;
                                param = 2;
                                break;
                            case "МК":
                                strStreet = s;
                                param = 2;
                                break;
                            case "ТР":
                                strStreet = s;
                                param = 2;
                                break;
                            case "Б-":
                                strStreet = s;
                                param = 2;
                                break;
                        }
                        if (strStreet.Length > 50) strStreet = strStreet.Substring(1, 50);
                    }
                    if (param == 0)
                    {
                        switch (s.Substring(0, 2).ToUpper())
                        {
                            case "Г.":
                                strCity = s;
                                param = 1;
                                break;
                            case "С.":
                                strCity = s;
                                param = 1;
                                break;
                            case "Н.":
                                strCity = s;
                                param = 1;
                                break;
                            case "П.":
                                strCity = s;
                                param = 1;
                                break;
                            case "Д.":
                                strCity = s;
                                param = 1;
                                break;
                        }
                        if (strCity.Length > 50) strCity = strCity.Substring(1, 50);
                    }
                }
                result[9] = strCity;
                result[10] = strStreet;
                result[11] = strHouse;
                result[14] = strCorpus;
                result[15] = strFlat;
            }
            conn.Close();
            return result;
        }

        public static object[] GetHeaderSP(string Ptn_Cd)//возвращает 0-номер заказа; 1-дату заказа; 2-дату отгрузки; 3-'RUR'; 4-'O'; 5-номер заказа EDI; 6-rcd плательщика; 7-rcd грузополучателя
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select PrdZkg_nmr , "
                                   + " convert(date,PrdZkg_Dt) , "
                                   + " convert(date,PrdZkg_DtOtg) , "
                                   + " 'RUR' , "
                                   + " 'O' , "
                                   + " prdzkg_txt , "
                                   + " g.Ptn_Cd , "
                                   + " p.Ptn_Cd  "
                                   + " from PRDZKG "
                                   + " left join dbo.PTNRK g on g.Ptn_Rcd=PRDZKG_RcvrID "
                                   + " left join dbo.PTNRK p on p.Ptn_Rcd=PRDZKG_KAgID "
                                   + " where PrdZkg_Rcd= '" + Ptn_Cd + "' ";
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

        internal static string GetILNBuyer(string Ptn_Rcd)
        {
            int i = 0;
            string ILN;
            string connString = Settings.Default.ConnStringISPRO;
            string CMDGetDV = " SELECT TOP 1 UFPRV.UF_RkValS FROM PTNRK "  
	                            +" JOIN UFPRV ON UFPRV.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRV.UF_TblId = 1126 "  
	                            +" JOIN UFRKV ON UFRKV.UFR_DbRcd = UFPRV.UF_TblId AND UFRKV.UFR_RkRcd = UFPRV.UF_RkRcd  AND UFRKV.UFR_Id = 'U_GLN' "
	                            +" LEFT JOIN UFPRV as UFPRV2 WITH (NOLOCK) ON UFPRV2.UF_TblId = 1126 AND UFPRV2.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRV2.UF_RkRcd = ( SELECT UFR_RkRcd FROM UFRKV WHERE UFR_DbRcd = 1126 AND UFR_Id = 'U_PRODCODE' ) " 
	                            +" LEFT JOIN PTNFILK on PTNFILK .Ptn_Rcd=PTNRK.Ptn_Rcd " 
                                +" WHERE PTNRK.Ptn_Rcd = '"+Ptn_Rcd+"'";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(CMDGetDV, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())
            {
                result[i] = dr.GetValue(0);
                i++;
            }
            conn.Close();
            ILN = Convert.ToString(result[0]);
            return ILN;
        }

        public static string GetILNReceiver(string Ptn_Rcd)
        {
            int i = 0;
            string ILN; 
            string connString = Settings.Default.ConnStringISPRO;
            string CMDGetDV = " SELECT TOP 1 UFPRV.UF_RkValS FROM PTNRK "
                                + " JOIN UFPRV ON UFPRV.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRV.UF_TblId = 1126 "
                                + " JOIN UFRKV ON UFRKV.UFR_DbRcd = UFPRV.UF_TblId AND UFRKV.UFR_RkRcd = UFPRV.UF_RkRcd  AND UFRKV.UFR_Id = 'U_GLNGR' "
                                + " LEFT JOIN UFPRV as UFPRV2 WITH (NOLOCK) ON UFPRV2.UF_TblId = 1126 AND UFPRV2.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRV2.UF_RkRcd = ( SELECT UFR_RkRcd FROM UFRKV WHERE UFR_DbRcd = 1126 AND UFR_Id = 'U_PRODCODE' ) "
                                + " LEFT JOIN PTNFILK on PTNFILK .Ptn_Rcd=PTNRK.Ptn_Rcd "
                                + " WHERE PTNRK.Ptn_Rcd = '" + Ptn_Rcd + "'";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(CMDGetDV, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())
            {
                result[i] = dr.GetValue(0);
                i++;
            }
            conn.Close();
            ILN = Convert.ToString(result[0]);
            return ILN;
        }

        public static string GetPtnGroup(string Ptn_cd)
        {
            int i = 0;
            string PtnGroup;
            string connString = Settings.Default.ConnStringISPRO;
            string CMDGetDV = "select PtnGrp_Cdk from PTNRK k "
                              + " left join PTNGRPK g on g.PtnGrpk_Rcd = k.PtnGrpk_Rcd "
                              + " where Ptn_Cd = '" + Ptn_cd + "' ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(CMDGetDV, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read())
            {
                result[i] = dr.GetValue(0);
                i++;
            }
            conn.Close();
            PtnGroup = Convert.ToString(result[0]);
            return PtnGroup;
        }

        public static string GetJurOrder(string PtnGroup, string TypeSkln)
        {
            int i = 0;
            string Jur;
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "  select rtrim(ltrim(JR_Cd)) from U_CHSETTJURORD "
                        + " left join JR on JR_Rcd = Jur_rcd "
                        + " where Code_GrPtnrk ='"+PtnGroup+"' and TypeSkln = "+TypeSkln+"";
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
                Jur = Convert.ToString(result[0]);
            }
            else 
            {
                int j = 0;
                string default_sql = "select rtrim(ltrim(JR_Cd)) from U_CHDEFAULTJUR join JR on JR_Rcd = Default_Jur_Rcd where TypeSkln = " + TypeSkln + "";
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
                Jur = Convert.ToString(default_result[0]);
            }
            return Jur;
        }

        public static bool CheckExistsOrder(string NumEDI)
        {
            int i = 0;
            bool Exists;
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "select PrdZkg_Nmr from PRDZKG where PrdZkg_Txt = '"+NumEDI+"'  ";
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

            if ( result[0] != null)
            {
                Exists = true;
            }
            else
            {
                Exists = false;
            }
            return Exists;
        }

        public static object GetDefaultTypeId()
        {
            int i = 0;
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "select TypeSkln from U_CHSPTYPESKLN where TypeDesc = 'молоко'";
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
            object type = result[0];
            return type;
        }

        public static string GetNamePrice(string SklPrcRst_Rcd)
        {
            int i = 0;
            string Name;
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "select SklPrcRst_Sh from SKLPRCRST where SklPrcRst_Rcd = " + SklPrcRst_Rcd + "";
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

        public static string GetGLNGR(string Ptn_Cd)
        {
            int i = 0;
            string GLNGR;
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "SELECT UFPRV.UF_RkValS FROM PTNRK "
                        + "       JOIN UFPRV ON UFPRV.UF_TblRcd = PTNRK.Ptn_Rcd "
                        + "                     AND UFPRV.UF_TblId = 1126 "
                        + "       JOIN UFRKV ON UFRKV.UFR_DbRcd = UFPRV.UF_TblId "
                        + "                     AND UFRKV.UFR_RkRcd = UFPRV.UF_RkRcd "
                        + "                     AND UFRKV.UFR_Id = 'U_GLNGR' "
                        + "WHERE PTNRK.Ptn_Cd = '"+Ptn_Cd+"'";
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
                GLNGR = Convert.ToString(result[0]);
            }
            else
            {
                GLNGR = "NULL";
            }
            return GLNGR;
        }

        public static string GetRcdPtn(string gln)
        {
            int i = 0;
            string RcdPtn;
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "SELECT top 1 PTNRK.Ptn_Rcd FROM PTNRK "
                                + "     JOIN UFPRV ON UFPRV.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRV.UF_TblId = 1126"
                                + "     JOIN UFRKV ON UFRKV.UFR_DbRcd = UFPRV.UF_TblId AND UFRKV.UFR_RkRcd = UFPRV.UF_RkRcd" // AND UFRKV.UFR_Id = 'U_GLNGR'"
                                + "     JOIN PTNFILK on PTNFILK.Ptn_Rcd = PTNRK.Ptn_Rcd"
                                + "     WHERE UFPRV.UF_RkValS = '" + gln + "'";
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
                RcdPtn = Convert.ToString(result[0]);
            }
            else
            {
                RcdPtn = Convert.ToString(0);
            }
            return RcdPtn;
        }

        public static bool UsePCE(string Ptn_Cd)
        {
            bool UsePCE;
            int i = 0;
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "SELECT UF_RkValN FROM PTNRK "
                                   + "LEFT JOIN UFPRV ON UFPRV.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRV.UF_TblId = 1126 "
                                   + "JOIN UFRKV ON UFRKV.UFR_DbRcd = UFPRV.UF_TblId AND UFRKV.UFR_RkRcd = UFPRV.UF_RkRcd AND UFRKV.UFR_Id = 'U_OTG_SHT' "
                                   + "JOIN PTNFILK on PTNFILK.Ptn_Rcd = PTNRK.Ptn_Rcd "
                                   + " WHERE PTNRK.Ptn_Cd = '" + Ptn_Cd + "'";
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
                UsePCE = true;
            }
            else
            {
                UsePCE = false;
            }
            return UsePCE;
        }

        public static object[] GetUPDInfo(string Nak_Rcd)//возвращает номер сф и дату сф
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select SklSf_Nmr,SklSf_Dt from SKLSF JOIN TAXSFD on TaxSfd_SfID = SklSf_Rcd join SKLNK on SklNk_Rcd=TaxSfd_DocID  where SklNk_Rcd=" + Nak_Rcd + "";
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

        public static int CountItemsInInvoice(string DocRcd)//количество позиций в заказе
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select count(*) from  TRDS where trds_rcdhdr = '" + DocRcd + "'" ;
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
            int count = Convert.ToInt32(result[0]);
            return count;
        }

        public static object[] GetDataFromSF(Int64 ptnRcd) //Возвращает заголовок СФ
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT SklSf_Nmr\n"
               + "     , SklSf_Dt\n"
               + "	 , SklSf_KAgID\n"
               + "	 , SklSf_KAgAdr\n"
               + "	 , SklSf_RcvrID\n"
               + "	 , SklSf_RcvrAdr\n"
               + "   , ISNULL(NULLIF(SVl_CdISO,''),643) SVl_CdISO\n"
               + "FROM SKLSF\n"
               + "     LEFT JOIN SVL ON SVl_Rcd = SklSf_ValCd\n"
               + "WHERE SKLSF.SklSf_Rcd = " + ptnRcd.ToString();

            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();

            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read()) dr.GetValues(result);
            conn.Close();
            return result;
        }

        public static object[,] GetItemsFromSF(string docRcd, bool PCE)//rcd документа, bool PCE - маркер ЕИ штука.
        {
            string sql;
            string connString = Settings.Default.ConnStringISPRO;
            if (PCE == true) //надо все в штуках
            {
                sql = "SELECT ISNULL(LEFT(BarCode_Code,13),'') BarCode_Code --0  \n"
                    + "    , SklN_Rcd --1  \n"
                    + "    , SklN_Cd --2  \n"
                    + "    , SklN_NmAlt --3  \n"
                    + "    , TrdS_Qt * (TrdS_QtOsn/TrdS_Qt)/EISht.NmEi_QtOsn Qt  --4 кол-во   \n"
                    + "    , CONVERT( DECIMAL(18, 2), (TrdS_Cn * EISht.NmEi_QtOsn / TrdS_QtOsn * TrdS_Qt) / CASE WHEN ISNULL(Trx.TaxRate_Val,'0') = '0' THEN 1 ELSE 1 + CAST( RTRIM( LTRIM( Trx.TaxRate_Val ))AS FLOAT ) / 100  END) CnWoNds --5 Цена без НДС	\n"
                    + "	   , CONVERT( DECIMAL(18, 2), (TrdS_Cn * EISht.NmEi_QtOsn / TrdS_QtOsn * TrdS_Qt)) CnWthNds  --6 Цена с НДС  \n"
                    + "    , 'PCE' --7  \n"
                    + "    , EI_OKEI --8  \n"
                    + "    , ISNULL(TaxRate_Sh,'без НДС') Tax --9  \n"
                    + "    , 'S' --10  \n"
                    + "    , CONVERT( DECIMAL(18, 2), TrdS_SumTax) SumNds --11  \n"
                    + "    , CONVERT( DECIMAL(18, 2), TrdS_SumOpl) SumWthNds --12 \n"
                    + "    , EI_ShNm --13 \n"
                    + "    , TrdS_QtOsn --14\n"
                    + "FROM TRDS      \n"
                    + "     JOIN SKLN AS Nom ON Nom.SklN_Rcd = TrdS_RcdNom  \n"
                    + "     LEFT JOIN BarCode AS bc ON bc.BarCode_RcdNom = Nom.SklN_Rcd AND BARCODE_Base = 1  \n"
                    + "     LEFT JOIN EI ON EI_Rcd = 5  \n"
                    + "     LEFT JOIN SKLGR ON SklN_RcdGrp = SklGr_Rcd  \n"
                    + "	    LEFT JOIN DBO.ATRDSTAX AS Tax ON Tax.ATrdSTax_RcdS = TRDS.TrdS_Rcd AND Tax.ATrdSTax_RcdAs = 0\n"
                    + "     LEFT JOIN DBO.TAXRATE AS Trx ON Trx.TaxRate_Rcd = Tax.TrdSTax_RateCd AND Trx.TaxRate_RcdTax = Tax.TrdSTax_Cd\n"
                    + "	    LEFT JOIN SKLNOMEI AS EISht ON Nom.SklN_Rcd = EISht.NmEi_RcdNom AND EISht.NmEi_Cd = 5 \n"
                    + "WHERE trds_rcdhdr = " + docRcd + "\n"
                    + "  AND TrdS_TypHdr = 5\n"
                    + "  AND Trds_Qt > 0 AND TrdS_QtOsn > 0";
            }
            else
                sql = "SELECT ISNULL(LEFT(BarCode_Code,13),'') BarCode_Code --0  \n"
                    + "    , SklN_Rcd --1  \n"
                    + "    , SklN_Cd --2  \n"
                    + "    , SklN_NmAlt --3  \n"
                    + "    , TrdS_QtCn Qt  --4 кол-во   \n"
                    + "    , CONVERT( DECIMAL(18, 2), TrdS_Cn / CASE WHEN ISNULL(Trx.TaxRate_Val,'0') = '0' THEN 1 ELSE 1 + CAST( RTRIM( LTRIM( Trx.TaxRate_Val ))AS FLOAT ) / 100  END) CnWoNds --5 Цена без НДС	\n"
                    + "	   , CONVERT( DECIMAL(18, 2), TrdS_Cn ) CnWthNds  --6 Цена с НДС  \n"
                    + "    , CASE WHEN EI_Rcd = 1 THEN 'KG' WHEN EI_Rcd = 5 THEN 'PCE' WHEN EI_Rcd = 39 THEN 'CT' ELSE '' END --7  \n"
                    + "    , EI_OKEI --8  \n"
                    + "    , ISNULL(TaxRate_Sh,'без НДС') Tax --9  \n"
                    + "    , 'S' --10  \n"
                    + "    , CONVERT( DECIMAL(18, 2), TrdS_SumTax) SumNds --11  \n"
                    + "    , CONVERT( DECIMAL(18, 2), TrdS_SumOpl) SumWthNds --12 \n"
                    + "    , EI_ShNm --13 \n"
                    + "    , TrdS_QtOsn --14\n"
                    + "FROM TRDS      \n"
                    + "     JOIN SKLN AS Nom ON Nom.SklN_Rcd = TrdS_RcdNom  \n"
                    + "     LEFT JOIN BarCode AS bc ON bc.BarCode_RcdNom = Nom.SklN_Rcd AND BARCODE_Base = 1  \n"
                    + "     LEFT JOIN EI ON EI_Rcd = TrdS_EiCn  \n"
                    + "     LEFT JOIN SKLGR ON SklN_RcdGrp = SklGr_Rcd  \n"
                    + "	    LEFT JOIN DBO.ATRDSTAX AS Tax ON Tax.ATrdSTax_RcdS = TRDS.TrdS_Rcd AND Tax.ATrdSTax_RcdAs = 0\n"
                    + "     LEFT JOIN DBO.TAXRATE AS Trx ON Trx.TaxRate_Rcd = Tax.TrdSTax_RateCd AND Trx.TaxRate_RcdTax = Tax.TrdSTax_Cd\n"
                    + "	    --LEFT JOIN SKLNOMEI AS EISht ON Nom.SklN_Rcd = EISht.NmEi_RcdNom AND EISht.NmEi_Cd = 5 \n"
                    + "WHERE trds_rcdhdr = " + docRcd + "\n"
                    + "  AND TrdS_TypHdr = 5\n"
                    + "  AND Trds_Qt > 0 AND TrdS_QtOsn > 0";
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
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();

            int recordCount = 0;
            while (dr.Read()) recordCount++;
            object[,] result = new object[recordCount, dr.FieldCount];

            dr.Close();

            dr = command.ExecuteReader();

            int i = 0;
            while (dr.Read())
            {
                for (int j = 0; j < dr.FieldCount; j++) result[i, j] = dr.GetValue(j);
                i++;
            }
            conn.Close();
            return result;
        }


        public static string GetFldFromEdiExch(Int64 zakRcd, string fieldNm) //Возвращает нужное поле из протокола EDI по рсд заказа
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT TOP 1 U_ChEdiExch." + fieldNm + " \n"
               + "FROM U_ChEdiExch \n"
               + "WHERE U_ChEdiExch.Exch_ZkgRcd = " + zakRcd.ToString();

            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            string result = String.Empty;
            SqlCommand command = new SqlCommand(verification, conn);
            if (command.ExecuteScalar() != null)
                result = (string)command.ExecuteScalar().ToString();
                
            conn.Close();
            return result;
        }

        public static string GetBuyerItemCodeRcd(string sprCd, Int64 sklnRcd)
        {
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "SELECT ISNULL((SELECT TOP 1 UFS_Nm FROM UFLstSpr JOIN UFSpr ON UFSpr.UFS_Rcd = UFLstSpr.UFS_Rcd WHERE UFS_CdSpr = " + sprCd + " and UFS_CdS = (SELECT LEFT(SklN_NmAlt,PATINDEX('%[ ._]%',SklN_NmAlt)-1) FROM SklN WHERE SklN_Rcd = " + sklnRcd.ToString() + ")),'')";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            string result = (string)command.ExecuteScalar().ToString();
            conn.Close();
            return result;
        }

        public static string[] GetSignerOpt()
        {
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "select ISNULL(VL_DOL, ''),ISNULL(VL_LASTNAME,''), ISNULL(VL_FIRSTNAMNE, ''), "
                                   + "       ISNULL(VL_PATRONYMICNAME,''), 'Должностные обязанности' "
                                   + " from U_CHEDIVLPODP";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            string[] result = new string[dr.FieldCount];
            while (dr.Read())
            {
                dr.GetValues(result);
            }
            conn.Close();
            return result;
        }

        public static object[,] GetItemsFromKSF(string docKorrSfRcd, string docSfRcd, bool PCE)//rcd документа, bool PCE - маркер ЕИ штука.
        {
            string sql;
            string connString = Settings.Default.ConnStringISPRO;

            /*Получаем список позиций указанного корректировочного СФ*/
            sql = "SELECT SklN_Rcd  --0 Рсд номенклатуры\n"
           + "    , ISNULL(LEFT(BarCode_Code,13),'') BarCode_Code  --1 ШК\n"
           + "	, SklN_NmAlt --2 Наименование   \n"
           + "FROM TRDS\n"
           + "    /* номенклатура */\n"
           + "    JOIN SKLN ON SklN_Rcd = TrdS_RcdNom \n"
           + "	LEFT JOIN BarCode AS bc ON bc.BarCode_RcdNom = TrdS.TrdS_RcdNom AND BARCODE_Base = 1\n"
           + "WHERE ABS(TrdS_QtCn) > 0 AND TrdS_RcdHdr = " + docKorrSfRcd + " AND TrdS_TypHdr = 5\n"
           + "ORDER BY SklN_Rcd";

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
            SqlCommand command = new SqlCommand(sql, conn);

            /*Считаем кол-во записей*/
            SqlDataReader dr = command.ExecuteReader();
            int recordCount = 0;
            while (dr.Read()) recordCount++;
            dr.Close();

            /*Обявляем массив для результирующе спецификации и заполняем реквизиты номенклатуры 0 - рсд, 1 - шк, 2- название*/
            object[,] result = new object[recordCount, 23];
            dr = command.ExecuteReader();
            string strNomRcd = string.Empty;
            for (int iRow = 0; iRow < recordCount; iRow++)
            {
                dr.Read();
                result[iRow, 0] = dr.GetValue(0);
                result[iRow, 1] = dr.GetValue(1);
                result[iRow, 2] = dr.GetValue(2);
            }
            dr.Close();

            for (int iRow = 0; iRow < recordCount; iRow++)
            {
                /*Запрашиваем спецификаций всех зависимых документов (Tp = 0 - отгрузочная СФ, 1 - предыдущие корретировки и 3 - текущая корректировка)*/
                sql = "DECLARE @KorrSfRcd BIGINT = " + docKorrSfRcd + "\n"
                    + "DECLARE @SfRcd BIGINT = " + docSfRcd + "\n"
                    + "\n"
                    + "DECLARE @KorrSfDt DATETIME\n"
                    + "DECLARE @KorrSfId BIGINT\n"
                    + "\n"
                    + "/*Получаем время и маркет текущего КоррСФ*/\n"
                    + "SELECT @KorrSfDt = SklNk_Dat + CONVERT(TIME,SklNk_Tim), @KorrSfId = SKLNK.bookmark\n"
                    + "FROM SKLSF\n"
                    + "	 JOIN TAXSFD ON TaxSfd_SfID = SklSf_Rcd\n"
                    + "	 JOIN SKLNK ON SklNk_Rcd = TaxSfd_DocID\n"
                    + "WHERE SklSf_Rcd = @KorrSfRcd\n"
                    + "\n"
                    + "IF ISNULL(@KorrSfId,0) > 0 \n"
                    + "BEGIN\n"
                    + "  DECLARE @TABLESF TABLE (ID BIGINT, Tp TINYINT)\n"
                    + "  INSERT INTO @TABLESF(ID,Tp) VALUES(@SfRcd,0)\n"
                    + "  INSERT INTO @TABLESF(ID,Tp) VALUES(@KorrSfRcd,3)\n"
                    + "\n"
                    + "  /*Находим предыдущие КоррСФ у которых время и маркер меньше текущего*/\n"
                    + "  INSERT INTO @TABLESF(ID,Tp)\n"
                    + "  SELECT DopKorrSf.SklSf_Rcd, 1\n"
                    + "  FROM SKLSF DopKorrSf\n"
                    + "       JOIN SKLSFA ON SKLSFA.SklSf_Rcd = DopKorrSf.SklSf_Rcd AND SklSfA_RcdCor = @SfRcd AND SklSfA_RcdDopL = 0\n"
                    + "       JOIN TAXSFD ON TaxSfd_SfID = DopKorrSf.SklSf_Rcd\n"
                    + "	      JOIN SKLNK ON SklNk_Rcd = TaxSfd_DocID\n"
                    + "  WHERE DopKorrSf.SklSf_Rcd <> @KorrSfRcd\n"
                    + "    AND SklNk_Dat + CONVERT(TIME,SklNk_Tim) <= @KorrSfDt\n"
                    + "    AND SklNk.bookmark < @KorrSfId\n"
                    + "\n";
                if (PCE == true) //надо все в штуках
                    sql = sql + "  SELECT TrdS_RcdNom --0 Рсд номенклатуры\n"
                    + "    , 0\n"
                    + "	   , 0\n"
                    + "    , EICn.EI_SHNM AS EiSh --3-A_EiSh\n"
                    + "    , EICn.EI_OKEI AS EiOkei --4-A_EiOkei\n"
                    + "	   , CASE WHEN EI_Rcd = 1 THEN 'KG' WHEN EI_Rcd = 5 THEN 'PCE' WHEN EI_Rcd = 39 THEN 'CT' ELSE '' END EiEdi --5-A_EiEdi\n"
                    + "    , CONVERT(INT,EICn.EI_Acc) AS EiAcc --6-A_EiAcc	      \n"
                    + "    , TrdS_QtOsn QtOsn --7-A_QtOsn\n"
                    + "    , TrdS_QtCn * (TrdS_QtOsn/TrdS_QtCn)/NomEiSht.NmEi_QtOsn QtCn  --8-A_QtCn\n"
                    + "	   , ROUND((TrdS_Cn * NomEiSht.NmEi_QtOsn / TrdS_QtOsn * TrdS_QtCn) / CASE WHEN ISNULL(TAXRATE.TaxRate_Val,'0') = '0' THEN 1 ELSE 1 + CAST( RTRIM( LTRIM( TAXRATE.TaxRate_Val ))AS FLOAT ) / 100  END, 2) CnWoNds --9-A_CnWoNds      \n"
                    + "    , ISNULL(TaxRate_Sh,'без НДС') Tax --10-A_Tax\n"
                    + "    , TrdS_SumTax SummNds --11-A_SummNds\n"
                    + "    , TrdS_SumOpl SummWthNds --12-A_SummWthNds\n"
                    + "	   , TblRcdSf.Tp --13 0-корректируемый сф, 1-предыдущий корректировочный сф, 3-текущий корректировочный сф\n"
                    + "  FROM TRDS\n"
                    + "       JOIN @TABLESF TblRcdSf ON TblRcdSf.ID = TrdS_RcdHdr AND TrdS_TypHdr = 5\n"
                    + "       /*еи цены*/\n"
                    + "       JOIN EI AS EICn ON EICn.EI_Rcd = 5\n"
                    + "	   LEFT JOIN SKLNOMEI AS NomEiSht ON TRDS.TrdS_RcdNom = NomEiSht.NmEi_RcdNom AND NomEiSht.NmEi_Cd = EICn.EI_Rcd \n"
                    + "       /*Цены и налоги*/\n"
                    + "       LEFT JOIN ATRDSTAX ON TrdS_Rcd = ATrdSTax_RcdS AND ATrdSTax_RcdAs = 0\n"
                    + "       LEFT JOIN TAXRATE ON TrdSTax_RateCd = TaxRate_Rcd AND TrdSTax_Cd = TaxRate_RcdTax	   \n"
                    + "  WHERE ABS(TrdS_QtCn) > 0 AND TrdS_RcdNom = " + result[iRow, 0].ToString() + "\n"
                    + "  ORDER BY TrdS_RcdNom, TblRcdSf.Tp\n"
                    + "END";
                else
                    sql = sql + "  SELECT TrdS_RcdNom --0 Рсд номенклатуры\n"
                    + "    , 0\n"
                    + "	   , 0\n"
                    + "    , EICn.EI_SHNM AS EiSh --3-A_EiSh\n"
                    + "    , EICn.EI_OKEI AS EiOkei --4-A_EiOkei\n"
                    + "	   , CASE WHEN EI_Rcd = 1 THEN 'KG' WHEN EI_Rcd = 5 THEN 'PCE' WHEN EI_Rcd = 39 THEN 'CT' ELSE '' END EiEdi --5-A_EiEdi\n"
                    + "    , CONVERT(INT,EICn.EI_Acc) AS EiAcc --6-A_EiAcc	      \n"
                    + "    , TrdS_QtOsn QtOsn --7-A_QtOsn\n"
                    + "    , TrdS_QtCn  QtCn  --8-A_QtCn\n"
                    + "	   , ROUND(TrdS_Cn / CASE WHEN ISNULL(TAXRATE.TaxRate_Val,'0') = '0' THEN 1 ELSE 1 + CAST( RTRIM( LTRIM( TAXRATE.TaxRate_Val ))AS FLOAT ) / 100  END, 2) CnWoNds --9-A_CnWoNds      \n"
                    + "    , ISNULL(TaxRate_Sh,'без НДС') Tax --10-A_Tax\n"
                    + "    , TrdS_SumTax SummNds --11-A_SummNds\n"
                    + "    , TrdS_SumOpl SummWthNds --12-A_SummWthNds\n"
                    + "	   , TblRcdSf.Tp --13 0-корректируемый сф, 1-предыдущий корректировочный сф, 3-текущий корректировочный сф \n"
                    + "  FROM TRDS\n"
                    + "       JOIN @TABLESF TblRcdSf ON TblRcdSf.ID = TrdS_RcdHdr AND TrdS_TypHdr = 5\n"
                    + "       /*еи цены*/\n"
                    + "       JOIN EI AS EICn ON TrdS_EiCn = EICn.EI_Rcd\n"
                    + "       /*Цены и налоги*/\n"
                    + "       LEFT JOIN ATRDSTAX ON TrdS_Rcd = ATrdSTax_RcdS AND ATrdSTax_RcdAs = 0\n"
                    + "       LEFT JOIN TAXRATE ON TrdSTax_RateCd = TaxRate_Rcd AND TrdSTax_Cd = TaxRate_RcdTax\n"
                    + "  WHERE ABS(TrdS_QtCn) > 0 AND TrdS_RcdNom = " + result[iRow, 0].ToString() + "\n"
                    + "  ORDER BY TrdS_RcdNom, TblRcdSf.Tp\n"
                    + "END";

                /*Проходим по всем записям и расчитываем графы А и В для УКД*/
                //result 0-Рсд, 1-ШК, 2-Название,  3-A_EiSh,  4-A_EiOkei,  5-A_EiEdi,  6-A_EiAcc,  7-A_QtOsn,  8-A_QtCn,  9-A_CnWoNds, 10-A_Tax, 11-A_SummNds, 12-A_SummWthNds 
                //                              , 13-B_EiSh, 14-B_EiOkei, 15-B_EiEdi, 16-B_EiAcc, 17-B_QtOsn, 18-B_QtCn, 19-B_CnWoNds, 20-B_Tax, 21-B_SummNds, 22-B_SummWthNds

                //dr --0 Рсд номенклатуры, 3-EiSh, 4-EiOkei, 5-EiEdi, 6-EiAcc, 7-QtOsn, 8-QtCn, 9-CnWoNds, 10-Tax, 11-SummNds, 12-SummWthNds, 13- 0корректируемый сф 1предыдущий корректировочный сф 3текущий корректировочный сф
                command = new SqlCommand(sql, conn);
                dr = command.ExecuteReader();
                decimal qtOsn = 0;
                decimal qtCn = 0;
                while (dr.Read())
                {
                    if (dr.GetByte(13) != 3) /*Если это спецификация предыдущих (0 корректируемая СФ или 1 предыдущая корректировочная СФ) то заполняем графу А значения result с 3 по 12*/
                    {
                        if (result[iRow, 3] != null) //Если графа А уже заполнена, то это (1 предыдущая корректировочная СФ), корректируем графу А с учетом изменений
                        {
                            qtOsn = Convert.ToDecimal(result[iRow, 7]) + dr.GetDecimal(7); //QtOsn графы А + QtOsn предыдущей корректировки
                            qtCn = Math.Round(qtOsn / (dr.GetDecimal(7) / dr.GetDecimal(8)), 2); ////Переводим разницу кол-во в еи предыдущей корректировки (Отношение QtOsn/QtCn предыдущей корректировки)
                            if (((Math.Round(qtCn - Math.Truncate(qtCn), 2) == 0) && (dr.GetInt32(6) == 0)) | (dr.GetInt32(6) != 0)) //Если разница кол-во в еи предыдущей корректировки перевелось в соответствии с точностью ЕИ то принимаем это значение
                            {
                                result[iRow, 3] = dr.GetValue(3);
                                result[iRow, 4] = dr.GetValue(4);
                                result[iRow, 5] = dr.GetValue(5);
                                result[iRow, 6] = dr.GetValue(6);
                                result[iRow, 7] = qtOsn;
                                result[iRow, 8] = qtCn;
                                result[iRow, 9] = dr.GetValue(9);
                                result[iRow, 10] = dr.GetValue(10);
                            }
                            else //Если не перевелось то переводим в еи графы А
                            {
                                result[iRow, 8] = qtOsn / (Convert.ToDecimal(result[iRow, 7]) / Convert.ToDecimal(result[iRow, 8]));
                                result[iRow, 7] = qtOsn;
                            }
                            result[iRow, 11] = Convert.ToDecimal(result[iRow, 11]) + dr.GetDecimal(11);
                            result[iRow, 12] = Convert.ToDecimal(result[iRow, 12]) + dr.GetDecimal(12);
                        }
                        else //Если графа А пустая то копируем данные из запроса в эту графу (0 корректируемая СФ)
                        {
                            for (int iCol = 3; iCol < 13; iCol++) result[iRow, iCol] = dr.GetValue(iCol);
                        }


                    }
                    else /*Если это текущая корректировочная то заполняем графу B значения result с 13 по 22*/
                    {
                        qtOsn = Convert.ToDecimal(result[iRow, 7]) + dr.GetDecimal(7); //QtOsn графы А + QtOsn текущей корректировки
                        qtCn = Math.Round(qtOsn / (dr.GetDecimal(7) / dr.GetDecimal(8)), 2); //Переводим разницу кол-во в еи текущей корректировки (Отношение QtOsn/QtCn текущей корректировки)
                        if (((Math.Round(qtCn - Math.Truncate(qtCn), 2) == 0) && (dr.GetInt32(6) == 0)) | (dr.GetInt32(6) != 0)) //Если разница кол-во в еи текущей корректировки перевелось в соответствии с точностью ЕИ то принимаем это значение
                        {
                            result[iRow, 13] = dr.GetValue(3);
                            result[iRow, 14] = dr.GetValue(4);
                            result[iRow, 15] = dr.GetValue(5);
                            result[iRow, 16] = dr.GetValue(6);
                            result[iRow, 17] = qtOsn;
                            result[iRow, 18] = qtCn;
                            result[iRow, 19] = dr.GetValue(9);
                            result[iRow, 20] = dr.GetValue(10);
                        }
                        else //Если не перевелось то переводим в еи графы А
                        {
                            result[iRow, 13] = result[iRow, 3];
                            result[iRow, 14] = result[iRow, 4];
                            result[iRow, 15] = result[iRow, 5];
                            result[iRow, 16] = result[iRow, 6];
                            result[iRow, 18] = qtOsn / (Convert.ToDecimal(result[iRow, 7]) / Convert.ToDecimal(result[iRow, 8]));
                            result[iRow, 17] = qtOsn;
                            result[iRow, 19] = result[iRow, 9];
                            result[iRow, 20] = result[iRow, 10];
                        }
                        result[iRow, 21] = Convert.ToDecimal(result[iRow, 11]) + dr.GetDecimal(11);
                        result[iRow, 22] = Convert.ToDecimal(result[iRow, 12]) + dr.GetDecimal(12);
                    }
                }
                dr.Close();
            }

            conn.Close();
            return result;
        }
        public static string GetPrevSfToKSF(string docKorrSfRcd, string docSfRcd)
        {
            string sql;
            string connString = Settings.Default.ConnStringISPRO;
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



            sql = "DECLARE @KorrSfRcd BIGINT = " + docKorrSfRcd + "\n"
                    + "DECLARE @SfRcd BIGINT = " + docSfRcd + "\n"
                    + "\n"
                    + "DECLARE @KorrSfDt DATETIME\n"
                    + "DECLARE @KorrSfId BIGINT\n"
                    + "\n"
                    + "/*Получаем время и маркет текущего КоррСФ*/\n"
                    + "SELECT @KorrSfDt = SklNk_Dat + CONVERT(TIME,SklNk_Tim), @KorrSfId = SKLNK.bookmark\n"
                    + "FROM SKLSF\n"
                    + "	 JOIN TAXSFD ON TaxSfd_SfID = SklSf_Rcd\n"
                    + "	 JOIN SKLNK ON SklNk_Rcd = TaxSfd_DocID\n"
                    + "WHERE SklSf_Rcd = @KorrSfRcd\n"
                    + "\n"
                    + "  /*Находим предыдущие КоррСФ у которых время и маркер меньше текущего*/\n"
                    + "  SELECT DopKorrSf.SklSf_Rcd, DopKorrSf.SklSf_Nmr, DopKorrSf.SklSf_Dt\n"
                    + "  FROM SKLSF DopKorrSf\n"
                    + "       JOIN SKLSFA ON SKLSFA.SklSf_Rcd = DopKorrSf.SklSf_Rcd AND SklSfA_RcdCor = @SfRcd AND SklSfA_RcdDopL = 0\n"
                    + "       JOIN TAXSFD ON TaxSfd_SfID = DopKorrSf.SklSf_Rcd\n"
                    + "	   JOIN SKLNK ON SklNk_Rcd = TaxSfd_DocID\n"
                    + "  WHERE DopKorrSf.SklSf_Rcd <> @KorrSfRcd\n"
                    + "    AND SklNk_Dat + CONVERT(TIME,SklNk_Tim) <= @KorrSfDt\n"
                    + "    AND SklNk.bookmark < @KorrSfId\n";

            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();

            string result = "";

            while (dr.Read())
            {
                result = result + " корректировочный " + dr.GetValue(1).ToString() + " от " + dr.GetDateTime(2).ToString("dd.MM.yyyy") + ",";
            }
            dr.Close();

            if (result.Length > 2) result = result.Substring(0, result.Length - 2);

            conn.Close();
            return result;
        }

        public static object[,] GetPrevSfToKSFAsOb(string docKorrSfRcd, string docSfRcd)
        {
            string sql;
            string connString = Settings.Default.ConnStringISPRO;
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



            sql = "DECLARE @KorrSfRcd BIGINT = " + docKorrSfRcd + "\n"
                    + "DECLARE @SfRcd BIGINT = " + docSfRcd + "\n"
                    + "\n"
                    + "DECLARE @KorrSfDt DATETIME\n"
                    + "DECLARE @KorrSfId BIGINT\n"
                    + "\n"
                    + "/*Получаем время и маркет текущего КоррСФ*/\n"
                    + "SELECT @KorrSfDt = SklNk_Dat + CONVERT(DATETIME,SklNk_Tim), @KorrSfId = SKLNK.bookmark\n"
                    + "FROM SKLSF\n"
                    + "	 JOIN TAXSFD ON TaxSfd_SfID = SklSf_Rcd\n"
                    + "	 JOIN SKLNK ON SklNk_Rcd = TaxSfd_DocID\n"
                    + "WHERE SklSf_Rcd = @KorrSfRcd\n"
                    + "\n"
                    + "  /*Находим предыдущие КоррСФ у которых время и маркер меньше текущего*/\n"
                    + "  SELECT DopKorrSf.SklSf_Rcd, DopKorrSf.SklSf_Nmr, DopKorrSf.SklSf_Dt\n"
                    + "  FROM SKLSF DopKorrSf\n"
                    + "       JOIN SKLSFA ON SKLSFA.SklSf_Rcd = DopKorrSf.SklSf_Rcd AND SklSfA_RcdCor = @SfRcd AND SklSfA_RcdDopL = 0\n"
                    + "       JOIN TAXSFD ON TaxSfd_SfID = DopKorrSf.SklSf_Rcd\n"
                    + "	   JOIN SKLNK ON SklNk_Rcd = TaxSfd_DocID\n"
                    + "  WHERE DopKorrSf.SklSf_Rcd <> @KorrSfRcd\n"
                    + "    AND SklNk_Dat + CONVERT(DATETIME,SklNk_Tim) <= @KorrSfDt\n"
                    + "    AND SklNk.bookmark < @KorrSfId\n"
                    + "  ORDER BY SklNk_Dat, SklNk_Tim, SklNk.bookmark DESC\n";


            SqlCommand command = new SqlCommand(sql, conn);

            /*Считаем кол-во записей*/
            SqlDataReader dr = command.ExecuteReader();
            int recordCount = 0;
            while (dr.Read()) recordCount++;
            dr.Close();

            /*Обявляем массив для результирующе спецификации и заполняем реквизиты номенклатуры 0- rcd, 1 - Номер, 2 - Дата*/
            object[,] result = new object[recordCount, 3];
            dr = command.ExecuteReader();
            //string strNomRcd = string.Empty;
            for (int iRow = 0; iRow < recordCount; iRow++)
            {
                dr.Read();
                result[iRow, 0] = dr.GetValue(0);
                result[iRow, 1] = dr.GetValue(1);
                result[iRow, 2] = dr.GetValue(2);
            }
            dr.Close();

            /*SqlCommand command = new SqlCommand(sql, conn);                 
            SqlDataReader dr = command.ExecuteReader();                
                
            string result = "";

            while (dr.Read())
            {
                result = result + " корректировочный " + dr.GetValue(1).ToString() + " от " + dr.GetDateTime(2).ToString("dd.MM.yyyy") + ",";
            }
            dr.Close();

            if (result.Length > 2) result = result.Substring(0,result.Length-2);         
            */
            conn.Close();
            return result;
        }

        public static DateTime Verification_NastDoc(string typDoc, string gplRcd) //проверка назначен ли данный тип документа для грузополучателя
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT SprDoc_ShNast FROM U_ChEdiSprDoc WHERE SprDoc_Std = '" + typDoc + "'";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            string fieldNm = (string)command.ExecuteScalar().ToString();

            DateTime result = DateTime.MinValue;

            if (fieldNm != "")
            {
                try
                {
                    verification = "SELECT " + fieldNm + "Dt from U_ChEdiNastDoc WHERE U_ChEdiNastDoc." + fieldNm + " = 1 AND U_ChEdiNastDoc.Gpl_Rcd = " + gplRcd;
                    command = new SqlCommand(verification, conn);
                    result = (DateTime)command.ExecuteScalar();
                }
                catch
                {
                    result = DateTime.MinValue;
                }
            }
            else result = DateTime.MinValue;
            conn.Close();
            return result;
        }

        /*
          Проверка на существование  элемента в xml структуре
        */
        public static string getInnerTextforXmlNode(XmlNode xmlN)
        {
            string result;

            if (xmlN != null)
            {
                result = xmlN.InnerText;
            }
            else
            {
                result = String.Empty;
            }

            return result;
        }

        /*
          Получает позиции по расходной накладной
          string ordNumber  - номер заказа
          string despatchNumber  - номер накладной или сф
          string  measure - единица измерения. Возможные значения:PCE - штука, PA - коробка, KGM - кг, GRM - гр, PF - палета
        */
        public static object[,] GetItemsFromSklnk(string ordNumber, string despatchNumber, string  measure = "PCE")
        {
            string sql;
            string connString = Settings.Default.ConnStringISPRO;
            int EiRcd;
            switch (measure)
            {
                case "PCE":
                    EiRcd = 5;
                    break;
                case "KGM":
                    EiRcd = 1;
                    break;
                case "PA":
                    EiRcd = 39;
                    break;
                default:
                    EiRcd = 5;
                    break;
            }
            sql = " SELECT   \n"
                + " BarCode_Code --0 \n"
                + " , SklN_Rcd --1  \n"
                + " , SklN_Cd --2   \n"
                + " , SklN_NmAlt --3  \n"
                + " , SUM(Qt) Qt  --4 кол-во   \n"
                + " , CnWoNds --5 Цена без НДС	\n"
                + " , CnWthNds  --6 Цена с НДС  \n"
                + " , '" + measure + "' --7  \n"
                + " , EI_OKEI --8  \n"
                + " , Tax --9  \n"
                + " , 'S' --10  \n"
                + " , SUM(SumNds)  SumNds--11  \n"
                + " , SUM(SumWthNds) SumWthNds--12 \n"
                + " , EI_ShNm--13 \n"
                + " , SUM(TrdS_QtOsn) TrdS_QtOsn--14 \n"
                + " , NULL --15 \n"
                + " , SklnCd --16 \n"
                + " , Nm1 --17 \n"
                + " , Sklnk_Nmr   --18 \n"
                + " , Sklnk_Dat   --19 \n"
                + " , Sklnk_Rcd \n"
                + " FROM \n"
                + " (    \n"
                + "     SELECT   \n"
                + "       ISNULL(BarCode_Code,'') BarCode_Code \n"
                + "     , SklN_Rcd  \n"
                + "     , SklN_Cd   \n"
                + "     , SklN_NmAlt  \n"
                + "     , (CASE WHEN SKLNK_Mov = 1 then (-1) ELSE 1 END) * TrdS_Qt * (TrdS_QtOsn/TrdS_Qt)/EISht.NmEi_QtOsn Qt  --4 кол-во   \n"
                + "     , CONVERT( DECIMAL(18, 2), (TrdS_Cn * EISht.NmEi_QtOsn / TrdS_QtOsn * TrdS_Qt) / CASE WHEN ISNULL(Trx.TaxRate_Val,'0') = '0' THEN 1 ELSE 1 + CAST( RTRIM( LTRIM( Trx.TaxRate_Val ))AS FLOAT ) / 100  END) CnWoNds --5 Цена без НДС	\n"
                + "     , CONVERT( DECIMAL(18, 2), (TrdS_Cn * EISht.NmEi_QtOsn / TrdS_QtOsn * TrdS_Qt)) CnWthNds  --6 Цена с НДС  \n"
                + "     , EI_OKEI --8  \n"
                + "     , ISNULL(TaxRate_Sh,'без НДС') Tax --9  \n"
                + "     , (CASE WHEN SKLNK_Mov = 1 then (-1) ELSE 1 END) * CONVERT( DECIMAL(18, 2), TrdS_SumTax) SumNds --11  \n"
                + "     , (CASE WHEN SKLNK_Mov = 1 then (-1) ELSE 1 END) * CONVERT( DECIMAL(18, 2), TrdS_SumOpl) SumWthNds --12 \n"
                + "     , EI_ShNm--13 \n"
                + "     , (CASE WHEN SKLNK_Mov = 1 then (-1) ELSE 1 END) * TrdS_QtOsn TrdS_QtOsn --14 \n"
                + "     , Substring(SklN_NmAlt, 0, CHARINDEX(' ',SklN_NmAlt)) SklnCd --16 \n"
                + "     , (SELECT UFS_NmK from UFSpr where UFS_Rcd = (SELECT top 1 UFLstSpr.UFS_Rcd FROM UFLstSpr JOIN UFSpr ON UFSpr.UFS_Rcd = UFLstSpr.UFS_Rcd WHERE UFS_CdSpr = "
                + "         (select UFPRV.UF_RkValS from Ptnrk LEFT JOIN UFPRV as UFPRV WITH(NOLOCK) ON UFPRV.UF_TblId = 1126 AND UFPRV.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRV.UF_RkRcd = (SELECT UFR_RkRcd FROM UFRKV WHERE UFR_DbRcd = 1126 AND UFR_Id = 'U_PRODCODE' ) \n"
                + "        where Ptn_Rcd = SklNk_KAgRcd))  and UFS_CdS = Substring(SklN_NmAlt, 0, CHARINDEX(' ', SklN_NmAlt))) Nm1 --17 \n"
                + "     , CASE WHEN s1.SklNk_RcdNak = 0 THEN s1.Sklnk_Nmr ELSE (SELECT s2.SklNk_Nmr From SKLNK s2 where s2.SklNk_Rcd = s1.SklNk_RcdNak) END SklNk_Nmr  --18 \n"
                + "     , CASE WHEN SklNk_RcdNak = 0 THEN Sklnk_Dat ELSE (SELECT s3.Sklnk_Dat From SKLNK s3 where s3.SklNk_Rcd = s1.SklNk_RcdNak) END  Sklnk_Dat --19 \n"
                + "     , CASE WHEN SklNk_RcdNak = 0 THEN SklNk_Rcd ELSE SklNk_RcdNak END Sklnk_Rcd   --20 \n"
                + "     FROM PRDZKG \n"
                + "     INNER JOIN SKLNk s1 ON s1.SklNk_RcdZkg = PrdZkg_Rcd \n"
                + "     LEFT join TAXSFD ON TaxSfd_DocID = case when SklNk_RcdNak = 0 THEN  SklNk_Rcd ELSE SklNk_RcdNak END \n"
                + "     LEFT join SKLSF on TaxSfd_SfID=SklSf_Rcd   /*СФ*/ \n"
                + "     INNER JOIN TRDS ON TrdS_RcdHdr = SklNk_Rcd \n"
                + "     INNER JOIN SKLN AS Nom ON Nom.SklN_Rcd = TrdS_RcdNom \n"
                + "     LEFT JOIN BarCode AS bc ON bc.BarCode_RcdNom = Nom.SklN_Rcd AND BARCODE_Base = 1   /*основной штрихкод*/ \n"
                + "     LEFT JOIN EI ON EI_Rcd = " + EiRcd.ToString() + " \n"
                + "     LEFT JOIN DBO.ATRDSTAX AS Tax ON Tax.ATrdSTax_RcdS = TRDS.TrdS_Rcd AND Tax.ATrdSTax_RcdAs = 0 \n"
                + "     LEFT JOIN DBO.TAXRATE AS Trx ON Trx.TaxRate_Rcd = Tax.TrdSTax_RateCd AND Trx.TaxRate_RcdTax = Tax.TrdSTax_Cd \n"
                + "     LEFT JOIN SKLNOMEI AS EISht ON EISht.NmEi_Cd =  " + EiRcd.ToString() + "  and EISht.NmEi_RcdNom = Nom.SklN_Rcd  \n"
                + "     WHERE PrdZkg_NmrExt =  '" + ordNumber + "' AND ('" + despatchNumber + "' = (case when SklNk_RcdNak <> 0 then (select s3.SklNk_Nmr from SKLNK s3 where s3.SklNk_Rcd = s1.SklNk_RcdNak) else SklNk_Nmr end ) or SklSf_Nmr = '" + despatchNumber + "') \n"
                + " ) q  \n"
                + " group by BarCode_Code, SklN_Rcd, SklN_Cd, SklN_NmAlt, EI_OKEI,Tax, CnWoNds, CnWthNds, SklnCd, EI_ShNm, Nm1, Sklnk_Nmr, Sklnk_Dat, Sklnk_Rcd \n"
                + " having Sum(Qt) > 0 \n"
                + " order by SklN_NmAlt \n";

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
            SqlCommand command = new SqlCommand(sql, conn);
            SqlDataReader dr = command.ExecuteReader();

            int recordCount = 0;
            while (dr.Read()) recordCount++;
            object[,] result = new object[recordCount, dr.FieldCount];

            dr.Close();

            dr = command.ExecuteReader();

            int i = 0;
            while (dr.Read())
            {
                for (int j = 0; j < dr.FieldCount; j++) result[i, j] = dr.GetValue(j);
                i++;
            }
            conn.Close();
            return result;
        }

        /*
        * Удаляет заказ по дате принятия заказа и по номеру (внешний номер заказа), при выполнении условий
        * 1. Заказ существует
        * 2. Заказ единственный
        * 3. Нет накладной
        */
        public static int deleteOrder(String order_number, String dt)
        {

            string connString = Settings.Default.ConnStringISPRO;
            int status = 0;
           // string verification = "DECLARE @status INT; exec dbo.U_MgDeleteOrderByNumberAndDate '" + order_number + "','" + Convert.ToDateTime(dt).ToString("yyyyMMdd") + "',@status OUTPUT";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            try
            {
                SqlCommand command = new SqlCommand("dbo.U_MgDeleteOrderByNumberAndDate", conn);
                command.CommandType = CommandType.StoredProcedure;
                // set up the parameters
                command.Parameters.Add("@MG_OrderNumber", SqlDbType.VarChar, 50);
                command.Parameters.Add("@MG_OrderDate", SqlDbType.DateTime, 10);
                command.Parameters.Add("@DEL_STATUS", SqlDbType.Int).Direction = ParameterDirection.Output;

                // set parameter values
                command.Parameters["@MG_OrderNumber"].Value = order_number;
                command.Parameters["@MG_OrderDate"].Value = Convert.ToDateTime(dt);

                // open connection and execute stored procedure
                command.ExecuteNonQuery();

                // read output value from @NewId
                status = Convert.ToInt32(command.Parameters["@DEL_STATUS"].Value);
            }
            catch
            {
                
            }
            conn.Close();

            return status;
        }


      /*
      * Приведение номера накладной в порядок
      * Если это не номер СФ (первый символ <> букве), добавить 0 слева
      */
        public static string combDespatchNumber(string despatchNumber)
        {
            int count_sym = 8; //сколько символов в номере документа
            char pad = '0';  //какими символами дополнить
            string pattern = @"^[A-Za-zА-Яа-я]\d+$";  //шаблон регулярки
            Regex regex = new Regex(pattern);
            bool match = regex.IsMatch(despatchNumber); 
            if (!match) //не совпали
            {
                despatchNumber = despatchNumber.PadLeft(count_sym, pad);
            }

            //если совпали будем предполагать, что номер не извенялся

            return despatchNumber;
        }


        public static object[] GetNkDataFromZkg(Int64 prdZkgRcd) //Возвращает заголовок накладной по рсд заказа
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT SklNk_Nmr, SklNk_Dat FROM SKLNK WHERE SklNk_CdDoc = 46 AND SklNk_Mov = 0 AND SklNk_RcdZkg = " + prdZkgRcd.ToString();

            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();

            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read()) dr.GetValues(result);
            conn.Close();
            return result;
        }

        public static object[] GetU_CHEDIEXCHDataFromZkg(Int64 prdZkgRcd) //Возвращает номер и дату заказа EDI по рсд заказа
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT Exch_OrdNmrExt, Exch_OrdDat FROM U_CHEDIEXCH WHERE Exch_ZkgRcd = " + prdZkgRcd.ToString();

            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();

            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int n = dr.VisibleFieldCount;
            object[] result = new object[n];
            while (dr.Read()) dr.GetValues(result);
            conn.Close();
            return result;
        }

        public static string GetSklnkNumber(Int32 RcdZkg)
        {
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "select SklNk_Nmr from sklnk where SklNk_RcdZkg = " + RcdZkg + "";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(sql, conn);
            string result = (string)command.ExecuteScalar().ToString();
            conn.Close();
            return result;
        }

        public static string GetMnemoCode(string gtin, string ptn_Rcd) // получение мнемокода
        {
            string result = "";
            string connString = Settings.Default.ConnStringISPRO;
            string GetMnemo = "select top 1 ChEdiibc_Mnem from U_CHEDIIBC where ChEdiibc_RcvrId = " + ptn_Rcd + " and ChEdiibc_gtin = '" + gtin + "'";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(GetMnemo, conn);
            SqlDataReader dr = command.ExecuteReader();
            while (dr.Read())
            {
                if (!dr.IsDBNull(0)) result = dr.GetString(0);
            }
            conn.Close();
            return result;
        }

        public static object[] GetIdProviderFromPtnCD(string Ptn_Cd)
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT Ptn_Cd "
                                   + "     , Ptn_NmSh "
                                   + "     , UFPRV.UF_RkValS "
                                   + "     , Ptn_Inn "
                                   + "     , Ptn_KPP "
                                   + "     , PTNRK.Ptn_Rcd "
                                   + " FROM PTNRK "
                                   + "     JOIN UFPRV ON UFPRV.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRV.UF_TblId = 1126 "
                                   + "     LEFT JOIN ufprv AS FILIALFROM ON FILIALFROM.UF_TblRcd = PTNRK.Ptn_Rcd AND FILIALFROM.UF_TblId = 1126 AND  FILIALFROM.UF_RkRcd = ( SELECT UFR_RkRcd FROM ufrkv WHERE UFR_Id = 'U_FILIAL_FROM' )"
                                   + "     JOIN UFRKV ON UFRKV.UFR_DbRcd = UFPRV.UF_TblId AND UFRKV.UFR_RkRcd = UFPRV.UF_RkRcd  AND UFRKV.UFR_Id = 'U_IdOperEDO'  "
                                   + " WHERE PTNRK.Ptn_Cd = '" + Ptn_Cd + "'";
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

        public static object[] GetDataFromPtn_Rcd(string Ptn_Rcd)//--5 элемент ILN плательщика
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT Ptn_Cd "
                                   + "     , Ptn_NmSh "
                                   + "     , UFPRV.UF_RkValS "
                                   + "     , Ptn_Inn "
                                   + "     , Ptn_KPP "
                                   + "     , UFPRV2.UF_RkValS "
                                   + "     , Filia_Adr "
                                   + "     , LEFT(Filia_Adr,6) "
                                   + "     , PTNRK.Ptn_Rcd "//8
                                   + "     , ISNULL(FILIALFROM.UF_RkValD,0) as FILIALFROM\n"  //9
                                   + "     , EdiNast.NastDoc_Fmt "  //10
                                   + " FROM PTNRK "
                                   + "     JOIN UFPRV ON UFPRV.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRV.UF_TblId = 1126 "
                                   + "     LEFT JOIN ufprv AS FILIALFROM ON FILIALFROM.UF_TblRcd = PTNRK.Ptn_Rcd AND FILIALFROM.UF_TblId = 1126 AND  FILIALFROM.UF_RkRcd = ( SELECT UFR_RkRcd FROM ufrkv WHERE UFR_Id = 'U_FILIAL_FROM' )"
                                   + "     JOIN UFRKV ON UFRKV.UFR_DbRcd = UFPRV.UF_TblId AND UFRKV.UFR_RkRcd = UFPRV.UF_RkRcd  AND UFRKV.UFR_Id = 'U_GLN'  "
                                   + "     LEFT JOIN UFPRV as UFPRV2 WITH (NOLOCK) ON UFPRV2.UF_TblId = 1126 AND UFPRV2.UF_TblRcd = PTNRK.Ptn_Rcd AND UFPRV2.UF_RkRcd = ( SELECT UFR_RkRcd FROM UFRKV WHERE UFR_DbRcd = 1126 AND UFR_Id = 'U_PRODCODE' ) "
                                   + "     LEFT JOIN PTNFILK on PTNFILK .Ptn_Rcd=PTNRK.Ptn_Rcd"
                                   + "     left join U_CHEDINASTDOC edinast on edinast.Gpl_Rcd = PTNRK.Ptn_Rcd"
                                   + " WHERE PTNRK.Ptn_Rcd = '" + Ptn_Rcd + "'";
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

        public static object[] GetPrdZkg(string PrdZkg_Rcd)//возвращает 0-номер заказа; 1-дату принятия; 2-дату отгрузки; 5-номер заказа EDI;
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select PrdZkg_nmr , "
                                   + " convert(date,PrdZkg_Dt) , "
                                   + " convert(date,PrdZkg_DtOtg) , "
                                   + " PrdZkg_NmrExt"
                                   + " from PRDZKG "
                                   + " where PrdZkg_Rcd= '" + PrdZkg_Rcd + "' ";
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

        public static string GetEdIzm(string OKEI)
        {
            string EdIzm;
            string EI_OKEI;
            EI_OKEI = OKEI.Trim();
            int RegCd;
            switch (EI_OKEI)
            {
                case "796": EdIzm = "шт"; break;
                case "0": EdIzm = "шт"; break;
                case "166": EdIzm = "кг"; break;
                case "112": EdIzm = "л"; break;
                case "8751": EdIzm = "кор"; break;
                default: EdIzm = "шт"; break;
            }
            return EdIzm;
        }


        public static DateTime GetEdoSvodDate(string SklSfRcd)
        {
            string getSvodDate = "SELECT tar.PtnTr_Date FROM PTNTARK tar\n";
            getSvodDate += "LEFT JOIN PTNRK ptn ON ptn.Ptn_Rcd = tar.Ptn_Rcd\n";
            getSvodDate += "LEFT JOIN SKLSF sf ON sf.SklSf_KAgID = ptn.Ptn_Rcd\n";
            getSvodDate += $"WHERE tar.PtnTr_Type = 'edo-svod' AND sf.SklSf_Rcd = {SklSfRcd}\n";

            SqlConnection conn = new SqlConnection(Settings.Default.ConnStringISPRO);
            SqlCommand command = new SqlCommand(getSvodDate, conn);

            conn.Open();
            SqlDataReader reader = command.ExecuteReader();
            if (reader.Read())
            {
                object[] results = new object[reader.VisibleFieldCount];
                reader.GetValues(results);
                return Convert.ToDateTime(results[0]);
            }

            return DateTime.MinValue;
        }

    }
}
