using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Data.SqlClient;
using System.Data;
using System.IO;
using System.Xml;
using System.Xml.Schema;

namespace AutoOrdersIntake
{
    class EDIformat
    {
        /*************************************************************************************************************************************/
        /********************************************************** Начало УПД АШАН **********************************************************/
        /*************************************************************************************************************************************/
        public static void CreateEdiAuchanUPD(List<object> CurrDataUPD, string typeFunc) //список УПД, 0 ProviderOpt, 1 ProviderZkg, 2 NastDoc_Fmt, 3 SklSf_Rcd, 4 SklSf_TpOtg, 5 SklSfA_RcdCor, 6 PrdZkg_NmrExt, 7 PrdZkg_Rcd, 8 PrdZkg_Dt ,9 SklNk_TDrvNm
        {
            //получение путей
            string pathArchiveEDI = /*"D:\\Edi\\Archive\\"; //*/ DispOrders.GetValueOption("EDI-СОФТ.АРХИВ");
            string pathUPDEDI;

            //Запрос данных СФ
            object[] infoSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataUPD[3])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO

            //запрос данных спецификации
            object[,] Item = Verifiacation.GetItemsFromSF(Convert.ToString(CurrDataUPD[3]), true); //0 BarCode_Code, 1 SklN_Rcd, 2 SklN_Cd, 3 SklN_NmAlt, 4 Кол-во, 5 Цена без НДC, 6 Цена с НДС, 7 Код ЕИ EDI, 8 ОКЕЙ, 9 Ставка, 10 'S', 11 Сумма НДС, 12 Сумма с НДС, 13 шифр ЕИ, 14 Вес

            //Запрос данных покупателя
            object[] infoKag = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(infoSf[2]), Convert.ToInt64(infoSf[3])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование, 12 Полное наименование
            //Запрос данных покупателя
            object[] infoGpl = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(infoSf[4]), Convert.ToInt64(infoSf[5])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование

            string codeByBuyer = Verifiacation.GetFldFromEdiExch(Convert.ToInt64(CurrDataUPD[7]), "Exch_USelCdByer"); //Код поставщика для Ашана

            //какой gln номер использовать
            bool useMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(infoSf[4]));
            string ilnFirm;

            object[] infoFirm;
            object[] infoFirmAdr;
            object[] infoFirmGrOt;
            object[] infoFirmAdrGrOt;
            if (useMasterGLN == false)//используем данные текущего предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ИЛН");
                pathUPDEDI =  DispOrders.GetValueOption("EDI-СОФТ.УПД");
                infoFirm = Verifiacation.GetFirmInfo(); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO
                infoFirmAdr = Verifiacation.GetFirmAdr(); // 0 CrtAdr_StrNm+','+CrtAdr_House, 1 CrtAdr_TowNm, 2 CrtAdr_RegNm, 3 CrtAdr_Ind, 4 CrtAdr_RegCd, 5 CrtAdr_StrNm, 6 CrtAdr_House 
                infoFirmGrOt = infoFirm;
                infoFirmAdrGrOt = infoFirmAdr;

            }
            else//используем данные головного предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");
                infoFirm = Verifiacation.GetMasterFirmInfo();
                infoFirmAdr = Verifiacation.GetMasterFirmAdr();
                infoFirmGrOt = Verifiacation.GetFirmInfo(); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO
                infoFirmAdrGrOt = Verifiacation.GetFirmAdr(); // 0 CrtAdr_StrNm+','+CrtAdr_House, 1 CrtAdr_TowNm, 2 CrtAdr_RegNm, 3 CrtAdr_Ind, 4 CrtAdr_RegCd, 5 CrtAdr_StrNm, 6 CrtAdr_House 
                try
                {
                    pathUPDEDI =  DispOrders.GetValueOption("EDI-СОФТ.ЭКСПОРТ");
                }
                catch
                {
                    pathUPDEDI =  DispOrders.GetValueOption("EDI-СОФТ.УПД");
                }
            }



            string idEdo = DispOrders.GetValueOption("EDI-СОФТ.ИДЭДО"); //"2IJ"; //ИдЭДО

            string idOtpr = idEdo + ilnFirm; //ИдОтпр
            string idPol = idEdo + infoGpl[2].ToString(); //ИдПол

            string guid = Convert.ToString(Guid.NewGuid());
            string fileName = "ON_NSCHFDOPPR_" + idPol + "_" + idOtpr + "_" + DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid;//ИдФайл


            /************************** 1 уровень. <Файл> ******************************/

            XDocument xdoc = new XDocument(new XDeclaration("1.0", "", ""));

            XElement File = new XElement("Файл");
            XAttribute IdFile = new XAttribute("ИдФайл", "000000000000000000000000000000000000"/*fileName*/);
            XAttribute VersForm = new XAttribute("ВерсФорм", "5.01");
            XAttribute VersProg = new XAttribute("ВерсПрог", "Эдисофт");

            xdoc.Add(File);
            File.Add(IdFile);
            File.Add(VersForm);
            File.Add(VersProg);

            /************************** 2 уровень. <СвУчДокОбор> ************************/

            XElement ID = new XElement("СвУчДокОбор");

            XAttribute IdSender = new XAttribute("ИдОтпр", "0000000000"/*idOtpr*/);
            XAttribute IdReciever = new XAttribute("ИдПол", "0000000000"/*idPol*/);

            File.Add(ID);
            ID.Add(IdSender);
            ID.Add(IdReciever);

            //<СвУчДокОбор><СвОЭДОтпр>
            XElement InfOrg = new XElement("СвОЭДОтпр");
            string providerNm = DispOrders.GetValueOption("EDI-СОФТ.НМ");
            string providerInn = DispOrders.GetValueOption("EDI-СОФТ.ИНН");
            XAttribute NaimOrg = new XAttribute("НаимОрг", providerNm/*"Эдисофт, ООО"*/);
            XAttribute INNUL = new XAttribute("ИННЮЛ", providerInn/*"7801471082"*/);
            XAttribute IDEDO = new XAttribute("ИдЭДО", idEdo);

            ID.Add(InfOrg);
            InfOrg.Add(NaimOrg);
            InfOrg.Add(INNUL);
            InfOrg.Add(IDEDO);

            /************************** 2 уровень. <Документ> ************************/

            XElement DOC = new XElement("Документ");
            XAttribute KND = new XAttribute("КНД", "1115131");
            XAttribute Function = new XAttribute("Функция", typeFunc);
            XAttribute PoFakt = new XAttribute("ПоФактХЖ", "Документ об отгрузке товаров (выполнении работ), передаче имущественных прав (документ об оказании услуг)");
            XAttribute NaimDocOpr = new XAttribute("НаимДокОпр", "Счет-фактура и документ об отгрузке товаров (выполнении работ), передаче имущественных прав (документ об оказании услуг)");
            XAttribute DateF = new XAttribute("ДатаИнфПр", DateTime.Today.ToString(@"dd.MM.yyyy"));
            XAttribute TimeF = new XAttribute("ВремИнфПр", DateTime.Today.ToString(@"hh.mm.ss"));
            XAttribute NameOrg = new XAttribute("НаимЭконСубСост", infoFirm[0].ToString() + ", ИНН-КПП: " + infoFirm[1].ToString() + "-" + infoFirm[2].ToString());

            File.Add(DOC);
            DOC.Add(KND);
            DOC.Add(Function);
            DOC.Add(PoFakt);
            DOC.Add(NaimDocOpr);
            DOC.Add(DateF);
            DOC.Add(TimeF);
            DOC.Add(NameOrg);

            //<Документ><СвСчФакт>
            XElement SVSF = new XElement("СвСчФакт");
            XAttribute NomSF = new XAttribute("НомерСчФ", infoSf[0].ToString());
            XAttribute DateSF = new XAttribute("ДатаСчФ", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));
            XAttribute Kod = new XAttribute("КодОКВ", infoSf[6].ToString());

            DOC.Add(SVSF);
            SVSF.Add(NomSF);
            SVSF.Add(DateSF);
            SVSF.Add(Kod);
            //исправление **********************************
            XElement IsprSF = new XElement("ИспрСчФ");
            SVSF.Add(IsprSF);
            //Проверяем необходимо ли отправить исправление, либо же это обычная отправка
            if (CurrDataUPD[11].ToString() == "0") //Обычная отправка
            {

                //если нет исправления
                XAttribute DfNISF = new XAttribute("ДефНомИспрСчФ", "-");
                XAttribute DfDISF = new XAttribute("ДефДатаИспрСчФ", "-");
                IsprSF.Add(DfNISF);
                IsprSF.Add(DfDISF);
            }
            else // нет исправления
            {
                XAttribute NmIsprSF = new XAttribute("НомИспрСчФ", CurrDataUPD[11].ToString());
                XAttribute DtIsprSF = new XAttribute("ДатаИспрСчФ", DateTime.Today.ToString(@"dd.MM.yyyy"));
                IsprSF.Add(NmIsprSF);
                IsprSF.Add(DtIsprSF);
            }
            //конец исправления **********************************
            //<Документ><СвСчФакт><СвПрод>
            XElement SvProd = new XElement("СвПрод");
            //XAttribute SvProdOKPO = new XAttribute("ОКПО", infoFirm[3].ToString());
            SVSF.Add(SvProd);
            //SvProd.Add(SvProdOKPO);

            //<Документ><СвСчФакт><СвПрод><ИдСв>
            XElement SvProdIdSv = new XElement("ИдСв");
            SvProd.Add(SvProdIdSv);

            //<Документ><СвСчФакт><СвПрод><ИдСв><СвЮЛУч>
            XElement SvProdSvUluchh = new XElement("СвЮЛУч");
            XAttribute SvProdIdSvName = new XAttribute("НаимОрг", infoFirm[0].ToString());
            XAttribute SvProdIdSvINN = new XAttribute("ИННЮЛ", infoFirm[1].ToString());
            XAttribute SvProdIdSvKPP = new XAttribute("КПП", infoFirm[2].ToString());
            if (useMasterGLN)  //используем данные головного предприятия
            {
                SvProdIdSvKPP = new XAttribute("КПП", infoFirmGrOt[2].ToString());
            }


            SvProdIdSv.Add(SvProdSvUluchh);
            SvProdSvUluchh.Add(SvProdIdSvName);
            SvProdSvUluchh.Add(SvProdIdSvINN);
            SvProdSvUluchh.Add(SvProdIdSvKPP);

            //<Документ><СвСчФакт><СвПрод><Адрес>
            XElement SvProdAdres = new XElement("Адрес");
            SvProd.Add(SvProdAdres);

            //<Документ><СвСчФакт><СвПрод><Адрес><АдресРФ>
            //Адрес
            XElement SvProdAdrRF = new XElement("АдрРФ");
            SvProdAdres.Add(SvProdAdrRF);
            if (infoFirmAdr[3].ToString() != "")
            {
                XAttribute SvProdIndex = new XAttribute("Индекс", infoFirmAdr[3].ToString());
                SvProdAdrRF.Add(SvProdIndex);
            }
         //   if (infoFirmAdr[4].ToString() != "")
        //    {
                XAttribute SvProdKodReg = new XAttribute("КодРегион", infoFirmAdr[4].ToString());
                SvProdAdrRF.Add(SvProdKodReg);
//            }

            //<Документ><СвСчФакт><ГрузОт>
            XElement GruzOt = new XElement("ГрузОт");
            if (useMasterGLN)
            {
                XElement GruzOtOther = new XElement("ГрузОтпр");
                SVSF.Add(GruzOt);
                GruzOt.Add(GruzOtOther);

                XElement SvGrOtpIdSv = new XElement("ИдСв");
                GruzOtOther.Add(SvGrOtpIdSv);

                //<Документ><СвСчФакт><СвПрод><ИдСв><СвЮЛУч>
                XElement SvGrOtpSvUluchh = new XElement("СвЮЛУч");
                XAttribute SvGrOtpIdSvName = new XAttribute("НаимОрг", infoFirmGrOt[0].ToString());
                XAttribute SvGrOtpIdSvINN = new XAttribute("ИННЮЛ", infoFirmGrOt[1].ToString());
                XAttribute SvGrOtpIdSvKPP = new XAttribute("КПП", infoFirmGrOt[2].ToString());

                SvGrOtpIdSv.Add(SvGrOtpSvUluchh);
                SvGrOtpSvUluchh.Add(SvGrOtpIdSvName);
                SvGrOtpSvUluchh.Add(SvGrOtpIdSvINN);
                SvGrOtpSvUluchh.Add(SvGrOtpIdSvKPP);

                //<Документ><СвСчФакт><СвПрод><Адрес>
                XElement SvGrOtpAdres = new XElement("Адрес");
                GruzOtOther.Add(SvGrOtpAdres);

                XElement SvGrOtpAdrRF = new XElement("АдрРФ");
                SvGrOtpAdres.Add(SvGrOtpAdrRF);
                if (infoFirmAdrGrOt[3].ToString() != "")
                {
                    XAttribute SvGrOtpIndex = new XAttribute("Индекс", infoFirmAdrGrOt[3].ToString());
                    SvGrOtpAdrRF.Add(SvGrOtpIndex);
                }
                XAttribute SvGrOtpKodReg = new XAttribute("КодРегион", infoFirmAdrGrOt[4].ToString());
                SvGrOtpAdrRF.Add(SvGrOtpKodReg);
                if (infoFirmAdrGrOt[1].ToString() != "")
                {
                    XAttribute SvGrOtpGorod = new XAttribute("Город", infoFirmAdrGrOt[1].ToString());
                    SvGrOtpAdrRF.Add(SvGrOtpGorod);
                }
                if (infoFirmAdrGrOt[5].ToString() != "")
                {
                    XAttribute SvGrOtpStreet = new XAttribute("Улица", infoFirmAdrGrOt[5].ToString());
                    SvGrOtpAdrRF.Add(SvGrOtpStreet);
                }
                if (infoFirmAdrGrOt[6].ToString() != "")
                {
                    XAttribute SvGrOtpHouse = new XAttribute("Дом", infoFirmAdrGrOt[6].ToString());
                    SvGrOtpAdrRF.Add(SvGrOtpHouse);
                }

            }
            else
            {
                XElement GruzOtOnJe = new XElement("ОнЖе", "он же");
                SVSF.Add(GruzOt);
                GruzOt.Add(GruzOtOnJe);
            }

            //<Документ><СвСчФакт><ГрузПолуч>
            XElement GruzPoluch = new XElement("ГрузПолуч");
            SVSF.Add(GruzPoluch);

            //<Документ><СвСчФакт><ГрузПолуч><ИдСв>
            XElement GruzPoluchIdSv = new XElement("ИдСв");
            GruzPoluch.Add(GruzPoluchIdSv);

            //<Документ><СвСчФакт><ГрузПолуч><ИдСв><СвЮЛУч>
            XElement GruzPoluchSvUluch = new XElement("СвЮЛУч");
            XAttribute GruzPoluchName = new XAttribute("НаимОрг", infoGpl[12]);
            XAttribute GruzPoluchINN = new XAttribute("ИННЮЛ", infoGpl[3]);
            GruzPoluchIdSv.Add(GruzPoluchSvUluch);
            GruzPoluchSvUluch.Add(GruzPoluchName);
            GruzPoluchSvUluch.Add(GruzPoluchINN);
            if (infoGpl[4].ToString().Trim().Length > 0)
            {
                XAttribute GruzPoluchKPP = new XAttribute("КПП", infoGpl[4]);
                GruzPoluchSvUluch.Add(GruzPoluchKPP);
            }

            //<Документ><СвСчФакт><ГрузПолуч><Адрес>
            XElement GruzPoluchAdres = new XElement("Адрес");
            GruzPoluch.Add(GruzPoluchAdres);

            //<Документ><СвСчФакт><ГрузПолуч><Адрес><АдресРФ>
            XElement GruzPoluchAdrRF = new XElement("АдрРФ");
            GruzPoluchAdres.Add(GruzPoluchAdrRF);
            if (infoGpl[7].ToString().Trim().Length > 0)
            {
                XAttribute GruzPoluchIndex = new XAttribute("Индекс", infoGpl[7]);
                GruzPoluchAdrRF.Add(GruzPoluchIndex);
            }
         //   if (infoGpl[8].ToString().Length > 0) Атрибут обязательный
         //   {
                XAttribute GruzPoluchKodReg = new XAttribute("КодРегион", infoGpl[8]);
                GruzPoluchAdrRF.Add(GruzPoluchKodReg);
         //   }
            if (infoGpl[9].ToString().Length > 0)
            {
                XAttribute GruzPoluchCity = new XAttribute("Город", infoGpl[9]);
                GruzPoluchAdrRF.Add(GruzPoluchCity);
            }
            if (infoGpl[10].ToString().Length > 0)
            {
                XAttribute GruzPoluchStreet = new XAttribute("Улица", infoGpl[10]);
                GruzPoluchAdrRF.Add(GruzPoluchStreet);
            }
            if (infoGpl[11].ToString().Length > 0)
            {
                XAttribute GruzPoluchHouse = new XAttribute("Дом", infoGpl[11]);
                GruzPoluchAdrRF.Add(GruzPoluchHouse);
            }


            //<Документ><СвСчФакт><СвПокуп>
            XElement SvPokup = new XElement("СвПокуп");
            SVSF.Add(SvPokup);

            //<Документ><СвСчФакт><СвПокуп><ИдСв>
            XElement SvPokupIdSv = new XElement("ИдСв");
            SvPokup.Add(SvPokupIdSv);

            //<Документ><СвСчФакт><СвПокуп><ИдСв><СвЮЛУч>             
            XElement SvPokupSvUluch = new XElement("СвЮЛУч");
            XAttribute SvPokupName = new XAttribute("НаимОрг", infoKag[12]);
            XAttribute SvPokupINN = new XAttribute("ИННЮЛ", infoKag[3]);
            XAttribute SvPokupKPP = new XAttribute("КПП", infoKag[4]);
            SvPokupIdSv.Add(SvPokupSvUluch);
            SvPokupSvUluch.Add(SvPokupName);
            SvPokupSvUluch.Add(SvPokupINN);
            SvPokupSvUluch.Add(SvPokupKPP);

            //<Документ><СвСчФакт><СвПокуп><Адрес>
            XElement SvPokupAdres = new XElement("Адрес");
            SvPokup.Add(SvPokupAdres);

            //<Документ><СвСчФакт><СвПокуп><Адрес><АдресРФ>
            XElement SvPokupAdrRF = new XElement("АдрРФ");
            SvPokupAdres.Add(SvPokupAdrRF);
            if (infoGpl[7].ToString().Trim().Length > 0)
            {
                XAttribute SvPokupIndex = new XAttribute("Индекс", infoKag[7]);
                SvPokupAdrRF.Add(SvPokupIndex);
            }
        //    if (infoGpl[8].ToString().Length > 0) Атрибут обязательный
        //    {
                XAttribute SvPokupKodReg = new XAttribute("КодРегион", infoKag[8]);
                SvPokupAdrRF.Add(SvPokupKodReg);
        //    }
            if (infoGpl[9].ToString().Length > 0)
            {
                XAttribute SvPokupCity = new XAttribute("Город", infoKag[9]);
                SvPokupAdrRF.Add(SvPokupCity);
            }
            if (infoGpl[10].ToString().Length > 0)
            {
                XAttribute SvPokupStreet = new XAttribute("Улица", infoKag[10]);
                SvPokupAdrRF.Add(SvPokupStreet);
            }
            if (infoGpl[11].ToString().Length > 0)
            {
                XAttribute SvPokupHouse = new XAttribute("Дом", infoKag[11]);
                SvPokupAdrRF.Add(SvPokupHouse);
            }

            //<Документ><СвСчФакт><ИнфПолФХЖ1>
            XElement DopSvFHJ1 = new XElement("ДопСвФХЖ1");
            XAttribute NaimOKV = new XAttribute("НаимОКВ", "Российский рубль");
            SVSF.Add(DopSvFHJ1);
            DopSvFHJ1.Add(NaimOKV);

            //<Документ><СвСчФакт><ИнфПолФХЖ1>
            XElement InfPolFHJ1 = new XElement("ИнфПолФХЖ1");
            SVSF.Add(InfPolFHJ1);

            //<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf1 = new XElement("ТекстИнф");
            XAttribute TxtInf1Identif = new XAttribute("Идентиф", "номер_заказа");
            XAttribute TxtInf1Znachen = new XAttribute("Значен", CurrDataUPD[6]);
            InfPolFHJ1.Add(TxtInf1);
            TxtInf1.Add(TxtInf1Identif);
            TxtInf1.Add(TxtInf1Znachen);

            //<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf2 = new XElement("ТекстИнф");
            XAttribute TxtInf2Identif = new XAttribute("Идентиф", "отправитель");
            XAttribute TxtInf2Znachen = new XAttribute("Значен", ilnFirm);
            InfPolFHJ1.Add(TxtInf2);
            TxtInf2.Add(TxtInf2Identif);
            TxtInf2.Add(TxtInf2Znachen);

            //<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf3 = new XElement("ТекстИнф");
            XAttribute TxtInf3Identif = new XAttribute("Идентиф", "получатель");
            XAttribute TxtInf3Znachen = new XAttribute("Значен", infoKag[2]);
            InfPolFHJ1.Add(TxtInf3);
            TxtInf3.Add(TxtInf3Identif);
            TxtInf3.Add(TxtInf3Znachen);

            //<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf4 = new XElement("ТекстИнф");
            XAttribute TxtInf4Identif = new XAttribute("Идентиф", "код_поставщика");
            XAttribute TxtInf4Znachen = new XAttribute("Значен", codeByBuyer);
            InfPolFHJ1.Add(TxtInf4);
            TxtInf4.Add(TxtInf4Identif);
            TxtInf4.Add(TxtInf4Znachen);

            //<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf5 = new XElement("ТекстИнф");
            XAttribute TxtInf5Identif = new XAttribute("Идентиф", "GLN_грузополучателя");
            XAttribute TxtInf5Znachen = new XAttribute("Значен", infoGpl[2]);
            InfPolFHJ1.Add(TxtInf5);
            TxtInf5.Add(TxtInf5Identif);
            TxtInf5.Add(TxtInf5Znachen);

            /************************** 3 уровень. <ТаблСчФакт> ************************/
            XElement TabSF = new XElement("ТаблСчФакт");
            DOC.Add(TabSF);

            decimal sumWthNds = 0;
            decimal sumNds = 0;
            decimal sumWeight = 0;

            for (int i = 0; i < Item.GetLongLength(0); i++) //Item[] //0 BarCode_Code, 1 SklN_Rcd, 2 SklN_Cd, 3 SklN_NmAlt, 4 Кол-во, 5 Цена без НДC, 6 Цена с НДС, 7 Код ЕИ EDI, 8 ОКЕЙ, 9 Ставка, 10 'S', 11 Сумма НДС, 12 Сумма с НДС, 13 шифр ЕИ, 14 Вес
            {
                sumWthNds = sumWthNds + Convert.ToDecimal(Item[i, 12]);
                sumNds = sumNds + Convert.ToDecimal(Item[i, 11]);
                sumWeight = sumWeight + Convert.ToDecimal(Item[i, 14]);
                //<Документ><ТаблСчФакт><СведТов>
                XElement SvedTov = new XElement("СведТов");
                XAttribute NomStr = new XAttribute("НомСтр", Convert.ToString(i + 1));
                XAttribute NaimTov = new XAttribute("НаимТов", Item[i, 3]);
                XAttribute OKEI = new XAttribute("ОКЕИ_Тов", Item[i, 8]);
                XAttribute KolTov = new XAttribute("КолТов", Math.Round(Convert.ToDecimal(Item[i, 4]), 2));
                XAttribute CenaTov = new XAttribute("ЦенаТов", Item[i, 5]);
                XAttribute BezNDS = new XAttribute("СтТовБезНДС", Math.Round(Convert.ToDecimal(Item[i, 12]) - Convert.ToDecimal(Item[i, 11]), 2));
                XAttribute NalSt = new XAttribute("НалСт", Convert.ToString(Item[i, 9]));
                XAttribute sNDS = new XAttribute("СтТовУчНал", Math.Round(Convert.ToDecimal(Item[i, 12]), 2));
                TabSF.Add(SvedTov);
                SvedTov.Add(NomStr);
                SvedTov.Add(NaimTov);
                SvedTov.Add(OKEI);
                SvedTov.Add(KolTov);
                SvedTov.Add(CenaTov);
                SvedTov.Add(BezNDS);
                SvedTov.Add(NalSt);
                SvedTov.Add(sNDS);

                //<Документ><ТаблСчФакт><СведТов><Акциз>
                XElement Akciz = new XElement("Акциз");
                XElement bezAkciz = new XElement("БезАкциз", "без акциза");
                SvedTov.Add(Akciz);
                Akciz.Add(bezAkciz);

                //<Документ><ТаблСчФакт><СведТов><СумНал>
                XElement SumNal = new XElement("СумНал");
                SvedTov.Add(SumNal);
                if (Convert.ToString(Item[i, 9]) == "без НДС")
                {
                    //<Документ><ТаблСчФакт><СведТов><СумНал><БезНДС>
                    XElement BezNds = new XElement("БезНДС", "без НДС");
                    SumNal.Add(BezNds);
                }
                else
                {
                    //<Документ><ТаблСчФакт><СведТов><СумНал><СумНал>
                    XElement SumNalSum = new XElement("СумНал", Math.Round(Convert.ToDecimal(Item[i, 11]), 2));
                    SumNal.Add(SumNalSum);
                }

                //<Документ><ТаблСчФакт><СведТов><ИнфПолФХЖ2>
                string nomBuyerCd = Verifiacation.GetBuyerItemCodeRcd(Convert.ToString(infoKag[5]), Convert.ToInt64(Item[i, 1]));

                //<Документ><ТаблСчФакт><СведТов><ДопСведТов>
                XElement DopSvedTov = new XElement("ДопСведТов");
                XAttribute PrTovRav = new XAttribute("ПрТовРаб", "1");
                XAttribute NaimEdIzm = new XAttribute("НаимЕдИзм", Item[i, 13]);
                SvedTov.Add(DopSvedTov);
                DopSvedTov.Add(PrTovRav);
                DopSvedTov.Add(NaimEdIzm);

                XElement InfPolFHJ21 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf1Identif = new XAttribute("Идентиф", "код_материала");
                XAttribute ItmTxtInf1Znachen = new XAttribute("Значен", nomBuyerCd);
                SvedTov.Add(InfPolFHJ21);
                InfPolFHJ21.Add(ItmTxtInf1Identif);
                InfPolFHJ21.Add(ItmTxtInf1Znachen);

                //<Документ><ТаблСчФакт><СведТов><ИнфПолФХЖ2>
                XElement InfPolFHJ22 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf2Identif = new XAttribute("Идентиф", "штрихкод");
                XAttribute ItmTxtInf2Znachen = new XAttribute("Значен", Item[i, 0]);
                SvedTov.Add(InfPolFHJ22);
                InfPolFHJ22.Add(ItmTxtInf2Identif);
                InfPolFHJ22.Add(ItmTxtInf2Znachen);

            }

            //<Документ><ТаблСчФакт><ВсегоОпл>
            XElement VsegoOpl = new XElement("ВсегоОпл");
            XAttribute VsegoBezNds = new XAttribute("СтТовБезНДСВсего", Math.Round(sumWthNds - sumNds, 2));
            XAttribute VsegoSNDS = new XAttribute("СтТовУчНалВсего", Math.Round(sumWthNds, 2));
            TabSF.Add(VsegoOpl);
            VsegoOpl.Add(VsegoBezNds);
            VsegoOpl.Add(VsegoSNDS);

            //<Документ><ТаблСчФакт><ВсегоОпл><СумНалВсего>
            XElement SumNalVsego = new XElement("СумНалВсего");
            VsegoOpl.Add(SumNalVsego);

            if (Convert.ToString(Item[0, 9]) == "без НДС")
            {
                //<Документ><ТаблСчФакт><ВсегоОпл><СумНалВсего><БезНДС>
                XElement SumNalVsegoSumNal = new XElement("БезНДС", "без НДС");
                SumNalVsego.Add(SumNalVsegoSumNal);
            }
            else
            {
                //<Документ><ТаблСчФакт><ВсегоОпл><СумНалВсего><СумНал>
                XElement SumNalVsegoSumNal = new XElement("СумНал", Math.Round(sumNds, 2));
                SumNalVsego.Add(SumNalVsegoSumNal);
            }

            //<Документ><ТаблСчФакт><ВсегоОпл><НеттоВс>
            XElement NettoVes = new XElement("КолНеттоВс", Math.Round(sumWeight, 3));
            VsegoOpl.Add(NettoVes);

            /************************** 3 уровень. <СвПродПер> ************************/
            XElement ProdPer = new XElement("СвПродПер");
            DOC.Add(ProdPer);

            //<Документ><СвПродПер><СвПер>
            XElement SvPer = new XElement("СвПер");
            XAttribute SodOper = new XAttribute("СодОпер", "Товары переданы");
            ProdPer.Add(SvPer);
            SvPer.Add(SodOper);

            //<Документ><СвПродПер><СвПер><ОснПер>
            XElement OsnPer = new XElement("ОснПер");
            XAttribute NaimOsn = new XAttribute("НаимОсн", "Заказ");
            XAttribute NomOsn = new XAttribute("НомОсн", CurrDataUPD[6]);
            XAttribute DataOsn = new XAttribute("ДатаОсн", Convert.ToDateTime(CurrDataUPD[8]).ToString(@"dd.MM.yyyy"));
            SvPer.Add(OsnPer);
            OsnPer.Add(NaimOsn);
            OsnPer.Add(NomOsn);
            OsnPer.Add(DataOsn);

            //<Документ><СвПродПер><СвПер><СвЛицПер>
            XElement SvLicPer = new XElement("СвЛицПер");
            SvPer.Add(SvLicPer);

            //<Документ><СвПродПер><СвПер><СвЛицПер><РабОргПрод>
            XElement RabOrgProd = new XElement("РабОргПрод");
            XAttribute Doljnost = new XAttribute("Должность", "Водитель");
            SvLicPer.Add(RabOrgProd);
            RabOrgProd.Add(Doljnost);

            //<Документ><СвПродПер><СвПер><СвЛицПер><РабОргПрод><ФИО>
            XElement pFIO = new XElement("ФИО");

            string sFIO = Convert.ToString(CurrDataUPD[9]);
            int len = sFIO.Length;
            string sF;
            string sI;
            string sO;
            int p1 = sFIO.IndexOf(" ");
            int p2 = sFIO.LastIndexOf(" ");
            if (len > 0)
            {
                sF = sFIO.Remove(p1);
                sI = sFIO.Substring(p1, (p2 - p1));
                sO = sFIO.Substring(p2, len - p2);

                if (sF.Length <= 0) sF = "НеУказано";
                if (sI.Length <= 0) sF = "НеУказано";
                if (sO.Length <= 0) sF = "НеУказано";
            }
            else
            {
                sF = "НеУказано";
                sI = "НеУказано";
                sO = "НеУказано";
            }

            XAttribute pFIOF = new XAttribute("Фамилия", sF.Trim());
            XAttribute pFIOI = new XAttribute("Имя", sI.Trim());
            XAttribute pFIOO = new XAttribute("Отчество", sO.Trim());

            RabOrgProd.Add(pFIO);
            pFIO.Add(pFIOF);
            pFIO.Add(pFIOI);
            pFIO.Add(pFIOO);

            /************************** 3 уровень. <Подписант> ************************/
            string[] infoSigner = Verifiacation.GetSignerOpt(); //0 ОБЩИЕ.ЭЦПДОЛЖНОСТЬ, 1 ОБЩИЕ.ЭЦПФАМИЛИЯ, 2 ОБЩИЕ.ЭЦПИМЯ, 3 ОБЩИЕ.ЭЦПОТЧЕСТВО, 4 ОБЩИЕ.ЭЦПДОЛЖНОБЯЗ

            XElement Podp = new XElement("Подписант");
            XAttribute obl = new XAttribute("ОблПолн", "6");
            XAttribute status = new XAttribute("Статус", "1");
            XAttribute osn = new XAttribute("ОснПолн", infoSigner[4]);
            DOC.Add(Podp);
            Podp.Add(obl);
            Podp.Add(status);
            Podp.Add(osn);

            //<Документ><Подписант><ЮЛ>
            XElement UL = new XElement("ЮЛ");
            XAttribute innUl = new XAttribute("ИННЮЛ", infoFirm[1]);
            XAttribute naimOrg = new XAttribute("НаимОрг", infoFirm[0]);
            XAttribute dolj = new XAttribute("Должн", infoSigner[0]);
            Podp.Add(UL);
            UL.Add(innUl);
            UL.Add(naimOrg);
            UL.Add(dolj);

            //<Документ><Подписант><ЮЛ><ФИО>
            XElement FIO = new XElement("ФИО");
            XAttribute famdir = new XAttribute("Фамилия", infoSigner[1]);
            XAttribute namedir = new XAttribute("Имя", infoSigner[2]);
            XAttribute otchesdir = new XAttribute("Отчество", infoSigner[3]);
            UL.Add(FIO);
            FIO.Add(famdir);
            FIO.Add(namedir);
            FIO.Add(otchesdir);


            //------сохранение документа-----------
            fileName = fileName + ".xml";
            try
            {
                xdoc.Save(pathArchiveEDI + fileName);
                try
                {
                    xdoc.Save(pathUPDEDI + fileName);
                    string message = "EDISOFT. УПД " + typeFunc + " " + fileName + " создан в " + pathUPDEDI;
                    Program.WriteLine(message);
                    DispOrders.WriteProtocolEDI("УПД " + typeFunc, fileName, infoKag[0] + " - " + infoKag[1], 0, infoGpl[0] + " - " + infoGpl[1], "УПД  " + typeFunc + " сформирован", DateTime.Now, Convert.ToString(CurrDataUPD[6]), "EDISOFT");
                    DispOrders.WriteEDiSentDoc("8", fileName, Convert.ToString(CurrDataUPD[3]), Convert.ToString(infoSf[0]), "123", Convert.ToString(sumWthNds), Convert.ToString(CurrDataUPD[7]),1, CurrDataUPD[11].ToString());
                }
                catch (Exception e)
                {
                    string message_error = "EDISOFT. Не могу создать xml файл УПД  " + typeFunc + " в " + pathUPDEDI + ". Нет доступа или диск переполнен.";
                    DispOrders.WriteProtocolEDI("УПД  " + typeFunc, fileName, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УПД  " + typeFunc  + " не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUPD[6]), "EDISOFT");
                    Program.WriteLine(message_error);
                    DispOrders.WriteErrorLog(e.Message);
                }
            }
            catch (Exception e)
            {
                string message_error = "EDISOFT. Не могу создать xml файл УПД " + typeFunc  + " в " + pathArchiveEDI + ". Нет доступа или диск переполнен.";
                DispOrders.WriteProtocolEDI("УПД " + typeFunc, fileName, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УПД " + typeFunc  + " не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUPD[6]), "EDISOFT");
                Program.WriteLine(message_error);
                DispOrders.WriteErrorLog(e.Message);
                //запись в лог о неудаче
            }
        }

        /*************************************************************************************************************************************/
        /********************************************************** Начало УКД АШАН **********************************************************/
        /*************************************************************************************************************************************/
        public static void CreateEdiAuchanUKD(List<object> CurrDataUKD) //список УКД, 0 ProviderOpt, 1 ProviderZkg, 2 NastDoc_Fmt, 3 SklSf_Rcd, 4 SklSf_TpOtg, 5 SklSfA_RcdCor, 6 PrdZkg_NmrExt, 7 PrdZkg_Rcd, 8 PrdZkg_Dt ,9 SklNk_TDrvNm
        {
            //получение путей
            string pathArchiveEDI = /*"D:\\Edi\\Archive\\"; //*/DispOrders.GetValueOption("EDI-СОФТ.АРХИВ");
            string pathUKDEDI;

            //Запрос данных КорректировочнойСФ
            object[] infoSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataUKD[3])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO
            //Запрос данных Корректируемой (отгрузочной) СФ
            object[] infoCorSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataUKD[5])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO
            //Запрос предыдущих Корректировочных СФ корректируемые текущей корректировочной СФ
            string InfoPrevSf = Verifiacation.GetPrevSfToKSF(Convert.ToString(CurrDataUKD[3]), Convert.ToString(CurrDataUKD[5]));

            //запрос данных спецификации
            object[,] Item = Verifiacation.GetItemsFromKSF(Convert.ToString(CurrDataUKD[3]), Convert.ToString(CurrDataUKD[5]), true); //0 BarCode_Code, 1 SklN_Rcd, 2 SklN_Cd, 3 SklN_NmAlt, 4 Кол-во, 5 Цена без НДC, 6 Цена с НДС, 7 Код ЕИ EDI, 8 ОКЕЙ, 9 Ставка, 10 'S', 11 Сумма НДС, 12 Сумма с НДС, 13 шифр ЕИ, 14 Вес

            //Запрос данных покупателя
            object[] infoKag = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(infoSf[2]), Convert.ToInt64(infoSf[3])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование
            //Запрос данных покупателя
            object[] infoGpl = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(infoSf[4]), Convert.ToInt64(infoSf[5])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование

            string codeByBuyer = Verifiacation.GetFldFromEdiExch(Convert.ToInt64(CurrDataUKD[7]), "Exch_USelCdByer"); //Код поставщика для Ашана

            //какой gln номер использовать
            bool useMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(infoSf[4]));
            string ilnFirm;

            object[] infoFirm;
            object[] infoFirmAdr;
            object[] infoFirmGrOt;
            object[] infoFirmAdrGrOt;
            if (useMasterGLN == false)//используем данные текущего предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ИЛН");
                pathUKDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/DispOrders.GetValueOption("EDI-СОФТ.УКД");
                infoFirm = Verifiacation.GetFirmInfo(); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO
                infoFirmAdr = Verifiacation.GetFirmAdr(); // 0 CrtAdr_StrNm+','+CrtAdr_House, 1 CrtAdr_TowNm, 2 CrtAdr_RegNm, 3 CrtAdr_Ind, 4 CrtAdr_RegCd
                infoFirmGrOt = infoFirm;
                infoFirmAdrGrOt = infoFirmAdr;

            }
            else//используем данные головного предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");
                infoFirm = Verifiacation.GetMasterFirmInfo();
                infoFirmAdr = Verifiacation.GetMasterFirmAdr();
                infoFirmGrOt = Verifiacation.GetFirmInfo();
                infoFirmAdrGrOt = Verifiacation.GetFirmAdr();
                try
                {
                    pathUKDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/DispOrders.GetValueOption("EDI-СОФТ.ЭКСПОРТ");
                }
                catch
                {
                    pathUKDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/DispOrders.GetValueOption("EDI-СОФТ.УКД");
                }
            }



            string idEdo = DispOrders.GetValueOption("EDI-СОФТ.ИДЭДО");  //"2IJ"; //ИдЭДО

            string idOtpr = idEdo + ilnFirm; //ИдОтпр
            string idPol = idEdo + infoGpl[2].ToString(); //ИдПол

            string guid = Convert.ToString(Guid.NewGuid());
            string fileName = "ON_NKORSCHFDOPPR_" + idPol + "_" + idOtpr + "_" + DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid;//ИдФайл


            /************************** 1 уровень. <Файл> ******************************/

            XDocument xdoc = new XDocument(new XDeclaration("1.0", "", ""));

            XElement File = new XElement("Файл");
            XAttribute IdFile = new XAttribute("ИдФайл", "000000000000000000000000000000000000"/*fileName*/);
            XAttribute VersForm = new XAttribute("ВерсФорм", "5.01");
            XAttribute VersProg = new XAttribute("ВерсПрог", "Edisoft");

            xdoc.Add(File);
            File.Add(IdFile);
            File.Add(VersForm);
            File.Add(VersProg);

            /************************** 2 уровень. <СвУчДокОбор> ************************/

            XElement ID = new XElement("СвУчДокОбор");

            XAttribute IdSender = new XAttribute("ИдОтпр", "0000000000"/*idOtpr*/);
            XAttribute IdReciever = new XAttribute("ИдПол", "0000000000"/*idPol*/);

            File.Add(ID);
            ID.Add(IdSender);
            ID.Add(IdReciever);

            //<СвУчДокОбор><СвОЭДОтпр>
            XElement InfOrg = new XElement("СвОЭДОтпр");
            string providerNm = DispOrders.GetValueOption("EDI-СОФТ.НМ");
            string providerInn = DispOrders.GetValueOption("EDI-СОФТ.ИНН");
            XAttribute NaimOrg = new XAttribute("НаимОрг", providerNm/*"Эдисофт, ООО"*/);
            XAttribute INNUL = new XAttribute("ИННЮЛ", providerInn/*"7801471082"*/);
            XAttribute IDEDO = new XAttribute("ИдЭДО", idEdo);

            ID.Add(InfOrg);
            InfOrg.Add(NaimOrg);
            InfOrg.Add(INNUL);
            InfOrg.Add(IDEDO);

            /************************** 2 уровень. <Документ> ************************/

            XElement DOC = new XElement("Документ");
            XAttribute KND = new XAttribute("КНД", "1115133");
            XAttribute Function = new XAttribute("Функция", "КСЧФДИС");
            XAttribute PoFakt = new XAttribute("ПоФактХЖ", "Документ, подтверждающий согласие (факт уведомления) покупателя на изменение стоимости отгруженных товаров (выполненных работ, оказанных услуг), переданных имущественных прав");
            XAttribute NaimDocOpr = new XAttribute("НаимДокОпр", "Документ, подтверждающий согласие (факт уведомления) покупателя на изменение стоимости отгруженных товаров (выполненных работ, оказанных услуг), переданных имущественных прав");
            XAttribute DateF = new XAttribute("ДатаИнфПр", DateTime.Today.ToString(@"dd.MM.yyyy"));
            XAttribute TimeF = new XAttribute("ВремИнфПр", DateTime.Today.ToString(@"hh.mm.ss"));
            XAttribute NameOrg = new XAttribute("НаимЭконСубСост", infoFirm[0].ToString() + ", ИНН-КПП: " + infoFirm[1].ToString() + "-" + infoFirm[2].ToString());

            File.Add(DOC);
            DOC.Add(KND);
            DOC.Add(Function);
            DOC.Add(PoFakt);
            DOC.Add(NaimDocOpr);
            DOC.Add(DateF);
            DOC.Add(TimeF);
            DOC.Add(NameOrg);

            //<Документ><СвКСчФ>
            XElement SVSF = new XElement("СвКСчФ");
            XAttribute NomSF = new XAttribute("НомерКСчФ", infoSf[0].ToString());
            XAttribute DateSF = new XAttribute("ДатаКСчФ", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));
            XAttribute Kod = new XAttribute("КодОКВ", infoSf[6].ToString());

            DOC.Add(SVSF);
            SVSF.Add(NomSF);
            SVSF.Add(DateSF);
            SVSF.Add(Kod);

            //<Документ><СвКСчФ><СчФ>
            XElement SchF = new XElement("СчФ");
            XAttribute NomSchF = new XAttribute("НомерСчФ", infoCorSf[0].ToString());
            XAttribute DateSchF = new XAttribute("ДатаСчФ", Convert.ToDateTime(infoCorSf[1]).ToString(@"dd.MM.yyyy"));
            SVSF.Add(SchF);
            SchF.Add(NomSchF);
            SchF.Add(DateSchF);


            //<Документ><<СвКСчФ>><СвПрод>
            XElement SvProd = new XElement("СвПрод");
            //XAttribute SvProdOKPO = new XAttribute("ОКПО", infoFirm[3].ToString());
            SVSF.Add(SvProd);
            //SvProd.Add(SvProdOKPO);

            //<Документ><<СвКСчФ>><СвПрод><ИдСв>
            XElement SvProdIdSv = new XElement("ИдСв");
            SvProd.Add(SvProdIdSv);

            //<Документ><<СвКСчФ>><СвПрод><ИдСв><СвЮЛУч>
            XElement SvProdSvUluchh = new XElement("СвЮЛУч");
            XAttribute SvProdIdSvName = new XAttribute("НаимОрг", infoFirm[0].ToString());
            XAttribute SvProdIdSvINN = new XAttribute("ИННЮЛ", infoFirm[1].ToString());
            XAttribute SvProdIdSvKPP = new XAttribute("КПП", infoFirm[2].ToString());
            if (useMasterGLN)
               SvProdIdSvKPP = new XAttribute("КПП", infoFirmGrOt[2].ToString());

            SvProdIdSv.Add(SvProdSvUluchh);
            SvProdSvUluchh.Add(SvProdIdSvName);
            SvProdSvUluchh.Add(SvProdIdSvINN);
            SvProdSvUluchh.Add(SvProdIdSvKPP);

            //<Документ><<СвКСчФ>><СвПрод><Адрес>
            XElement SvProdAdres = new XElement("Адрес");
            SvProd.Add(SvProdAdres);

            //<Документ><<СвКСчФ>><СвПрод><Адрес><АдресРФ>
            //Адрес
            XElement SvProdAdrRF = new XElement("АдрРФ");
            SvProdAdres.Add(SvProdAdrRF);
            if (infoFirmAdr[3].ToString() != "")
            {
                XAttribute SvProdIndex = new XAttribute("Индекс", infoFirmAdr[3].ToString());
                SvProdAdrRF.Add(SvProdIndex);
            }
        //    if (infoFirmAdr[4].ToString() != "")
        //    {
                XAttribute SvProdKodReg = new XAttribute("КодРегион", infoFirmAdr[4].ToString());
                SvProdAdrRF.Add(SvProdKodReg);
        //    }

            /*//<Документ><<СвКСчФ>><ГрузОт>
            XElement GruzOt = new XElement("ГрузОт");
            XElement GruzOtOnJe = new XElement("ОнЖе", "он же");
            SVSF.Add(GruzOt);
            GruzOt.Add(GruzOtOnJe);

            //<Документ><<СвКСчФ>><ГрузПолуч>
            XElement GruzPoluch = new XElement("ГрузПолуч");
            SVSF.Add(GruzPoluch);

            //<Документ><<СвКСчФ>><ГрузПолуч><ИдСв>
            XElement GruzPoluchIdSv = new XElement("ИдСв");
            GruzPoluch.Add(GruzPoluchIdSv);

            //<Документ><<СвКСчФ>><ГрузПолуч><ИдСв><СвЮЛУч>
            XElement GruzPoluchSvUluch = new XElement("СвЮЛУч");
            XAttribute GruzPoluchName = new XAttribute("НаимОрг", infoGpl[1]);
            XAttribute GruzPoluchINN = new XAttribute("ИННЮЛ", infoGpl[3]);
            XAttribute GruzPoluchKPP = new XAttribute("КПП", infoGpl[4]);

            GruzPoluchIdSv.Add(GruzPoluchSvUluch);
            GruzPoluchSvUluch.Add(GruzPoluchName);
            GruzPoluchSvUluch.Add(GruzPoluchINN);
            GruzPoluchSvUluch.Add(GruzPoluchKPP);

            //<Документ><<СвКСчФ>><ГрузПолуч><Адрес>
            XElement GruzPoluchAdres = new XElement("Адрес");
            GruzPoluch.Add(GruzPoluchAdres);

            //<Документ><<СвКСчФ>><ГрузПолуч><Адрес><АдресРФ>
            XElement GruzPoluchAdrRF = new XElement("АдрРФ");
            XAttribute GruzPoluchIndex = new XAttribute("Индекс", infoGpl[7]);
            XAttribute GruzPoluchKodReg = new XAttribute("КодРегион", infoGpl[8]);
            GruzPoluchAdres.Add(GruzPoluchAdrRF);
            GruzPoluchAdrRF.Add(GruzPoluchIndex);
            GruzPoluchAdrRF.Add(GruzPoluchKodReg);*/

            //<Документ><<СвКСчФ>><СвПокуп>
            XElement SvPokup = new XElement("СвПокуп");
            SVSF.Add(SvPokup);

            //<Документ><<СвКСчФ>><СвПокуп><ИдСв>
            XElement SvPokupIdSv = new XElement("ИдСв");
            SvPokup.Add(SvPokupIdSv);

            //<Документ><СвСчФакт><СвПокуп><ИдСв><СвЮЛУч>             
            XElement SvPokupSvUluch = new XElement("СвЮЛУч");
            XAttribute SvPokupName = new XAttribute("НаимОрг", infoKag[12]);
            XAttribute SvPokupINN = new XAttribute("ИННЮЛ", infoKag[3]);
            XAttribute SvPokupKPP = new XAttribute("КПП", infoKag[4]);
            SvPokupIdSv.Add(SvPokupSvUluch);
            SvPokupSvUluch.Add(SvPokupName);
            SvPokupSvUluch.Add(SvPokupINN);
            SvPokupSvUluch.Add(SvPokupKPP);

            //<Документ><<СвКСчФ>><СвПокуп><Адрес>
            XElement SvPokupAdres = new XElement("Адрес");
            SvPokup.Add(SvPokupAdres);

            //<Документ><<СвКСчФ>><СвПокуп><Адрес><АдресРФ>
            XElement SvPokupAdrRF = new XElement("АдрРФ");
            SvPokupAdres.Add(SvPokupAdrRF);
            if (infoGpl[7].ToString().Trim().Length > 0)
            {
                XAttribute SvPokupIndex = new XAttribute("Индекс", infoKag[7]);
                SvPokupAdrRF.Add(SvPokupIndex);
            }
          //  if (infoGpl[8].ToString().Length > 0) Атрибут обязательный
          //  {
                XAttribute SvPokupKodReg = new XAttribute("КодРегион", infoKag[8]);
                SvPokupAdrRF.Add(SvPokupKodReg);
           // }
            if (infoGpl[9].ToString().Length > 0)
            {
                XAttribute SvPokupCity = new XAttribute("Город", infoKag[9]);
                SvPokupAdrRF.Add(SvPokupCity);
            }
            if (infoGpl[10].ToString().Length > 0)
            {
                XAttribute SvPokupStreet = new XAttribute("Улица", infoKag[10]);
                SvPokupAdrRF.Add(SvPokupStreet);
            }
            if (infoGpl[11].ToString().Length > 0)
            {
                XAttribute SvPokupHouse = new XAttribute("Дом", infoKag[11]);
                SvPokupAdrRF.Add(SvPokupHouse);
            }
            //<Документ><<СвКСчФ>><ИнфПолФХЖ1>
            XElement DopSvFHJ1 = new XElement("ДопСвФХЖ1");
            XAttribute NaimOKV = new XAttribute("НаимОКВ", "Российский рубль");
            SVSF.Add(DopSvFHJ1);
            DopSvFHJ1.Add(NaimOKV);

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1>
            XElement InfPolFHJ1 = new XElement("ИнфПолФХЖ1");
            SVSF.Add(InfPolFHJ1);

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf1 = new XElement("ТекстИнф");
            XAttribute TxtInf1Identif = new XAttribute("Идентиф", "номер_заказа");
            XAttribute TxtInf1Znachen = new XAttribute("Значен", CurrDataUKD[6]);
            InfPolFHJ1.Add(TxtInf1);
            TxtInf1.Add(TxtInf1Identif);
            TxtInf1.Add(TxtInf1Znachen);

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf2 = new XElement("ТекстИнф");
            XAttribute TxtInf2Identif = new XAttribute("Идентиф", "отправитель");
            XAttribute TxtInf2Znachen = new XAttribute("Значен", ilnFirm);
            InfPolFHJ1.Add(TxtInf2);
            TxtInf2.Add(TxtInf2Identif);
            TxtInf2.Add(TxtInf2Znachen);

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf3 = new XElement("ТекстИнф");
            XAttribute TxtInf3Identif = new XAttribute("Идентиф", "получатель");
            XAttribute TxtInf3Znachen = new XAttribute("Значен", infoKag[2]);
            InfPolFHJ1.Add(TxtInf3);
            TxtInf3.Add(TxtInf3Identif);
            TxtInf3.Add(TxtInf3Znachen);

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf4 = new XElement("ТекстИнф");
            XAttribute TxtInf4Identif = new XAttribute("Идентиф", "код_поставщика");
            XAttribute TxtInf4Znachen = new XAttribute("Значен", codeByBuyer);
            InfPolFHJ1.Add(TxtInf4);
            TxtInf4.Add(TxtInf4Identif);
            TxtInf4.Add(TxtInf4Znachen);

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf5 = new XElement("ТекстИнф");
            XAttribute TxtInf5Identif = new XAttribute("Идентиф", "GLN_грузополучателя");
            XAttribute TxtInf5Znachen = new XAttribute("Значен", infoGpl[2]);
            InfPolFHJ1.Add(TxtInf5);
            TxtInf5.Add(TxtInf5Identif);
            TxtInf5.Add(TxtInf5Znachen);

            /************************** 3 уровень. <ТаблКСчФ> ************************/
            XElement TabSF = new XElement("ТаблКСчФ");
            DOC.Add(TabSF);

            decimal sumW0Nds_V = 0;
            decimal sumNds_V = 0;
            decimal sumWthNds_V = 0;

            decimal sumW0Nds_G = 0;
            decimal sumNds_G = 0;
            decimal sumWthNds_G = 0;

            decimal summWoNds_A = 0;
            decimal summWoNds_B = 0;

            //Item[] 0-Рсд, 1-ШК, 2-Название,  3-A_EiSh,  4-A_EiOkei,  5-A_EiEdi,  6-A_EiAcc,  7-A_QtOsn,  8-A_QtCn,  9-A_CnWoNds, 10-A_Tax, 11-A_SummNds, 12-A_SummWthNds 
            //                              , 13-B_EiSh, 14-B_EiOkei, 15-B_EiEdi, 16-B_EiAcc, 17-B_QtOsn, 18-B_QtCn, 19-B_CnWoNds, 20-B_Tax, 21-B_SummNds, 22-B_SummWthNds
            for (int i = 0; i < Item.GetLongLength(0); i++)
            {
                //<Документ><ТаблКСчФ><СведТов>
                XElement SvedTov = new XElement("СведТов");
                XAttribute NomStr = new XAttribute("НомСтр", Convert.ToString(i + 1));
                XAttribute NomTovVStr = new XAttribute("ПорНомТовВСЧФ", Convert.ToString(i + 1));
                XAttribute NaimTov = new XAttribute("НаимТов", Item[i, 2]);
                XAttribute OKEI_TovDo = new XAttribute("ОКЕИ_ТовДо", Item[i, 4]);
                XAttribute OKEI_TovPosle = new XAttribute("ОКЕИ_ТовПосле", Item[i, 14]);
                XAttribute KolTovDo = new XAttribute("КолТовДо", Math.Round(Convert.ToDecimal(Item[i, 8]), 2));
                XAttribute KolTovPosle = new XAttribute("КолТовПосле", Math.Round(Convert.ToDecimal(Item[i, 18]), 2));
                XAttribute CenaTovDo = new XAttribute("ЦенаТовДо", Item[i, 9]);
                XAttribute CenaTovPosle = new XAttribute("ЦенаТовПосле", Item[i, 19]);
                XAttribute NalStDo = new XAttribute("НалСтДо", Convert.ToString(Item[i, 10]));
                XAttribute NalStPosle = new XAttribute("НалСтПосле", Convert.ToString(Item[i, 10]));
                TabSF.Add(SvedTov);
                SvedTov.Add(NomStr);
                SvedTov.Add(NomTovVStr);
                SvedTov.Add(NaimTov);
                SvedTov.Add(OKEI_TovDo);
                SvedTov.Add(OKEI_TovPosle);
                SvedTov.Add(KolTovDo);
                SvedTov.Add(KolTovPosle);
                SvedTov.Add(CenaTovDo);
                SvedTov.Add(CenaTovPosle);
                SvedTov.Add(NalStDo);
                SvedTov.Add(NalStPosle);

                //<Документ><ТаблКСчФ><СведТов><СтТовБезНДС>
                XElement StTovBezNds = new XElement("СтТовБезНДС");
                summWoNds_A = Math.Round(Convert.ToDecimal(Item[i, 12]) - Convert.ToDecimal(Item[i, 11]), 2);
                summWoNds_B = Math.Round(Convert.ToDecimal(Item[i, 22]) - Convert.ToDecimal(Item[i, 21]), 2);
                XAttribute StoimDoIzm = new XAttribute("СтоимДоИзм", summWoNds_A);
                XAttribute StoimPosleIzm = new XAttribute("СтоимПослеИзм", summWoNds_B);
                XAttribute StoimUvel = new XAttribute("СтоимУвел", Math.Round(summWoNds_B - summWoNds_A, 2));
                XAttribute StoimUm = new XAttribute("СтоимУм", Math.Round(summWoNds_A - summWoNds_B, 2));
                SvedTov.Add(StTovBezNds);
                StTovBezNds.Add(StoimDoIzm);
                StTovBezNds.Add(StoimPosleIzm);
                if (summWoNds_A < summWoNds_B)
                {
                    StTovBezNds.Add(StoimUvel);
                    sumW0Nds_V = sumW0Nds_V + Math.Round(summWoNds_B - summWoNds_A, 2);
                }
                if (summWoNds_A > summWoNds_B)
                {
                    StTovBezNds.Add(StoimUm);
                    sumW0Nds_G = sumW0Nds_G + Math.Round(summWoNds_A - summWoNds_B, 2);
                }

                //<Документ><ТаблКСчФ><СведТов><АкцизДо>
                XElement AkcizDo = new XElement("АкцизДо");
                XElement bezAkcizDo = new XElement("БезАкциз", "без акциза");
                SvedTov.Add(AkcizDo);
                AkcizDo.Add(bezAkcizDo);

                //<Документ><ТаблКСчФ><СведТов><АкцизПосле>
                XElement AkcizPosle = new XElement("АкцизПосле");
                XElement bezAkcizPosle = new XElement("БезАкциз", "без акциза");
                SvedTov.Add(AkcizPosle);
                AkcizPosle.Add(bezAkcizPosle);

                //<Документ><ТаблКСчФ><СведТов><АкцизРазн>
                /*  XElement AkcizRazn = new XElement("АкцизРазн");
                XElement AkcizRaznSumUvel = new XElement("СумУвел", "0.00");
                XElement AkcizRaznSumUm = new XElement("СумУм", "0.00");
                SvedTov.Add(AkcizRazn);
                if (summWoNds_A < summWoNds_B) AkcizRazn.Add(AkcizRaznSumUvel);
                if (summWoNds_A > summWoNds_B) AkcizRazn.Add(AkcizRaznSumUm);*/

                //<Документ><ТаблКСчФ><СведТов><СумНалДо>
                XElement SumNalDo = new XElement("СумНалДо");
                SvedTov.Add(SumNalDo);
                if (Convert.ToString(Item[i, 10]) == "без НДС")
                {
                    //<Документ><ТаблКСчФ><СведТов><СумНалДо><БезНДС>
                    XElement BezNdsDo = new XElement("БезНДС", "без НДС");
                    SumNalDo.Add(BezNdsDo);
                }
                else
                {
                    //<Документ><ТаблКСчФ><СведТов><СумНалДо><СумНДС>
                    XElement SumNalSumNdsDo = new XElement("СумНДС", Math.Round(Convert.ToDecimal(Item[i, 11]), 2));
                    SumNalDo.Add(SumNalSumNdsDo);
                }

                //<Документ><ТаблКСчФ><СведТов><СумНалПосле>
                XElement SumNalPosle = new XElement("СумНалПосле");
                SvedTov.Add(SumNalPosle);
                if (Convert.ToString(Item[i, 20]) == "без НДС")
                {
                    //<Документ><ТаблКСчФ><СведТов><СумНалПосле><БезНДС>
                    XElement BezNdsPosle = new XElement("БезНДС", "без НДС");
                    SumNalPosle.Add(BezNdsPosle);
                }
                else
                {
                    //<Документ><ТаблКСчФ><СведТов><СумНалПосле><СумНДС>
                    XElement SumNalSumNdsPosle = new XElement("СумНДС", Math.Round(Convert.ToDecimal(Item[i, 21]), 2));
                    SumNalPosle.Add(SumNalSumNdsPosle);
                }

                //<Документ><ТаблКСчФ><СведТов><СумНалРазн>
                XElement SumNalRazn = new XElement("СумНалРазн");
                XElement SumNalRaznSumUvel = new XElement("СумУвел", Math.Round(Convert.ToDecimal(Item[i, 21]) - Convert.ToDecimal(Item[i, 11]), 2));
                XElement SumNalRaznSumUm = new XElement("СумУм", Math.Round(Convert.ToDecimal(Item[i, 11]) - Convert.ToDecimal(Item[i, 21]), 2));
                SvedTov.Add(SumNalRazn);
                if (summWoNds_A < summWoNds_B)
                {
                    SumNalRazn.Add(SumNalRaznSumUvel);
                    sumNds_V = sumNds_V + Math.Round(Convert.ToDecimal(Item[i, 21]) - Convert.ToDecimal(Item[i, 11]), 2);
                }
                if (summWoNds_A > summWoNds_B)
                {
                    SumNalRazn.Add(SumNalRaznSumUm);
                    sumNds_G = sumNds_G + Math.Round(Convert.ToDecimal(Item[i, 11]) - Convert.ToDecimal(Item[i, 21]), 2);
                }


                //<Документ><ТаблКСчФ><СведТов><СтТовУчНал>
                XElement StTovUchNal = new XElement("СтТовУчНал");
                XAttribute StTovUchNalStoimDoIzm = new XAttribute("СтоимДоИзм", Math.Round(Convert.ToDecimal(Item[i, 12]), 2));
                XAttribute StTovUchNalStoimPosleIzm = new XAttribute("СтоимПослеИзм", Math.Round(Convert.ToDecimal(Item[i, 22]), 2));
                XAttribute StTovUchNalStoimUvel = new XAttribute("СтоимУвел", Math.Round(Convert.ToDecimal(Item[i, 22]) - Convert.ToDecimal(Item[i, 12]), 2));
                XAttribute StTovUchNalStoimUm = new XAttribute("СтоимУм", Math.Round(Convert.ToDecimal(Item[i, 12]) - Convert.ToDecimal(Item[i, 22]), 2));
                SvedTov.Add(StTovUchNal);
                StTovUchNal.Add(StTovUchNalStoimDoIzm);
                StTovUchNal.Add(StTovUchNalStoimPosleIzm);
                if (summWoNds_A < summWoNds_B)
                    if (summWoNds_A > summWoNds_B)

                        if (summWoNds_A < summWoNds_B)
                        {
                            StTovUchNal.Add(StTovUchNalStoimUvel);
                            sumWthNds_V = sumWthNds_V + Math.Round(Convert.ToDecimal(Item[i, 22]) - Convert.ToDecimal(Item[i, 12]), 2);
                        }
                if (summWoNds_A > summWoNds_B)
                {
                    StTovUchNal.Add(StTovUchNalStoimUm);
                    sumWthNds_G = sumWthNds_G + Math.Round(Convert.ToDecimal(Item[i, 12]) - Convert.ToDecimal(Item[i, 22]), 2);
                }

                //<Документ><ТаблКСчФ><СведТов><ИнфПолФХЖ2>
                string nomBuyerCd = Verifiacation.GetBuyerItemCodeRcd(Convert.ToString(infoKag[5]), Convert.ToInt64(Item[i, 0]));

                XElement InfPolFHJ21 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf1Identif = new XAttribute("Идентиф", "код_материала");
                XAttribute ItmTxtInf1Znachen = new XAttribute("Значен", nomBuyerCd);
                SvedTov.Add(InfPolFHJ21);
                InfPolFHJ21.Add(ItmTxtInf1Identif);
                InfPolFHJ21.Add(ItmTxtInf1Znachen);

                //<Документ><ТаблКСчФ><СведТов><ИнфПолФХЖ2>
                XElement InfPolFHJ22 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf2Identif = new XAttribute("Идентиф", "штрихкод");
                XAttribute ItmTxtInf2Znachen = new XAttribute("Значен", Item[i, 1]);
                SvedTov.Add(InfPolFHJ22);
                InfPolFHJ22.Add(ItmTxtInf2Identif);
                InfPolFHJ22.Add(ItmTxtInf2Znachen);

                //<Документ><ТаблКСчФ><СведТов><ДопСведТов>
                XElement DopInfo = new XElement("ДопСведТов");
                XAttribute NmEiBefore = new XAttribute("НаимЕдИзмДо", Item[i, 3]);
                XAttribute NmEiAfter = new XAttribute("НаимЕдИзмПосле", Item[i, 13]);
                SvedTov.Add(DopInfo);
                DopInfo.Add(NmEiBefore);
                DopInfo.Add(NmEiAfter);
           
        }

            if (sumWthNds_V > 0)
            {
                //<Документ><ТаблКСчФ><ВсегоУвел>
                XElement VsegoUvel = new XElement("ВсегоУвел");
                XAttribute VsegoUvelBezNds = new XAttribute("СтТовБезНДСВсего", Math.Round(sumW0Nds_V, 2));
                XAttribute VsegoUvelSNDS = new XAttribute("СтТовУчНалВсего", Math.Round(sumWthNds_V, 2));
                TabSF.Add(VsegoUvel);
                VsegoUvel.Add(VsegoUvelBezNds);
                VsegoUvel.Add(VsegoUvelSNDS);

                //<Документ><ТаблКСчФ><ВсегоУвел><СумНал>
                XElement VsegoUvelSumNal = new XElement("СумНал");
                VsegoUvel.Add(VsegoUvelSumNal);
                if (Convert.ToString(Item[0, 10]) == "без НДС")
                {
                    //<Документ><ТаблКСчФ><ВсегоУвел><СумНал><БезНДС>
                    XElement VsegoUvelSumNalBezNds = new XElement("БезНДС", "без НДС");
                    VsegoUvelSumNal.Add(VsegoUvelSumNalBezNds);
                }
                else
                {
                    //<Документ><ТаблКСчФ><ВсегоОпл><СумНал><СумНал>
                    XElement VsegoUvelSumNalSumMal = new XElement("СумНДС", Math.Round(sumNds_V, 2));
                    VsegoUvelSumNal.Add(VsegoUvelSumNalSumMal);
                }
            }

            if (sumWthNds_G > 0)
            {
                //<Документ><ТаблКСчФ><ВсегоУм>
                XElement VsegoUm = new XElement("ВсегоУм");
                XAttribute VsegoUmBezNds = new XAttribute("СтТовБезНДСВсего", Math.Round(sumW0Nds_G, 2));
                XAttribute VsegoUmSNDS = new XAttribute("СтТовУчНалВсего", Math.Round(sumWthNds_G, 2));
                TabSF.Add(VsegoUm);
                VsegoUm.Add(VsegoUmBezNds);
                VsegoUm.Add(VsegoUmSNDS);

                //<Документ><ТаблКСчФ><ВсегоУвел><СумНал>
                XElement VsegoUmSumNal = new XElement("СумНал");
                VsegoUm.Add(VsegoUmSumNal);
                if (Convert.ToString(Item[0, 10]) == "без НДС")
                {
                    //<Документ><ТаблКСчФ><ВсегоУвел><СумНал><БезНДС>
                    XElement VsegoUmSumNalBezNds = new XElement("БезНДС", "без НДС");
                    VsegoUmSumNal.Add(VsegoUmSumNalBezNds);
                }
                else
                {
                    //<Документ><ТаблКСчФ><ВсегоОпл><СумНал><СумНал>
                    XElement VsegoUmSumNalSumMal = new XElement("СумНДС", Math.Round(sumNds_G, 2));
                    VsegoUmSumNal.Add(VsegoUmSumNalSumMal);
                }
            }

            /************************** 3 уровень. <СодФХЖ3> ************************/
            XElement SodFHJ3 = new XElement("СодФХЖ3");
            XAttribute SodOper = new XAttribute("СодОпер", "Корректировка");
            XAttribute DataNapr = new XAttribute("ДатаНапр", DateTime.Today.ToString(@"dd.MM.yyyy"));
            DOC.Add(SodFHJ3);
            SodFHJ3.Add(SodOper);
            SodFHJ3.Add(DataNapr);

            //<Документ><СодФХЖ3><ПередатДокум>
            XElement PeredDoc = new XElement("ПередатДокум");
            XAttribute PeredDocNmOsn = new XAttribute("НаимОсн", "Универсальный передаточный документ");
            XAttribute PeredDocDataOsn = new XAttribute("ДатаОсн", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));
            XAttribute PeredDocNmrOsn = new XAttribute("НомОсн", infoCorSf[0].ToString());
            SodFHJ3.Add(PeredDoc);
            PeredDoc.Add(PeredDocNmOsn);
            PeredDoc.Add(PeredDocDataOsn);
            PeredDoc.Add(PeredDocNmrOsn);

            //<Документ><СодФХЖ3><ОснКор>
            XElement PeredDocOsnKorr = new XElement("ДокумОснКор");
            XAttribute NaimOsn = new XAttribute("НаимОсн", "Иные");
            XAttribute DataOsn = new XAttribute("ДатаОсн", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));
            XAttribute DopSvedOsn = new XAttribute("ДопСвОсн", "Отсутствуют");
            SodFHJ3.Add(PeredDocOsnKorr);
            PeredDocOsnKorr.Add(NaimOsn);
            PeredDocOsnKorr.Add(DataOsn);
            PeredDocOsnKorr.Add(DopSvedOsn);

            /************************** 3 уровень. <Подписант> ************************/
            string[] infoSigner = Verifiacation.GetSignerOpt(); //0 ОБЩИЕ.ЭЦПДОЛЖНОСТЬ, 1 ОБЩИЕ.ЭЦПФАМИЛИЯ, 2 ОБЩИЕ.ЭЦПИМЯ, 3 ОБЩИЕ.ЭЦПОТЧЕСТВО, 4 ОБЩИЕ.ЭЦПДОЛЖНОБЯЗ

            XElement Podp = new XElement("Подписант");
            XAttribute obl = new XAttribute("ОблПолн", "6");
            XAttribute status = new XAttribute("Статус", "1");
            XAttribute osn = new XAttribute("ОснПолн", infoSigner[4]);
            DOC.Add(Podp);
            Podp.Add(obl);
            Podp.Add(status);
            Podp.Add(osn);

            //<Документ><Подписант><ЮЛ>
            XElement UL = new XElement("ЮЛ");
            XAttribute innUl = new XAttribute("ИННЮЛ", infoFirm[1]);
            XAttribute naimOrg = new XAttribute("НаимОрг", infoFirm[0]);
            XAttribute dolj = new XAttribute("Должн", infoSigner[0]);
            Podp.Add(UL);
            UL.Add(innUl);
            UL.Add(naimOrg);
            UL.Add(dolj);

            //<Документ><Подписант><ЮЛ><ФИО>
            XElement FIO = new XElement("ФИО");
            XAttribute famdir = new XAttribute("Фамилия", infoSigner[1]);
            XAttribute namedir = new XAttribute("Имя", infoSigner[2]);
            XAttribute otchesdir = new XAttribute("Отчество", infoSigner[3]);
            UL.Add(FIO);
            FIO.Add(famdir);
            FIO.Add(namedir);
            FIO.Add(otchesdir);



            //------сохранение документа-----------
            fileName = fileName + ".xml";
            try
            {
                xdoc.Save(pathArchiveEDI + fileName);
                try
                {
                    xdoc.Save(pathUKDEDI + fileName);
                    string message = "EDISOFT. УКД " + fileName + " создан в " + pathUKDEDI;
                    Program.WriteLine(message);
                    DispOrders.WriteProtocolEDI("УКД", fileName, infoKag[0] + " - " + infoKag[1], 0, infoGpl[0] + " - " + infoGpl[1], "УКД сформирован", DateTime.Now, Convert.ToString(CurrDataUKD[6]), "EDISOFT");
                    DispOrders.WriteEDiSentDoc("8", fileName, Convert.ToString(CurrDataUKD[3]), Convert.ToString(infoSf[0]), "123", Convert.ToString(sumWthNds_V - sumWthNds_G), Convert.ToString(CurrDataUKD[7]),1);
                    //запись в лог о удаче
                }
                catch (Exception e)
                {
                    string message_error = "EDISOFT. Не могу создать xml файл УКД в " + pathUKDEDI + ". Нет доступа или диск переполнен.";
                    DispOrders.WriteProtocolEDI("УКД", fileName, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УКД не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUKD[6]), "EDISOFT");
                    Program.WriteLine(message_error);
                    DispOrders.WriteErrorLog(e.Message);
                }
            }
            catch (Exception e)
            {
                string message_error = "EDISOFT. Не могу создать xml файл УКД в " + pathArchiveEDI + ". Нет доступа или диск переполнен.";
                DispOrders.WriteProtocolEDI("УКД", fileName, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УКД не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUKD[6]), "EDISOFT");
                Program.WriteLine(message_error);
                DispOrders.WriteErrorLog(e.Message);
                //запись в лог о неудаче
            }
        }

        /*************************************************************************************************************************************/
        /********************************************************** Начало УПД X5 ************************************************************/
        /*************************************************************************************************************************************/
        public static void CreateEdiX5UPD(List<object> CurrDataUPD, string typeFunc) //список УПД, 0 ProviderOpt, 1 ProviderZkg, 2 NastDoc_Fmt, 3 SklSf_Rcd, 4 SklSf_TpOtg, 5 SklSfA_RcdCor, 6 PrdZkg_NmrExt, 7 PrdZkg_Rcd, 8 PrdZkg_Dt ,9 SklNk_TDrvNm
        {
            //получение путей
            string pathArchiveEDI = /*"D:\\Edi\\Archive\\"; //*/ DispOrders.GetValueOption("EDI-СОФТ.АРХИВ");
            string pathUPDEDI;

            //Запрос данных СФ
            object[] infoSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataUPD[3])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO

            //запрос данных спецификации
            object[,] Item = Verifiacation.GetItemsFromSF(Convert.ToString(CurrDataUPD[3]), true); //0 BarCode_Code, 1 SklN_Rcd, 2 SklN_Cd, 3 SklN_NmAlt, 4 Кол-во, 5 Цена без НДC, 6 Цена с НДС, 7 Код ЕИ EDI, 8 ОКЕЙ, 9 Ставка, 10 'S', 11 Сумма НДС, 12 Сумма с НДС, 13 шифр ЕИ, 14 Вес

            //Запрос данных покупателя
            object[] infoKag = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(infoSf[2]), Convert.ToInt64(infoSf[3])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование
            //Запрос данных покупателя
            object[] infoGpl = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(infoSf[4]), Convert.ToInt64(infoSf[5])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование

            string codeByBuyer = Verifiacation.GetFldFromEdiExch(Convert.ToInt64(CurrDataUPD[7]), "Exch_USelCdByer"); //Код поставщика для Ашана

            //какой gln номер использовать
            bool useMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(infoSf[4]));
            string ilnFirm;

            object[] infoFirm;
            object[] infoFirmAdr;
            object[] infoFirmGrOt; //данные грузоотправителя
            object[] infoFirmAdrGrOt; //адрес грузоотправителя
            if (useMasterGLN == false)//используем данные текущего предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ИЛН");
                pathUPDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/ DispOrders.GetValueOption("EDI-СОФТ.УПД");
                infoFirm = Verifiacation.GetFirmInfo(); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO
                infoFirmAdr = Verifiacation.GetFirmAdr(); // 0 CrtAdr_StrNm+','+CrtAdr_House, 1 CrtAdr_TowNm, 2 CrtAdr_RegNm, 3 CrtAdr_Ind, 4 CrtAdr_RegCd
                infoFirmGrOt = infoFirm;
                infoFirmAdrGrOt = infoFirmAdr;

            }
            else//используем данные головного предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");
                infoFirm = Verifiacation.GetMasterFirmInfo();
                infoFirmAdr = Verifiacation.GetMasterFirmAdr();
                infoFirmGrOt = Verifiacation.GetFirmInfo(); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO
                infoFirmAdrGrOt = Verifiacation.GetFirmAdr(); // 0 CrtAdr_StrNm+','+CrtAdr_House, 1 CrtAdr_TowNm, 2 CrtAdr_RegNm, 3 CrtAdr_Ind, 4 CrtAdr_RegCd, 5 CrtAdr_StrNm, 6 CrtAdr_House 
                try
                {
                    pathUPDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/ DispOrders.GetValueOption("EDI-СОФТ.ЭКСПОРТ");
                }
                catch
                {
                    pathUPDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/ DispOrders.GetValueOption("EDI-СОФТ.УПД");
                }
            }

            string idEdo = DispOrders.GetValueOption("EDI-СОФТ.ИДЭДО");  //"2IJ"; //ИдЭДО

            string idOtpr = idEdo + ilnFirm; //ИдОтпр
            string idPol = idEdo + infoGpl[2].ToString(); //ИдПол

            string guid = Convert.ToString(Guid.NewGuid());
            string fileName;
            if (CurrDataUPD[2].ToString().Equals("X5Mark"))
            {
                fileName = "ON_NSCHFDOPPRMARK_" + idPol + "_" + idOtpr + "_" + DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid;//ИдФайл
            }
            else
            {
                fileName = "ON_NSCHFDOPPR_" + idPol + "_" + idOtpr + "_" + DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid;//ИдФайл
            }
            

            /************************** 1 уровень. <Файл> ******************************/

            XDocument xdoc = new XDocument(new XDeclaration("1.0", "", ""));

            XElement File = new XElement("Файл");
            XAttribute IdFile = new XAttribute("ИдФайл", "000000000000000000000000000000000000");
            XAttribute VersForm = new XAttribute("ВерсФорм", "5.01");
            XAttribute VersProg = new XAttribute("ВерсПрог", "Эдисофт");

            xdoc.Add(File);
            File.Add(IdFile);
            File.Add(VersForm);
            File.Add(VersProg);

            /************************** 2 уровень. <СвУчДокОбор> ************************/

            XElement ID = new XElement("СвУчДокОбор");

            XAttribute IdSender = new XAttribute("ИдОтпр", "0000000000"/*idOtpr*/);
            XAttribute IdReciever = new XAttribute("ИдПол", "0000000000"/*idPol*/);

            File.Add(ID);
            ID.Add(IdSender);
            ID.Add(IdReciever);

            //<СвУчДокОбор><СвОЭДОтпр>
            XElement InfOrg = new XElement("СвОЭДОтпр");
            string providerNm = DispOrders.GetValueOption("EDI-СОФТ.НМ");
            string providerInn = DispOrders.GetValueOption("EDI-СОФТ.ИНН");
            XAttribute NaimOrg = new XAttribute("НаимОрг", providerNm/*"Эдисофт, ООО"*/);
            XAttribute INNUL = new XAttribute("ИННЮЛ", providerInn/*"7801471082"*/);
            XAttribute IDEDO = new XAttribute("ИдЭДО", idEdo);

            ID.Add(InfOrg);
            InfOrg.Add(NaimOrg);
            InfOrg.Add(INNUL);
            InfOrg.Add(IDEDO);

            /************************** 2 уровень. <Документ> ************************/

            XElement DOC = new XElement("Документ");
            XAttribute KND = new XAttribute("КНД", "1115131");
            XAttribute Function = new XAttribute("Функция", typeFunc);
            XAttribute PoFakt = new XAttribute("ПоФактХЖ", "Документ об отгрузке товаров (выполнении работ), передаче имущественных прав (документ об оказании услуг)");
            XAttribute NaimDocOpr;
            if (typeFunc == "СЧФДОП")
                NaimDocOpr = new XAttribute("НаимДокОпр", "Счет-фактура и документ об отгрузке товаров (выполнении работ), передаче имущественных прав (документ об оказании услуг)");
            else if (typeFunc == "ДОП")
                NaimDocOpr = new XAttribute("НаимДокОпр", "Документ об отгрузке товаров (выполнении работ), передаче имущественных прав (документ об оказании услуг)");
            else
                NaimDocOpr = new XAttribute("НаимДокОпр", "");
            XAttribute DateF = new XAttribute("ДатаИнфПр", DateTime.Today.ToString(@"dd.MM.yyyy"));
            XAttribute TimeF = new XAttribute("ВремИнфПр", DateTime.Today.ToString(@"hh.mm.ss"));
            XAttribute NameOrg = new XAttribute("НаимЭконСубСост", infoFirm[0].ToString() + ", ИНН-КПП: " + infoFirm[1].ToString() + "-" + infoFirm[2].ToString());

            File.Add(DOC);
            DOC.Add(KND);
            DOC.Add(Function);
            if (typeFunc != "СЧФ")
            {
                DOC.Add(PoFakt);
                DOC.Add(NaimDocOpr);
            }
            DOC.Add(DateF);
            DOC.Add(TimeF);
            DOC.Add(NameOrg);

            //<Документ><СвСчФакт>
            XElement SVSF = new XElement("СвСчФакт");
            XAttribute NomSF = new XAttribute("НомерСчФ", infoSf[0].ToString());
            XAttribute DateSF = new XAttribute("ДатаСчФ", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));
            XAttribute Kod = new XAttribute("КодОКВ", infoSf[6].ToString());

            DOC.Add(SVSF);
            SVSF.Add(NomSF);
            SVSF.Add(DateSF);
            SVSF.Add(Kod);

            //исправление
            XElement IsprSF = new XElement("ИспрСчФ");
            SVSF.Add(IsprSF);
            //Проверяем необходимо ли отправить исправление, либо же это обычная отправка
            if (CurrDataUPD[11].ToString() == "0") //Обычная отправка
            {
                
                //если нет исправления
                XAttribute DfNISF = new XAttribute("ДефНомИспрСчФ", "-");
                XAttribute DfDISF = new XAttribute("ДефДатаИспрСчФ", "-");
                IsprSF.Add(DfNISF);
                IsprSF.Add(DfDISF);
            }
            else //есть исправление
            {
               XAttribute NmIsprSF = new XAttribute("НомИспрСчФ", CurrDataUPD[11].ToString());
               XAttribute DtIsprSF = new XAttribute("ДатаИспрСчФ", DateTime.Today.ToString(@"dd.MM.yyyy"));
               IsprSF.Add(NmIsprSF);
               IsprSF.Add(DtIsprSF);
            }
            
            //<Документ><СвСчФакт><СвПрод>            
            XElement SvProd = new XElement("СвПрод");
            //XAttribute SvProdOKPO = new XAttribute("ОКПО", infoFirm[3].ToString());
            SVSF.Add(SvProd);
            //SvProd.Add(SvProdOKPO);

            //<Документ><СвСчФакт><СвПрод><ИдСв>
            XElement SvProdIdSv = new XElement("ИдСв");
            SvProd.Add(SvProdIdSv);

            //<Документ><СвСчФакт><СвПрод><ИдСв><СвЮЛУч>
            XElement SvProdSvUluchh = new XElement("СвЮЛУч");
            XAttribute SvProdIdSvName = new XAttribute("НаимОрг", infoFirm[0].ToString());
            XAttribute SvProdIdSvINN = new XAttribute("ИННЮЛ", infoFirm[1].ToString());
            XAttribute SvProdIdSvKPP = new XAttribute("КПП", infoFirm[2].ToString());
            if (useMasterGLN)  //используем данные головного предприятия, поэтому КПП указываем свой
            {
                SvProdIdSvKPP = new XAttribute("КПП", infoFirmGrOt[2].ToString());
            }

            SvProdIdSv.Add(SvProdSvUluchh);
            SvProdSvUluchh.Add(SvProdIdSvName);
            SvProdSvUluchh.Add(SvProdIdSvINN);
            SvProdSvUluchh.Add(SvProdIdSvKPP);

            //<Документ><СвСчФакт><СвПрод><Адрес>
            XElement SvProdAdres = new XElement("Адрес");
            SvProd.Add(SvProdAdres);

            //<Документ><СвСчФакт><СвПрод><Адрес><АдресРФ>
            //Адрес
            XElement SvProdAdrRF = new XElement("АдрРФ");
            SvProdAdres.Add(SvProdAdrRF);
            if (infoFirmAdr[3].ToString() != "")
            {
                XAttribute SvProdIndex = new XAttribute("Индекс", infoFirmAdr[3].ToString());
                SvProdAdrRF.Add(SvProdIndex);
            }
            XAttribute SvProdKodReg = new XAttribute("КодРегион", infoFirmAdr[4].ToString());
            SvProdAdrRF.Add(SvProdKodReg);

            //<Документ><СвСчФакт><ГрузОт>

            XElement GruzOt = new XElement("ГрузОт");
            if (useMasterGLN)
            {
                XElement GruzOtOther = new XElement("ГрузОтпр");
                SVSF.Add(GruzOt);
                GruzOt.Add(GruzOtOther);

                XElement SvGrOtpIdSv = new XElement("ИдСв");
                GruzOtOther.Add(SvGrOtpIdSv);

                //<Документ><СвСчФакт><СвПрод><ИдСв><СвЮЛУч>
                XElement SvGrOtpSvUluchh = new XElement("СвЮЛУч");
                XAttribute SvGrOtpIdSvName = new XAttribute("НаимОрг", infoFirmGrOt[0].ToString());
                XAttribute SvGrOtpIdSvINN = new XAttribute("ИННЮЛ", infoFirmGrOt[1].ToString());
                XAttribute SvGrOtpIdSvKPP = new XAttribute("КПП", infoFirmGrOt[2].ToString());

                SvGrOtpIdSv.Add(SvGrOtpSvUluchh);
                SvGrOtpSvUluchh.Add(SvGrOtpIdSvName);
                SvGrOtpSvUluchh.Add(SvGrOtpIdSvINN);
                SvGrOtpSvUluchh.Add(SvGrOtpIdSvKPP);

                //<Документ><СвСчФакт><СвПрод><Адрес>
                XElement SvGrOtpAdres = new XElement("Адрес");
                GruzOtOther.Add(SvGrOtpAdres);

                XElement SvGrOtpAdrRF = new XElement("АдрРФ");
                SvGrOtpAdres.Add(SvGrOtpAdrRF);
                if (infoFirmAdrGrOt[3].ToString() != "")
                {
                    XAttribute SvGrOtpIndex = new XAttribute("Индекс", infoFirmAdrGrOt[3].ToString());
                    SvGrOtpAdrRF.Add(SvGrOtpIndex);
                }
                XAttribute SvGrOtpKodReg = new XAttribute("КодРегион", infoFirmAdrGrOt[4].ToString());
                SvGrOtpAdrRF.Add(SvGrOtpKodReg);
                if (infoFirmAdrGrOt[1].ToString() != "")
                {
                    XAttribute SvGrOtpGorod = new XAttribute("Город", infoFirmAdrGrOt[1].ToString());
                    SvGrOtpAdrRF.Add(SvGrOtpGorod);
                }
                if (infoFirmAdrGrOt[5].ToString() != "")
                {
                    XAttribute SvGrOtpStreet = new XAttribute("Улица", infoFirmAdrGrOt[5].ToString());
                    SvGrOtpAdrRF.Add(SvGrOtpStreet);
                }
                if (infoFirmAdrGrOt[6].ToString() != "")
                {
                    XAttribute SvGrOtpHouse = new XAttribute("Дом", infoFirmAdrGrOt[6].ToString());
                    SvGrOtpAdrRF.Add(SvGrOtpHouse);
                }

            }
            else
            {
                XElement GruzOtOnJe = new XElement("ОнЖе", "он же");
                SVSF.Add(GruzOt);
                GruzOt.Add(GruzOtOnJe);
            }

            //<Документ><СвСчФакт><ГрузПолуч>
            XElement GruzPoluch = new XElement("ГрузПолуч");
            SVSF.Add(GruzPoluch);

            //<Документ><СвСчФакт><ГрузПолуч><ИдСв>
            XElement GruzPoluchIdSv = new XElement("ИдСв");
            GruzPoluch.Add(GruzPoluchIdSv);

            //<Документ><СвСчФакт><ГрузПолуч><ИдСв><СвЮЛУч>
            XElement GruzPoluchSvUluch = new XElement("СвЮЛУч");
            XAttribute GruzPoluchName = new XAttribute("НаимОрг", "#NAME#"/*infoGpl[1]*/);
            XAttribute GruzPoluchINN = new XAttribute("ИННЮЛ", infoGpl[3]);
            GruzPoluchIdSv.Add(GruzPoluchSvUluch);
            GruzPoluchSvUluch.Add(GruzPoluchName);
            GruzPoluchSvUluch.Add(GruzPoluchINN);
            if(infoGpl[4].ToString().Trim().Length > 0)
            {
                 XAttribute GruzPoluchKPP = new XAttribute("КПП", infoGpl[4]);
                GruzPoluchSvUluch.Add(GruzPoluchKPP);
            }
            

            //<Документ><СвСчФакт><ГрузПолуч><Адрес>
            XElement GruzPoluchAdres = new XElement("Адрес");
            GruzPoluch.Add(GruzPoluchAdres);

            //<Документ><СвСчФакт><ГрузПолуч><Адрес><АдресРФ>
            XElement GruzPoluchAdrRF = new XElement("АдрРФ");
            GruzPoluchAdres.Add(GruzPoluchAdrRF);
            if (infoGpl[7].ToString().Trim().Length > 0)
            {
                XAttribute GruzPoluchIndex = new XAttribute("Индекс", infoGpl[7]);
                GruzPoluchAdrRF.Add(GruzPoluchIndex);
            }
           // if (infoGpl[8].ToString().Length > 0) Атрибут обязательный
           // {
                XAttribute GruzPoluchKodReg = new XAttribute("КодРегион", infoGpl[8]);
                GruzPoluchAdrRF.Add(GruzPoluchKodReg);
           // }
            if (infoGpl[9].ToString().Length > 0)
            {
                XAttribute GruzPoluchCity = new XAttribute("Город", infoGpl[9]);
                GruzPoluchAdrRF.Add(GruzPoluchCity);
            }
            if (infoGpl[10].ToString().Length > 0)
            {
                XAttribute GruzPoluchStreet = new XAttribute("Улица", infoGpl[10]);
                GruzPoluchAdrRF.Add(GruzPoluchStreet);
            }
            if (infoGpl[11].ToString().Length > 0)
            {
                XAttribute GruzPoluchHouse = new XAttribute("Дом", infoGpl[11]);
                GruzPoluchAdrRF.Add(GruzPoluchHouse);
            }


            //<Документ><СвСчФакт><СвПокуп>
            XElement SvPokup = new XElement("СвПокуп");
            SVSF.Add(SvPokup);

            //<Документ><СвСчФакт><СвПокуп><ИдСв>
            XElement SvPokupIdSv = new XElement("ИдСв");
            SvPokup.Add(SvPokupIdSv);

            //<Документ><СвСчФакт><СвПокуп><ИдСв><СвЮЛУч>             
            XElement SvPokupSvUluch = new XElement("СвЮЛУч");
            XAttribute SvPokupName = new XAttribute("НаимОрг", "#NAME#"/*infoKag[1]*/);
            XAttribute SvPokupINN = new XAttribute("ИННЮЛ", infoKag[3]);
            XAttribute SvPokupKPP = new XAttribute("КПП", infoKag[4]);
            SvPokupIdSv.Add(SvPokupSvUluch);
            SvPokupSvUluch.Add(SvPokupName);
            SvPokupSvUluch.Add(SvPokupINN);
            SvPokupSvUluch.Add(SvPokupKPP);

            //<Документ><СвСчФакт><СвПокуп><Адрес>
            XElement SvPokupAdres = new XElement("Адрес");
            SvPokup.Add(SvPokupAdres);

            //<Документ><СвСчФакт><СвПокуп><Адрес><АдресРФ>
            XElement SvPokupAdrRF = new XElement("АдрРФ");
            SvPokupAdres.Add(SvPokupAdrRF);
            if (infoKag[7].ToString().Trim().Length > 0)
            {
                XAttribute SvPokupIndex = new XAttribute("Индекс", infoKag[7]);
                SvPokupAdrRF.Add(SvPokupIndex);
            }
        //    if (infoGpl[8].ToString().Length > 0)  атрибут обязательный
        //    {
                XAttribute SvPokupKodReg = new XAttribute("КодРегион", infoKag[8]);
                SvPokupAdrRF.Add(SvPokupKodReg);
        //    }
            if (infoKag[9].ToString().Length > 0)
            {
                XAttribute SvPokupCity = new XAttribute("Город", infoKag[9]);
                SvPokupAdrRF.Add(SvPokupCity);
            }
            if (infoKag[10].ToString().Length > 0)
            {
                XAttribute SvPokupStreet = new XAttribute("Улица", infoKag[10]);
                SvPokupAdrRF.Add(SvPokupStreet);
            }
            if (infoKag[11].ToString().Length > 0)
            {
                XAttribute SvPokupHouse = new XAttribute("Дом", infoKag[11]);
                SvPokupAdrRF.Add(SvPokupHouse);
            }


            //<Документ><СвСчФакт><ИнфПолФХЖ1>
            XElement DopSvFHJ1 = new XElement("ДопСвФХЖ1");
            XAttribute NaimOKV = new XAttribute("НаимОКВ", "643");
            SVSF.Add(DopSvFHJ1);
            DopSvFHJ1.Add(NaimOKV);

            //<Документ><СвСчФакт><ИнфПолФХЖ1>
            XElement InfPolFHJ1 = new XElement("ИнфПолФХЖ1");
            SVSF.Add(InfPolFHJ1);

            /*//<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf1 = new XElement("ТекстИнф");
            XAttribute TxtInf1Identif = new XAttribute("Идентиф", "номер_заказа");
            XAttribute TxtInf1Znachen = new XAttribute("Значен", CurrDataUPD[6]);
            InfPolFHJ1.Add(TxtInf1);
            TxtInf1.Add(TxtInf1Identif);
            TxtInf1.Add(TxtInf1Znachen);*/

            //<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf2 = new XElement("ТекстИнф");
            XAttribute TxtInf2Identif = new XAttribute("Идентиф", "отправитель");
            XAttribute TxtInf2Znachen = new XAttribute("Значен", ilnFirm);
            InfPolFHJ1.Add(TxtInf2);
            TxtInf2.Add(TxtInf2Identif);
            TxtInf2.Add(TxtInf2Znachen);

            //<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf3 = new XElement("ТекстИнф");
            XAttribute TxtInf3Identif = new XAttribute("Идентиф", "получатель");
            XAttribute TxtInf3Znachen = new XAttribute("Значен", infoKag[2]);
            InfPolFHJ1.Add(TxtInf3);
            TxtInf3.Add(TxtInf3Identif);
            TxtInf3.Add(TxtInf3Znachen);

            /*//<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf4 = new XElement("ТекстИнф");
            XAttribute TxtInf4Identif = new XAttribute("Идентиф", "код_поставщика");
            XAttribute TxtInf4Znachen = new XAttribute("Значен", codeByBuyer);
            InfPolFHJ1.Add(TxtInf4);
            TxtInf4.Add(TxtInf4Identif);
            TxtInf4.Add(TxtInf4Znachen);*/

            //<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf5 = new XElement("ТекстИнф");
            XAttribute TxtInf5Identif = new XAttribute("Идентиф", "грузополучатель");
            XAttribute TxtInf5Znachen = new XAttribute("Значен", infoGpl[2]);
            InfPolFHJ1.Add(TxtInf5);
            TxtInf5.Add(TxtInf5Identif);
            TxtInf5.Add(TxtInf5Znachen);

            /************************** 3 уровень. <ТаблСчФакт> ************************/
            XElement TabSF = new XElement("ТаблСчФакт");
            DOC.Add(TabSF);

            decimal sumWthNds = 0;
            decimal sumNds = 0;
            decimal sumWeight = 0;

            for (int i = 0; i < Item.GetLongLength(0); i++) //Item[] //0 BarCode_Code, 1 SklN_Rcd, 2 SklN_Cd, 3 SklN_NmAlt, 4 Кол-во, 5 Цена без НДC, 6 Цена с НДС, 7 Код ЕИ EDI, 8 ОКЕЙ, 9 Ставка, 10 'S', 11 Сумма НДС, 12 Сумма с НДС, 13 шифр ЕИ, 14 Вес
            {
                sumWthNds = sumWthNds + Convert.ToDecimal(Item[i, 12]);
                sumNds = sumNds + Convert.ToDecimal(Item[i, 11]);
                sumWeight = sumWeight + Convert.ToDecimal(Item[i, 14]);
                //<Документ><ТаблСчФакт><СведТов>
                XElement SvedTov = new XElement("СведТов");
                XAttribute NomStr = new XAttribute("НомСтр", Convert.ToString(i + 1));
                XAttribute NaimTov = new XAttribute("НаимТов", Item[i, 3]);
                XAttribute OKEI = new XAttribute("ОКЕИ_Тов", Item[i, 8]);
                XAttribute KolTov = new XAttribute("КолТов", Math.Round(Convert.ToDecimal(Item[i, 4]), 2));
                XAttribute CenaTov = new XAttribute("ЦенаТов", Item[i, 5]);
                XAttribute BezNDS = new XAttribute("СтТовБезНДС", Math.Round(Convert.ToDecimal(Item[i, 12]) - Convert.ToDecimal(Item[i, 11]), 2));
                XAttribute NalSt = new XAttribute("НалСт", Convert.ToString(Item[i, 9]));
                XAttribute sNDS = new XAttribute("СтТовУчНал", Math.Round(Convert.ToDecimal(Item[i, 12]), 2));
                TabSF.Add(SvedTov);
                SvedTov.Add(NomStr);
                SvedTov.Add(NaimTov);
                SvedTov.Add(OKEI);
                SvedTov.Add(KolTov);
                SvedTov.Add(CenaTov);
                SvedTov.Add(BezNDS);
                SvedTov.Add(NalSt);
                SvedTov.Add(sNDS);

                //<Документ><ТаблСчФакт><СведТов><Акциз>
                XElement Akciz = new XElement("Акциз");
                XElement bezAkciz = new XElement("БезАкциз", "без акциза");
                SvedTov.Add(Akciz);
                Akciz.Add(bezAkciz);

                //<Документ><ТаблСчФакт><СведТов><СумНал>
                XElement SumNal = new XElement("СумНал");
                SvedTov.Add(SumNal);
                if (Convert.ToString(Item[i, 9]) == "без НДС")
                {
                    //<Документ><ТаблСчФакт><СведТов><СумНал><БезНДС>
                    XElement BezNds = new XElement("БезНДС", "без НДС");
                    SumNal.Add(BezNds);
                }
                else
                {
                    //<Документ><ТаблСчФакт><СведТов><СумНал><СумНал>
                    XElement SumNalSum = new XElement("СумНал", Math.Round(Convert.ToDecimal(Item[i, 11]), 2));
                    SumNal.Add(SumNalSum);
                }

                //<Документ><ТаблСчФакт><СведТов><СвТД>
                XElement SvTD = new XElement("СвТД");
                XAttribute DfKP = new XAttribute("ДефКодПроисх",  "-");
                SvedTov.Add(SvTD);
                SvTD.Add(DfKP);

                //<Документ><ТаблСчФакт><СведТов><ДопСведТов>
                XElement DopSvedTov = new XElement("ДопСведТов");
                XAttribute PrTovRav = new XAttribute("ПрТовРаб", "1");
                XAttribute NaimEdIzm = new XAttribute("НаимЕдИзм", Item[i, 13]);
                SvedTov.Add(DopSvedTov);
                DopSvedTov.Add(PrTovRav);
                DopSvedTov.Add(NaimEdIzm);

                //<Документ><ТаблСчФакт><СведТов><ДопСведТов><НомСредИдентТов><НомУпак>     маркировка
                if (Item[i, 9].ToString().Contains("10") && CurrDataUPD[2].ToString().Equals("X5Mark"))
                {
                    string nomUpakValue = "020" + Item[i, 0] + "37";
                    nomUpakValue += (Math.Round(Convert.ToDecimal(Item[i, 4]))).ToString();
                    XElement NomSredIdent = new XElement("НомСредИдентТов");
                    XElement NomUpak = new XElement("НомУпак", nomUpakValue);
                    DopSvedTov.Add(NomSredIdent);
                    NomSredIdent.Add(NomUpak);
                }

                //<Документ><ТаблСчФакт><СведТов><ИнфПолФХЖ2>
                XElement InfPolFHJ23 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf3Identif = new XAttribute("Идентиф", "номер_заказа");
                XAttribute ItmTxtInf3Znachen = new XAttribute("Значен", CurrDataUPD[6]);
                SvedTov.Add(InfPolFHJ23);
                InfPolFHJ23.Add(ItmTxtInf3Identif);
                InfPolFHJ23.Add(ItmTxtInf3Znachen);


                //<Документ><ТаблСчФакт><СведТов><ИнфПолФХЖ2>
                string nomBuyerCd = Verifiacation.GetBuyerItemCodeRcd(Convert.ToString(infoKag[5]), Convert.ToInt64(Item[i, 1]));

                XElement InfPolFHJ21 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf1Identif = new XAttribute("Идентиф", "код_материала");
                XAttribute ItmTxtInf1Znachen = new XAttribute("Значен", nomBuyerCd);
                SvedTov.Add(InfPolFHJ21);
                InfPolFHJ21.Add(ItmTxtInf1Identif);
                InfPolFHJ21.Add(ItmTxtInf1Znachen);

                //<Документ><ТаблСчФакт><СведТов><ИнфПолФХЖ2>
                XElement InfPolFHJ22 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf2Identif = new XAttribute("Идентиф", "штрихкод");
                XAttribute ItmTxtInf2Znachen = new XAttribute("Значен", Item[i, 0]);
                SvedTov.Add(InfPolFHJ22);
                InfPolFHJ22.Add(ItmTxtInf2Identif);
                InfPolFHJ22.Add(ItmTxtInf2Znachen);
            }

            //<Документ><ТаблСчФакт><ВсегоОпл>
            XElement VsegoOpl = new XElement("ВсегоОпл");
            XAttribute VsegoBezNds = new XAttribute("СтТовБезНДСВсего", Math.Round(sumWthNds - sumNds, 2));
            XAttribute VsegoSNDS = new XAttribute("СтТовУчНалВсего", Math.Round(sumWthNds, 2));
            TabSF.Add(VsegoOpl);
            VsegoOpl.Add(VsegoBezNds);
            VsegoOpl.Add(VsegoSNDS);

            //<Документ><ТаблСчФакт><ВсегоОпл><СумНалВсего>
            XElement SumNalVsego = new XElement("СумНалВсего");
            VsegoOpl.Add(SumNalVsego);

            if (Convert.ToString(Item[0, 9]) == "без НДС")
            {
                //<Документ><ТаблСчФакт><ВсегоОпл><СумНалВсего><БезНДС>
                XElement SumNalVsegoSumNal = new XElement("БезНДС", "без НДС");
                SumNalVsego.Add(SumNalVsegoSumNal);
            }
            else
            {
                //<Документ><ТаблСчФакт><ВсегоОпл><СумНалВсего><СумНал>
                XElement SumNalVsegoSumNal = new XElement("СумНал", Math.Round(sumNds, 2));
                SumNalVsego.Add(SumNalVsegoSumNal);
            }

            //<Документ><ТаблСчФакт><ВсегоОпл><НеттоВс>
            XElement NettoVes = new XElement("КолНеттоВс", Math.Round(sumWeight, 3));
            VsegoOpl.Add(NettoVes);

            /************************** 3 уровень. <СвПродПер> ************************/
            XElement ProdPer = new XElement("СвПродПер");
            DOC.Add(ProdPer);

            //<Документ><СвПродПер><СвПер>
            XElement SvPer = new XElement("СвПер");
            XAttribute SodOper = new XAttribute("СодОпер", "Товары переданы");
            ProdPer.Add(SvPer);
            SvPer.Add(SodOper);

            //<Документ><СвПродПер><СвПер><ОснПер>
            XElement OsnPer = new XElement("ОснПер");
            XAttribute NaimOsn = new XAttribute("НаимОсн", "Заказ");
            XAttribute NomOsn = new XAttribute("НомОсн", CurrDataUPD[6]);
            XAttribute DataOsn = new XAttribute("ДатаОсн", Convert.ToDateTime(CurrDataUPD[8]).ToString(@"dd.MM.yyyy"));
            SvPer.Add(OsnPer);
            OsnPer.Add(NaimOsn);
            OsnPer.Add(NomOsn);
            OsnPer.Add(DataOsn);

            //<Документ><СвПродПер><СвПер><СвЛицПер>
            XElement SvLicPer = new XElement("СвЛицПер");
            SvPer.Add(SvLicPer);

            //<Документ><СвПродПер><СвПер><СвЛицПер><РабОргПрод>
            XElement RabOrgProd = new XElement("РабОргПрод");
            XAttribute Doljnost = new XAttribute("Должность", "Водитель");
            SvLicPer.Add(RabOrgProd);
            RabOrgProd.Add(Doljnost);

            //<Документ><СвПродПер><СвПер><СвЛицПер><РабОргПрод><ФИО>
            XElement pFIO = new XElement("ФИО");

            string sFIO = Convert.ToString(CurrDataUPD[9]);
            int len = sFIO.Length;
            string sF;
            string sI;
            string sO;
            int p1 = sFIO.IndexOf(" ");
            int p2 = sFIO.LastIndexOf(" ");
            if (len > 0)
            {
                sF = sFIO.Remove(p1);
                sI = sFIO.Substring(p1, (p2 - p1));
                sO = sFIO.Substring(p2, len - p2);
                if (sF.Length <= 0) sF = "НеУказано";
                if (sI.Length <= 0) sF = "НеУказано";
                if (sO.Length <= 0) sF = "НеУказано";
            }
            else
            {
                sF = "НеУказано";
                sI = "НеУказано";
                sO = "НеУказано";
            }

            XAttribute pFIOF = new XAttribute("Фамилия", sF.Trim());
            XAttribute pFIOI = new XAttribute("Имя", sI.Trim());
            XAttribute pFIOO = new XAttribute("Отчество", sO.Trim());

            RabOrgProd.Add(pFIO);
            pFIO.Add(pFIOF);
            pFIO.Add(pFIOI);
            pFIO.Add(pFIOO);

            /************************** 3 уровень. <Подписант> ************************/
            string[] infoSigner = Verifiacation.GetSignerOpt(); //0 ОБЩИЕ.ЭЦПДОЛЖНОСТЬ, 1 ОБЩИЕ.ЭЦПФАМИЛИЯ, 2 ОБЩИЕ.ЭЦПИМЯ, 3 ОБЩИЕ.ЭЦПОТЧЕСТВО, 4 ОБЩИЕ.ЭЦПДОЛЖНОБЯЗ

            XElement Podp = new XElement("Подписант");
            XAttribute obl = new XAttribute("ОблПолн", "6");
            XAttribute status = new XAttribute("Статус", "1");
            XAttribute osn = new XAttribute("ОснПолн", infoSigner[4]);
            DOC.Add(Podp);
            Podp.Add(obl);
            Podp.Add(status);
            Podp.Add(osn);

            //<Документ><Подписант><ЮЛ>
            XElement UL = new XElement("ЮЛ");
            XAttribute innUl = new XAttribute("ИННЮЛ", infoFirm[1]);
            XAttribute naimOrg = new XAttribute("НаимОрг", infoFirm[0]);
            XAttribute dolj = new XAttribute("Должн", infoSigner[0]);
            Podp.Add(UL);
            UL.Add(innUl);
            UL.Add(naimOrg);
            UL.Add(dolj);

            //<Документ><Подписант><ЮЛ><ФИО>
            XElement FIO = new XElement("ФИО");
            XAttribute famdir = new XAttribute("Фамилия", infoSigner[1]);
            XAttribute namedir = new XAttribute("Имя", infoSigner[2]);
            XAttribute otchesdir = new XAttribute("Отчество", infoSigner[3]);
            UL.Add(FIO);
            FIO.Add(famdir);
            FIO.Add(namedir);
            FIO.Add(otchesdir);


            //------сохранение документа-----------
            fileName = fileName + ".xml";
            try
            {
                xdoc.Save(pathArchiveEDI + fileName);
                try
                {
                    xdoc.Save(pathUPDEDI + fileName);
                    /*//Load the XmlSchemaSet.
                    XmlSchemaSet schemaSet = new XmlSchemaSet();
                    schemaSet.Add("urn:bookstore-schema", "ON_SCHFDOPPR_1_995_01_05_01_02.xsd");

                    //Validate the file using the schema stored in the schema set.
                    //Any elements belonging to the namespace "urn:cd-schema" generate
                    //a warning because there is no schema matching that namespace.
                    Validate(pathUPDEDI + fileName, schemaSet);
                    Console.ReadLine();*/
                    string message = "EDISOFT. УПД " + typeFunc + " " + fileName + " создан в " + pathUPDEDI;
                    Program.WriteLine(message);
                    DispOrders.WriteProtocolEDI("УПД " + typeFunc, fileName, infoKag[0] + " - " + infoKag[1], 0, infoGpl[0] + " - " + infoGpl[1], "УПД " + typeFunc +  " сформирован", DateTime.Now, Convert.ToString(CurrDataUPD[6]), "EDISOFT");
                    if(typeFunc == "ДОП")
                        DispOrders.WriteEDiSentDoc("10", fileName, Convert.ToString(CurrDataUPD[3]), Convert.ToString(infoSf[0]), "123", Convert.ToString(sumWthNds), Convert.ToString(CurrDataUPD[7]),1, CurrDataUPD[11].ToString());
                    else
                        DispOrders.WriteEDiSentDoc("8", fileName, Convert.ToString(CurrDataUPD[3]), Convert.ToString(infoSf[0]), "123", Convert.ToString(sumWthNds), Convert.ToString(CurrDataUPD[7]), 1, CurrDataUPD[11].ToString());

                }
                catch (Exception e)
                {
                    string message_error = "EDISOFT. Не могу создать xml файл УПД " + typeFunc + " в " + pathUPDEDI + ". Нет доступа или диск переполнен.";
                    DispOrders.WriteProtocolEDI("УПД " + typeFunc, fileName, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УПД " + typeFunc + " не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUPD[6]), "EDISOFT");
                    Program.WriteLine(message_error);
                    DispOrders.WriteErrorLog(e.Message);
                }
            }
            catch (Exception e)
            {
                string message_error = "EDISOFT. Не могу создать xml файл УПД " + typeFunc + " в " + pathArchiveEDI + ". Нет доступа или диск переполнен.";
                DispOrders.WriteProtocolEDI("УПД " + typeFunc, fileName, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УПД не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUPD[6]), "EDISOFT");
                Program.WriteLine(message_error);
                DispOrders.WriteErrorLog(e.Message);
                //запись в лог о неудаче
            }
        }

        /*private static void Validate(String filename, XmlSchemaSet schemaSet)
        {
            Console.WriteLine();
            Console.WriteLine("\r\nValidating XML file {0}...", filename.ToString());

            XmlSchema compiledSchema = null;

            foreach (XmlSchema schema in schemaSet.Schemas())
            {
                compiledSchema = schema;
            }

            XmlReaderSettings settings = new XmlReaderSettings();
            settings.Schemas.Add(compiledSchema);
            settings.ValidationEventHandler += new ValidationEventHandler(ValidationCallBack);
            settings.ValidationType = ValidationType.Schema;

            //Create the schema validating reader.
            XmlReader vreader = XmlReader.Create(filename, settings);

            while (vreader.Read()) { }

            //Close the reader.
            vreader.Close();
        }

        private static void ValidationCallBack(object sender, ValidationEventArgs args)
        {
            if (args.Severity == XmlSeverityType.Warning)
                Console.WriteLine("\tWarning: Matching schema not found.  No validation occurred." + args.Message);
            else
                Console.WriteLine("\tValidation error: " + args.Message);

        }*/

        /*************************************************************************************************************************************/
        /********************************************************** Начало УКД X5 ************************************************************/
        /*************************************************************************************************************************************/
        public static void CreateEdiX5UKD(List<object> CurrDataUKD) //список УКД, 0 ProviderOpt, 1 ProviderZkg, 2 NastDoc_Fmt, 3 SklSf_Rcd, 4 SklSf_TpOtg, 5 SklSfA_RcdCor, 6 PrdZkg_NmrExt, 7 PrdZkg_Rcd, 8 PrdZkg_Dt ,9 SklNk_TDrvNm
        {
            //получение путей
            string pathArchiveEDI = /*"D:\\Edi\\Archive\\"; //*/DispOrders.GetValueOption("EDI-СОФТ.АРХИВ");
            string pathUKDEDI;

            //Запрос данных КорректировочнойСФ
            object[] infoSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataUKD[3])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO
            //Запрос данных Корректируемой (отгрузочной) СФ
            object[] infoCorSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataUKD[5])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO
            //Запрос предыдущих Корректировочных СФ корректируемые текущей корректировочной СФ
            string InfoPrevSf = Verifiacation.GetPrevSfToKSF(Convert.ToString(CurrDataUKD[3]), Convert.ToString(CurrDataUKD[5]));

            //запрос данных спецификации
            object[,] Item = Verifiacation.GetItemsFromKSF(Convert.ToString(CurrDataUKD[3]), Convert.ToString(CurrDataUKD[5]), true); //0 BarCode_Code, 1 SklN_Rcd, 2 SklN_Cd, 3 SklN_NmAlt, 4 Кол-во, 5 Цена без НДC, 6 Цена с НДС, 7 Код ЕИ EDI, 8 ОКЕЙ, 9 Ставка, 10 'S', 11 Сумма НДС, 12 Сумма с НДС, 13 шифр ЕИ, 14 Вес

            //Запрос данных покупателя
            object[] infoKag = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(infoSf[2]), Convert.ToInt64(infoSf[3])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование
            //Запрос данных покупателя
            object[] infoGpl = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(infoSf[4]), Convert.ToInt64(infoSf[5])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование

            string codeByBuyer = Verifiacation.GetFldFromEdiExch(Convert.ToInt64(CurrDataUKD[7]), "Exch_USelCdByer"); //Код поставщика для Ашана

            //какой gln номер использовать
            bool useMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(infoSf[4]));
            string ilnFirm;

            object[] infoFirm;
            object[] infoFirmAdr;
            object[] infoFirmGrOt; //грузоотправитель
            object[] infoFirmAdrGrOt;  //адрес грузоотправителя
            if (useMasterGLN == false)//используем данные текущего предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ИЛН");
                pathUKDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/DispOrders.GetValueOption("EDI-СОФТ.УКД");
                infoFirm = Verifiacation.GetFirmInfo(); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO
                infoFirmAdr = Verifiacation.GetFirmAdr(); // 0 CrtAdr_StrNm+','+CrtAdr_House, 1 CrtAdr_TowNm, 2 CrtAdr_RegNm, 3 CrtAdr_Ind, 4 CrtAdr_RegCd
                infoFirmGrOt = infoFirm;
                infoFirmAdrGrOt = infoFirmAdr;

            }
            else//используем данные головного предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");
                infoFirm = Verifiacation.GetMasterFirmInfo();
                infoFirmAdr = Verifiacation.GetMasterFirmAdr();
                infoFirmGrOt = Verifiacation.GetFirmInfo(); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO
                infoFirmAdrGrOt = Verifiacation.GetFirmAdr(); // 0 CrtAdr_StrNm+','+CrtAdr_House, 1 CrtAdr_TowNm, 2 CrtAdr_RegNm, 3 CrtAdr_Ind, 4 CrtAdr_RegCd, 5 CrtAdr_StrNm, 6 CrtAdr_House
                try
                {
                    pathUKDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/DispOrders.GetValueOption("EDI-СОФТ.ЭКСПОРТ");
                }
                catch
                {
                    pathUKDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/DispOrders.GetValueOption("EDI-СОФТ.УКД");
                }
            }



            string idEdo = DispOrders.GetValueOption("EDI-СОФТ.ИДЭДО");  //"2IJ"; //ИдЭДО

            string idOtpr = idEdo + ilnFirm; //ИдОтпр
            string idPol = idEdo + infoGpl[2].ToString(); //ИдПол

            string guid = Convert.ToString(Guid.NewGuid());
            string fileName;
            if (CurrDataUKD[2].ToString().Equals("X5Mark"))
            {
                fileName = "ON_NKORSCHFDOPPRMARK_" + idPol + "_" + idOtpr + "_" + DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid;//ИдФайл
            }
            else
            {
                fileName = "ON_NKORSCHFDOPPR_" + idPol + "_" + idOtpr + "_" + DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid;//ИдФайл
            }


            /************************** 1 уровень. <Файл> ******************************/

            XDocument xdoc = new XDocument(new XDeclaration("1.0", "", ""));

            XElement File = new XElement("Файл");
            XAttribute IdFile = new XAttribute("ИдФайл", "000000000000000000000000000000000000"/*fileName*/);
            XAttribute VersForm = new XAttribute("ВерсФорм", "5.01");
            XAttribute VersProg = new XAttribute("ВерсПрог", "Edisoft");

            xdoc.Add(File);
            File.Add(IdFile);
            File.Add(VersForm);
            File.Add(VersProg);

            /************************** 2 уровень. <СвУчДокОбор> ************************/

            XElement ID = new XElement("СвУчДокОбор");

            XAttribute IdSender = new XAttribute("ИдОтпр", "0000000000"/*idOtpr*/);
            XAttribute IdReciever = new XAttribute("ИдПол", "0000000000"/*idPol*/);

            File.Add(ID);
            ID.Add(IdSender);
            ID.Add(IdReciever);

            //<СвУчДокОбор><СвОЭДОтпр>
            XElement InfOrg = new XElement("СвОЭДОтпр");
            string providerNm = DispOrders.GetValueOption("EDI-СОФТ.НМ");
            string providerInn = DispOrders.GetValueOption("EDI-СОФТ.ИНН");
            XAttribute NaimOrg = new XAttribute("НаимОрг", providerNm/*"Эдисофт, ООО"*/);
            XAttribute INNUL = new XAttribute("ИННЮЛ", providerInn/*"7801471082"*/);
            XAttribute IDEDO = new XAttribute("ИдЭДО", idEdo);

            ID.Add(InfOrg);
            InfOrg.Add(NaimOrg);
            InfOrg.Add(INNUL);
            InfOrg.Add(IDEDO);

            /************************** 2 уровень. <Документ> ************************/

            XElement DOC = new XElement("Документ");
            XAttribute KND = new XAttribute("КНД", "1115133");
            XAttribute Function = new XAttribute("Функция", "КСЧФДИС");
            XAttribute PoFakt = new XAttribute("ПоФактХЖ", "Документ, подтверждающий согласие (факт уведомления) покупателя на изменение стоимости отгруженных товаров (выполненных работ, оказанных услуг), переданных имущественных прав");
            XAttribute NaimDocOpr = new XAttribute("НаимДокОпр", "Документ, подтверждающий согласие (факт уведомления) покупателя на изменение стоимости отгруженных товаров (выполненных работ, оказанных услуг), переданных имущественных прав");
            XAttribute DateF = new XAttribute("ДатаИнфПр", DateTime.Today.ToString(@"dd.MM.yyyy"));
            XAttribute TimeF = new XAttribute("ВремИнфПр", DateTime.Today.ToString(@"hh.mm.ss"));
            XAttribute NameOrg = new XAttribute("НаимЭконСубСост", infoFirm[0].ToString() /*+ ", ИНН-КПП: " + infoFirm[1].ToString() + "-" + infoFirm[2].ToString()*/);

            File.Add(DOC);
            DOC.Add(KND);
            DOC.Add(Function);
            DOC.Add(PoFakt);
            DOC.Add(NaimDocOpr);
            DOC.Add(DateF);
            DOC.Add(TimeF);
            DOC.Add(NameOrg);

            //<Документ><СвКСчФ>
            XElement SVSF = new XElement("СвКСчФ");
            XAttribute NomSF = new XAttribute("НомерКСчФ", infoSf[0].ToString());
            XAttribute DateSF = new XAttribute("ДатаКСчФ", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));
            XAttribute Kod = new XAttribute("КодОКВ", infoSf[6].ToString());

            DOC.Add(SVSF);
            SVSF.Add(NomSF);
            SVSF.Add(DateSF);
            SVSF.Add(Kod);

            //<Документ><СвКСчФ><СчФ>
            XElement SchF = new XElement("СчФ");
            XAttribute NomSchF = new XAttribute("НомерСчФ", infoCorSf[0].ToString());
            XAttribute DateSchF = new XAttribute("ДатаСчФ", Convert.ToDateTime(infoCorSf[1]).ToString(@"dd.MM.yyyy"));
            SVSF.Add(SchF);
            SchF.Add(NomSchF);
            SchF.Add(DateSchF);


            //<Документ><<СвКСчФ>><СвПрод>
            XElement SvProd = new XElement("СвПрод");
            //XAttribute SvProdOKPO = new XAttribute("ОКПО", infoFirm[3].ToString());
            SVSF.Add(SvProd);
            //SvProd.Add(SvProdOKPO);

            //<Документ><<СвКСчФ>><СвПрод><ИдСв>
            XElement SvProdIdSv = new XElement("ИдСв");
            SvProd.Add(SvProdIdSv);

            //<Документ><<СвКСчФ>><СвПрод><ИдСв><СвЮЛУч>
            XElement SvProdSvUluchh = new XElement("СвЮЛУч");
            XAttribute SvProdIdSvName = new XAttribute("НаимОрг", infoFirm[0].ToString());
            XAttribute SvProdIdSvINN = new XAttribute("ИННЮЛ", infoFirm[1].ToString());
            XAttribute SvProdIdSvKPP = new XAttribute("КПП", infoFirm[2].ToString());
            if (useMasterGLN)
                SvProdIdSvKPP = new XAttribute("КПП", infoFirmGrOt[2].ToString());

            SvProdIdSv.Add(SvProdSvUluchh);
            SvProdSvUluchh.Add(SvProdIdSvName);
            SvProdSvUluchh.Add(SvProdIdSvINN);
            SvProdSvUluchh.Add(SvProdIdSvKPP);

            //<Документ><<СвКСчФ>><СвПрод><Адрес>
            XElement SvProdAdres = new XElement("Адрес");
            SvProd.Add(SvProdAdres);

            //<Документ><<СвКСчФ>><СвПрод><Адрес><АдресРФ>
            //Адрес
            XElement SvProdAdrRF = new XElement("АдрРФ");
            SvProdAdres.Add(SvProdAdrRF);
            if (infoFirmAdr[3].ToString() != "")
            {
                XAttribute SvProdIndex = new XAttribute("Индекс", infoFirmAdr[3].ToString());
                SvProdAdrRF.Add(SvProdIndex);
            }
    //        if (infoFirmAdr[4].ToString() != "")
    //        {
                XAttribute SvProdKodReg = new XAttribute("КодРегион", infoFirmAdr[4].ToString());
                SvProdAdrRF.Add(SvProdKodReg);
    //        }

            /*//<Документ><<СвКСчФ>><ГрузОт>
            XElement GruzOt = new XElement("ГрузОт");
            XElement GruzOtOnJe = new XElement("ОнЖе", "он же");
            SVSF.Add(GruzOt);
            GruzOt.Add(GruzOtOnJe);

            //<Документ><<СвКСчФ>><ГрузПолуч>
            XElement GruzPoluch = new XElement("ГрузПолуч");
            SVSF.Add(GruzPoluch);

            //<Документ><<СвКСчФ>><ГрузПолуч><ИдСв>
            XElement GruzPoluchIdSv = new XElement("ИдСв");
            GruzPoluch.Add(GruzPoluchIdSv);

            //<Документ><<СвКСчФ>><ГрузПолуч><ИдСв><СвЮЛУч>
            XElement GruzPoluchSvUluch = new XElement("СвЮЛУч");
            XAttribute GruzPoluchName = new XAttribute("НаимОрг", infoGpl[1]);
            XAttribute GruzPoluchINN = new XAttribute("ИННЮЛ", infoGpl[3]);
            XAttribute GruzPoluchKPP = new XAttribute("КПП", infoGpl[4]);

            GruzPoluchIdSv.Add(GruzPoluchSvUluch);
            GruzPoluchSvUluch.Add(GruzPoluchName);
            GruzPoluchSvUluch.Add(GruzPoluchINN);
            GruzPoluchSvUluch.Add(GruzPoluchKPP);

            //<Документ><<СвКСчФ>><ГрузПолуч><Адрес>
            XElement GruzPoluchAdres = new XElement("Адрес");
            GruzPoluch.Add(GruzPoluchAdres);

            //<Документ><<СвКСчФ>><ГрузПолуч><Адрес><АдресРФ>
            XElement GruzPoluchAdrRF = new XElement("АдрРФ");
            XAttribute GruzPoluchIndex = new XAttribute("Индекс", infoGpl[7]);
            XAttribute GruzPoluchKodReg = new XAttribute("КодРегион", infoGpl[8]);
            GruzPoluchAdres.Add(GruzPoluchAdrRF);
            GruzPoluchAdrRF.Add(GruzPoluchIndex);
            GruzPoluchAdrRF.Add(GruzPoluchKodReg);*/

            //<Документ><<СвКСчФ>><СвПокуп>
            XElement SvPokup = new XElement("СвПокуп");
            SVSF.Add(SvPokup);

            //<Документ><<СвКСчФ>><СвПокуп><ИдСв>
            XElement SvPokupIdSv = new XElement("ИдСв");
            SvPokup.Add(SvPokupIdSv);

            //<Документ><СвСчФакт><СвПокуп><ИдСв><СвЮЛУч>             
            XElement SvPokupSvUluch = new XElement("СвЮЛУч");
            XAttribute SvPokupName = new XAttribute("НаимОрг", "#NAME#"/*infoKag[1]*/);
            XAttribute SvPokupINN = new XAttribute("ИННЮЛ", infoKag[3]);
            XAttribute SvPokupKPP = new XAttribute("КПП", infoKag[4]);
            SvPokupIdSv.Add(SvPokupSvUluch);
            SvPokupSvUluch.Add(SvPokupName);
            SvPokupSvUluch.Add(SvPokupINN);
            SvPokupSvUluch.Add(SvPokupKPP);

            //<Документ><<СвКСчФ>><СвПокуп><Адрес>
            XElement SvPokupAdres = new XElement("Адрес");
            SvPokup.Add(SvPokupAdres);

            //<Документ><<СвКСчФ>><СвПокуп><Адрес><АдресРФ>
            XElement SvPokupAdrRF = new XElement("АдрРФ");
            SvPokupAdres.Add(SvPokupAdrRF);
            if (infoGpl[7].ToString().Trim().Length > 0)
            {
                XAttribute SvPokupIndex = new XAttribute("Индекс", infoKag[7]);
                SvPokupAdrRF.Add(SvPokupIndex);
            }
       //     if (infoGpl[8].ToString().Length > 0) Атрибут обязательный
       //     {
                XAttribute SvPokupKodReg = new XAttribute("КодРегион", infoKag[8]);
                SvPokupAdrRF.Add(SvPokupKodReg);
      //      }
            if (infoGpl[9].ToString().Length > 0)
            {
                XAttribute SvPokupCity = new XAttribute("Город", infoKag[9]);
                SvPokupAdrRF.Add(SvPokupCity);
            }
            if (infoGpl[10].ToString().Length > 0)
            {
                XAttribute SvPokupStreet = new XAttribute("Улица", infoKag[10]);
                SvPokupAdrRF.Add(SvPokupStreet);
            }
            if (infoGpl[11].ToString().Length > 0)
            {
                XAttribute SvPokupHouse = new XAttribute("Дом", infoKag[11]);
                SvPokupAdrRF.Add(SvPokupHouse);
            }


            //<Документ><<СвКСчФ>><ИнфПолФХЖ1>
            XElement DopSvFHJ1 = new XElement("ДопСвФХЖ1");
            XAttribute NaimOKV = new XAttribute("НаимОКВ", "Российский рубль");
            SVSF.Add(DopSvFHJ1);
            DopSvFHJ1.Add(NaimOKV);

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1>
            XElement InfPolFHJ1 = new XElement("ИнфПолФХЖ1");
            SVSF.Add(InfPolFHJ1);

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1><ТекстИнф>
            /*XElement TxtInf1 = new XElement("ТекстИнф");
            XAttribute TxtInf1Identif = new XAttribute("Идентиф", "номер_заказа");
            XAttribute TxtInf1Znachen = new XAttribute("Значен", CurrDataUKD[6]);
            InfPolFHJ1.Add(TxtInf1);
            TxtInf1.Add(TxtInf1Identif);
            TxtInf1.Add(TxtInf1Znachen);*/

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf2 = new XElement("ТекстИнф");
            XAttribute TxtInf2Identif = new XAttribute("Идентиф", "отправитель");
            XAttribute TxtInf2Znachen = new XAttribute("Значен", ilnFirm);
            InfPolFHJ1.Add(TxtInf2);
            TxtInf2.Add(TxtInf2Identif);
            TxtInf2.Add(TxtInf2Znachen);

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf3 = new XElement("ТекстИнф");
            XAttribute TxtInf3Identif = new XAttribute("Идентиф", "получатель");
            XAttribute TxtInf3Znachen = new XAttribute("Значен", infoKag[2]);
            InfPolFHJ1.Add(TxtInf3);
            TxtInf3.Add(TxtInf3Identif);
            TxtInf3.Add(TxtInf3Znachen);

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1><ТекстИнф>
            /*XElement TxtInf4 = new XElement("ТекстИнф");
            XAttribute TxtInf4Identif = new XAttribute("Идентиф", "код_поставщика");
            XAttribute TxtInf4Znachen = new XAttribute("Значен", codeByBuyer);
            InfPolFHJ1.Add(TxtInf4);
            TxtInf4.Add(TxtInf4Identif);
            TxtInf4.Add(TxtInf4Znachen);*/

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf5 = new XElement("ТекстИнф");
            XAttribute TxtInf5Identif = new XAttribute("Идентиф", "грузополучатель");
            XAttribute TxtInf5Znachen = new XAttribute("Значен", infoGpl[2]);
            InfPolFHJ1.Add(TxtInf5);
            TxtInf5.Add(TxtInf5Identif);
            TxtInf5.Add(TxtInf5Znachen);

            /************************** 3 уровень. <ТаблКСчФ> ************************/
            XElement TabSF = new XElement("ТаблКСчФ");
            DOC.Add(TabSF);

            decimal sumW0Nds_V = 0;
            decimal sumNds_V = 0;
            decimal sumWthNds_V = 0;

            decimal sumW0Nds_G = 0;
            decimal sumNds_G = 0;
            decimal sumWthNds_G = 0;

            decimal summWoNds_A = 0;
            decimal summWoNds_B = 0;

            //Item[] 0-Рсд, 1-ШК, 2-Название,  3-A_EiSh,  4-A_EiOkei,  5-A_EiEdi,  6-A_EiAcc,  7-A_QtOsn,  8-A_QtCn,  9-A_CnWoNds, 10-A_Tax, 11-A_SummNds, 12-A_SummWthNds 
            //                              , 13-B_EiSh, 14-B_EiOkei, 15-B_EiEdi, 16-B_EiAcc, 17-B_QtOsn, 18-B_QtCn, 19-B_CnWoNds, 20-B_Tax, 21-B_SummNds, 22-B_SummWthNds
            for (int i = 0; i < Item.GetLongLength(0); i++)
            {
                //<Документ><ТаблКСчФ><СведТов>
                XElement SvedTov = new XElement("СведТов");
                XAttribute NomStr = new XAttribute("НомСтр", Convert.ToString(i + 1));
                XAttribute NomTovVStr = new XAttribute("ПорНомТовВСЧФ", Convert.ToString(i + 1));
                XAttribute NaimTov = new XAttribute("НаимТов", Item[i, 2]);
                XAttribute OKEI_TovDo = new XAttribute("ОКЕИ_ТовДо", Item[i, 4]);
                XAttribute OKEI_TovPosle = new XAttribute("ОКЕИ_ТовПосле", Item[i, 14]);
                XAttribute KolTovDo = new XAttribute("КолТовДо", Math.Round(Convert.ToDecimal(Item[i, 8]), 2));
                XAttribute KolTovPosle = new XAttribute("КолТовПосле", Math.Round(Convert.ToDecimal(Item[i, 18]), 2));
                XAttribute CenaTovDo = new XAttribute("ЦенаТовДо", Item[i, 9]);
                XAttribute CenaTovPosle = new XAttribute("ЦенаТовПосле", Item[i, 19]);
                XAttribute NalStDo = new XAttribute("НалСтДо", Convert.ToString(Item[i, 10]));
                XAttribute NalStPosle = new XAttribute("НалСтПосле", Convert.ToString(Item[i, 10]));
                TabSF.Add(SvedTov);
                SvedTov.Add(NomStr);
                SvedTov.Add(NomTovVStr);
                SvedTov.Add(NaimTov);
                SvedTov.Add(OKEI_TovDo);
                SvedTov.Add(OKEI_TovPosle);
                SvedTov.Add(KolTovDo);
                SvedTov.Add(KolTovPosle);
                SvedTov.Add(CenaTovDo);
                SvedTov.Add(CenaTovPosle);
                SvedTov.Add(NalStDo);
                SvedTov.Add(NalStPosle);

                //<Документ><ТаблКСчФ><СведТов><СтТовБезНДС>
                XElement StTovBezNds = new XElement("СтТовБезНДС");
                summWoNds_A = Math.Round(Convert.ToDecimal(Item[i, 12]) - Convert.ToDecimal(Item[i, 11]), 2);
                summWoNds_B = Math.Round(Convert.ToDecimal(Item[i, 22]) - Convert.ToDecimal(Item[i, 21]), 2);
                XAttribute StoimDoIzm = new XAttribute("СтоимДоИзм", summWoNds_A);
                XAttribute StoimPosleIzm = new XAttribute("СтоимПослеИзм", summWoNds_B);
                XAttribute StoimUvel = new XAttribute("СтоимУвел", Math.Round(summWoNds_B - summWoNds_A, 2));
                XAttribute StoimUm = new XAttribute("СтоимУм", Math.Round(summWoNds_A - summWoNds_B, 2));
                SvedTov.Add(StTovBezNds);
                StTovBezNds.Add(StoimDoIzm);
                StTovBezNds.Add(StoimPosleIzm);
                if (summWoNds_A < summWoNds_B)
                {
                    StTovBezNds.Add(StoimUvel);
                    sumW0Nds_V = sumW0Nds_V + Math.Round(summWoNds_B - summWoNds_A, 2);
                }
                if (summWoNds_A > summWoNds_B)
                {
                    StTovBezNds.Add(StoimUm);
                    sumW0Nds_G = sumW0Nds_G + Math.Round(summWoNds_A - summWoNds_B, 2);
                }

                //<Документ><ТаблКСчФ><СведТов><АкцизДо>
                XElement AkcizDo = new XElement("АкцизДо");
                XElement bezAkcizDo = new XElement("БезАкциз", "без акциза");
                SvedTov.Add(AkcizDo);
                AkcizDo.Add(bezAkcizDo);

                //<Документ><ТаблКСчФ><СведТов><АкцизПосле>
                XElement AkcizPosle = new XElement("АкцизПосле");
                XElement bezAkcizPosle = new XElement("БезАкциз", "без акциза");
                SvedTov.Add(AkcizPosle);
                AkcizPosle.Add(bezAkcizPosle);

                //<Документ><ТаблКСчФ><СведТов><АкцизРазн>
                XElement AkcizRazn = new XElement("АкцизРазн");
                XElement AkcizRaznSumUvel = new XElement("СумУвел", "0.00");
                XElement AkcizRaznSumUm = new XElement("СумУм", "0.00");
                SvedTov.Add(AkcizRazn);
                if (summWoNds_A < summWoNds_B) AkcizRazn.Add(AkcizRaznSumUvel);
                if (summWoNds_A > summWoNds_B) AkcizRazn.Add(AkcizRaznSumUm);

                //<Документ><ТаблКСчФ><СведТов><СумНалДо>
                XElement SumNalDo = new XElement("СумНалДо");
                SvedTov.Add(SumNalDo);
                if (Convert.ToString(Item[i, 10]) == "без НДС")
                {
                    //<Документ><ТаблКСчФ><СведТов><СумНалДо><БезНДС>
                    XElement BezNdsDo = new XElement("БезНДС", "без НДС");
                    SumNalDo.Add(BezNdsDo);
                }
                else
                {
                    //<Документ><ТаблКСчФ><СведТов><СумНалДо><СумНДС>
                    XElement SumNalSumNdsDo = new XElement("СумНДС", Math.Round(Convert.ToDecimal(Item[i, 11]), 2));
                    SumNalDo.Add(SumNalSumNdsDo);
                }

                //<Документ><ТаблКСчФ><СведТов><СумНалПосле>
                XElement SumNalPosle = new XElement("СумНалПосле");
                SvedTov.Add(SumNalPosle);
                if (Convert.ToString(Item[i, 20]) == "без НДС")
                {
                    //<Документ><ТаблКСчФ><СведТов><СумНалПосле><БезНДС>
                    XElement BezNdsPosle = new XElement("БезНДС", "без НДС");
                    SumNalPosle.Add(BezNdsPosle);
                }
                else
                {
                    //<Документ><ТаблКСчФ><СведТов><СумНалПосле><СумНДС>
                    XElement SumNalSumNdsPosle = new XElement("СумНДС", Math.Round(Convert.ToDecimal(Item[i, 21]), 2));
                    SumNalPosle.Add(SumNalSumNdsPosle);
                }

                //<Документ><ТаблКСчФ><СведТов><СумНалРазн>
                XElement SumNalRazn = new XElement("СумНалРазн");
                XElement SumNalRaznSumUvel = new XElement("СумУвел", Math.Round(Convert.ToDecimal(Item[i, 21]) - Convert.ToDecimal(Item[i, 11]), 2));
                XElement SumNalRaznSumUm = new XElement("СумУм", Math.Round(Convert.ToDecimal(Item[i, 11]) - Convert.ToDecimal(Item[i, 21]), 2));
                SvedTov.Add(SumNalRazn);
                if (summWoNds_A < summWoNds_B)
                {
                    SumNalRazn.Add(SumNalRaznSumUvel);
                    sumNds_V = sumNds_V + Math.Round(Convert.ToDecimal(Item[i, 21]) - Convert.ToDecimal(Item[i, 11]), 2);
                }
                if (summWoNds_A > summWoNds_B)
                {
                    SumNalRazn.Add(SumNalRaznSumUm);
                    sumNds_G = sumNds_G + Math.Round(Convert.ToDecimal(Item[i, 11]) - Convert.ToDecimal(Item[i, 21]), 2);
                }


                //<Документ><ТаблКСчФ><СведТов><СтТовУчНал>
                XElement StTovUchNal = new XElement("СтТовУчНал");
                XAttribute StTovUchNalStoimDoIzm = new XAttribute("СтоимДоИзм", Math.Round(Convert.ToDecimal(Item[i, 12]), 2));
                XAttribute StTovUchNalStoimPosleIzm = new XAttribute("СтоимПослеИзм", Math.Round(Convert.ToDecimal(Item[i, 22]), 2));
                XAttribute StTovUchNalStoimUvel = new XAttribute("СтоимУвел", Math.Round(Convert.ToDecimal(Item[i, 22]) - Convert.ToDecimal(Item[i, 12]), 2));
                XAttribute StTovUchNalStoimUm = new XAttribute("СтоимУм", Math.Round(Convert.ToDecimal(Item[i, 12]) - Convert.ToDecimal(Item[i, 22]), 2));
                SvedTov.Add(StTovUchNal);
                StTovUchNal.Add(StTovUchNalStoimDoIzm);
                StTovUchNal.Add(StTovUchNalStoimPosleIzm);
                if (summWoNds_A < summWoNds_B)
                    if (summWoNds_A > summWoNds_B)

                        if (summWoNds_A < summWoNds_B)
                        {
                            StTovUchNal.Add(StTovUchNalStoimUvel);
                            sumWthNds_V = sumWthNds_V + Math.Round(Convert.ToDecimal(Item[i, 22]) - Convert.ToDecimal(Item[i, 12]), 2);
                        }
                if (summWoNds_A > summWoNds_B)
                {
                    StTovUchNal.Add(StTovUchNalStoimUm);
                    sumWthNds_G = sumWthNds_G + Math.Round(Convert.ToDecimal(Item[i, 12]) - Convert.ToDecimal(Item[i, 22]), 2);
                }

                //<Документ><ТаблКСчФ><СведТов><ИнфПолФХЖ2>
                XElement InfPolFHJ23 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf3Identif = new XAttribute("Идентиф", "номер_заказа");
                XAttribute ItmTxtInf3Znachen = new XAttribute("Значен", CurrDataUKD[6]);
                SvedTov.Add(InfPolFHJ23);
                InfPolFHJ23.Add(ItmTxtInf3Identif);
                InfPolFHJ23.Add(ItmTxtInf3Znachen);

                //<Документ><ТаблКСчФ><СведТов><ИнфПолФХЖ2>
                string nomBuyerCd = Verifiacation.GetBuyerItemCodeRcd(Convert.ToString(infoKag[5]), Convert.ToInt64(Item[i, 0]));

                XElement InfPolFHJ21 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf1Identif = new XAttribute("Идентиф", "код_материала");
                XAttribute ItmTxtInf1Znachen = new XAttribute("Значен", nomBuyerCd);
                SvedTov.Add(InfPolFHJ21);
                InfPolFHJ21.Add(ItmTxtInf1Identif);
                InfPolFHJ21.Add(ItmTxtInf1Znachen);

                //<Документ><ТаблКСчФ><СведТов><ИнфПолФХЖ2>
                XElement InfPolFHJ22 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf2Identif = new XAttribute("Идентиф", "штрихкод");
                XAttribute ItmTxtInf2Znachen = new XAttribute("Значен", Item[i, 1]);
                SvedTov.Add(InfPolFHJ22);
                InfPolFHJ22.Add(ItmTxtInf2Identif);
                InfPolFHJ22.Add(ItmTxtInf2Znachen);

                //<Документ><ТаблКСчФ><СведТов><ДопСведТов>
                XElement DopInfo = new XElement("ДопСведТов");
                XAttribute NmEiBefore = new XAttribute("НаимЕдИзмДо", Item[i, 3]);
                XAttribute NmEiAfter = new XAttribute("НаимЕдИзмПосле", Item[i, 13]);
                SvedTov.Add(DopInfo);
                DopInfo.Add(NmEiBefore);
                DopInfo.Add(NmEiAfter);

                //<Документ><ТаблКСчФ><СведТов><ДопСведТов><НомСредИдентТов[До/После]><НомУпак>
                if (Item[i, 10].ToString().Contains("10") && CurrDataUKD[2].ToString().Equals("X5Mark"))     //Item[i, 10] - налоговая ставка после (как бы)
                {
                    string nomUpakValueDo = "020" + Item[i, 0] + "37" + (Math.Round(Convert.ToDecimal(Item[i, 8]))).ToString();             //Item[i, 8] - количество до
                    string nomUpakValuePosle = "020" + Item[i, 0] + "37" + (Math.Round(Convert.ToDecimal(Item[i, 18]))).ToString();         //Item[i, 18] - количество после
                    XElement NomSredIdentDo = new XElement("НомСредИдентТовДо");
                    XElement NomUpakDo = new XElement("НомУпак", nomUpakValueDo);
                    XElement NomSredIdentPosle = new XElement("НомСредИдентТовПосле");
                    XElement NomUpakPosle = new XElement("НомУпак", nomUpakValuePosle);
                    DopInfo.Add(NomSredIdentDo);
                    NomSredIdentDo.Add(NomUpakDo);
                    DopInfo.Add(NomSredIdentPosle);
                    NomSredIdentPosle.Add(NomUpakPosle);
                }
            }

            if (sumWthNds_V > 0)
            {
                //<Документ><ТаблКСчФ><ВсегоУвел>
                XElement VsegoUvel = new XElement("ВсегоУвел");
                XAttribute VsegoUvelBezNds = new XAttribute("СтТовБезНДСВсего", Math.Round(sumW0Nds_V, 2));
                XAttribute VsegoUvelSNDS = new XAttribute("СтТовУчНалВсего", Math.Round(sumWthNds_V, 2));
                TabSF.Add(VsegoUvel);
                VsegoUvel.Add(VsegoUvelBezNds);
                VsegoUvel.Add(VsegoUvelSNDS);

                //<Документ><ТаблКСчФ><ВсегоУвел><СумНал>
                XElement VsegoUvelSumNal = new XElement("СумНал");
                VsegoUvel.Add(VsegoUvelSumNal);
                if (Convert.ToString(Item[0, 10]) == "без НДС")
                {
                    //<Документ><ТаблКСчФ><ВсегоУвел><СумНал><БезНДС>
                    XElement VsegoUvelSumNalBezNds = new XElement("БезНДС", "без НДС");
                    VsegoUvelSumNal.Add(VsegoUvelSumNalBezNds);
                }
                else
                {
                    //<Документ><ТаблКСчФ><ВсегоОпл><СумНал><СумНал>
                    XElement VsegoUvelSumNalSumMal = new XElement("СумНДС", Math.Round(sumNds_V, 2));
                    VsegoUvelSumNal.Add(VsegoUvelSumNalSumMal);
                }
            }

            if (sumWthNds_G > 0)
            {
                //<Документ><ТаблКСчФ><ВсегоУм>
                XElement VsegoUm = new XElement("ВсегоУм");
                XAttribute VsegoUmBezNds = new XAttribute("СтТовБезНДСВсего", Math.Round(sumW0Nds_G, 2));
                XAttribute VsegoUmSNDS = new XAttribute("СтТовУчНалВсего", Math.Round(sumWthNds_G, 2));
                TabSF.Add(VsegoUm);
                VsegoUm.Add(VsegoUmBezNds);
                VsegoUm.Add(VsegoUmSNDS);

                //<Документ><ТаблКСчФ><ВсегоУвел><СумНал>
                XElement VsegoUmSumNal = new XElement("СумНал");
                VsegoUm.Add(VsegoUmSumNal);
                if (Convert.ToString(Item[0, 10]) == "без НДС")
                {
                    //<Документ><ТаблКСчФ><ВсегоУвел><СумНал><БезНДС>
                    XElement VsegoUmSumNalBezNds = new XElement("БезНДС", "без НДС");
                    VsegoUmSumNal.Add(VsegoUmSumNalBezNds);
                }
                else
                {
                    //<Документ><ТаблКСчФ><ВсегоОпл><СумНал><СумНал>
                    XElement VsegoUmSumNalSumMal = new XElement("СумНДС", Math.Round(sumNds_G, 2));
                    VsegoUmSumNal.Add(VsegoUmSumNalSumMal);
                }
            }

            /************************** 3 уровень. <СодФХЖ3> ************************/
            XElement SodFHJ3 = new XElement("СодФХЖ3");
            //string allPeredSf = infoCorSf[0].ToString() + " от " + Convert.ToDateTime(infoCorSf[1]).ToString(@"dd.MM.yyyy");
            //if (InfoPrevSf.Length > 2) allPeredSf = allPeredSf + "," + InfoPrevSf;
            //XAttribute InieSvIzmStoim = new XAttribute("ИныеСвИзмСтоим", "Изменения");
            //XAttribute PeredatDocum = new XAttribute("ПередатДокум", allPeredSf);
            XAttribute SodOper = new XAttribute("СодОпер", "Корректировка");
            XAttribute DataNapr = new XAttribute("ДатаНапр", DateTime.Today.ToString(@"dd.MM.yyyy"));
            DOC.Add(SodFHJ3);
            //SodFHJ3.Add(InieSvIzmStoim);
            //SodFHJ3.Add(PeredatDocum);
            SodFHJ3.Add(SodOper);
            SodFHJ3.Add(DataNapr);
            //<Документ><СодФХЖ3><ПередатДокум>
            XElement PeredDoc = new XElement("ПередатДокум");
            XAttribute PeredDocNmOsn = new XAttribute("НаимОсн", "Универсальный передаточный документ");
            XAttribute PeredDocDataOsn = new XAttribute("ДатаОсн", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));
            XAttribute PeredDocNmrOsn = new XAttribute("НомОсн", infoCorSf[0].ToString());
            SodFHJ3.Add(PeredDoc);
            PeredDoc.Add(PeredDocNmOsn);
            PeredDoc.Add(PeredDocDataOsn);
            PeredDoc.Add(PeredDocNmrOsn);
            
            //<Документ><СодФХЖ3><ОснКор>
            XElement PeredDocOsnKorr = new XElement("ДокумОснКор");
            XAttribute NaimOsn = new XAttribute("НаимОсн", "Иные");
            XAttribute DataOsn = new XAttribute("ДатаОсн", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));
            XAttribute DopSvedOsn = new XAttribute("ДопСвОсн", "Отсутствуют");
            SodFHJ3.Add(PeredDocOsnKorr);
            PeredDocOsnKorr.Add(NaimOsn);
            PeredDocOsnKorr.Add(DataOsn);
            PeredDocOsnKorr.Add(DopSvedOsn);

            /************************** 3 уровень. <Подписант> ************************/
            string[] infoSigner = Verifiacation.GetSignerOpt(); //0 ОБЩИЕ.ЭЦПДОЛЖНОСТЬ, 1 ОБЩИЕ.ЭЦПФАМИЛИЯ, 2 ОБЩИЕ.ЭЦПИМЯ, 3 ОБЩИЕ.ЭЦПОТЧЕСТВО, 4 ОБЩИЕ.ЭЦПДОЛЖНОБЯЗ

            XElement Podp = new XElement("Подписант");
            XAttribute obl = new XAttribute("ОблПолн", "6");
            XAttribute status = new XAttribute("Статус", "1");
            XAttribute osn = new XAttribute("ОснПолн", infoSigner[4]);
            DOC.Add(Podp);
            Podp.Add(obl);
            Podp.Add(status);
            Podp.Add(osn);

            //<Документ><Подписант><ЮЛ>
            XElement UL = new XElement("ЮЛ");
            XAttribute innUl = new XAttribute("ИННЮЛ", infoFirm[1]);
            XAttribute naimOrg = new XAttribute("НаимОрг", infoFirm[0]);
            XAttribute dolj = new XAttribute("Должн", infoSigner[0]);
            Podp.Add(UL);
            UL.Add(innUl);
            UL.Add(naimOrg);
            UL.Add(dolj);

            //<Документ><Подписант><ЮЛ><ФИО>
            XElement FIO = new XElement("ФИО");
            XAttribute famdir = new XAttribute("Фамилия", infoSigner[1]);
            XAttribute namedir = new XAttribute("Имя", infoSigner[2]);
            XAttribute otchesdir = new XAttribute("Отчество", infoSigner[3]);
            UL.Add(FIO);
            FIO.Add(famdir);
            FIO.Add(namedir);
            FIO.Add(otchesdir);



            //------сохранение документа-----------
            fileName = fileName + ".xml";
            try
            {
                xdoc.Save(pathArchiveEDI + fileName);
                try
                {
                    xdoc.Save(pathUKDEDI + fileName);
                    string message = "EDISOFT. УКД " + fileName + " создан в " + pathUKDEDI;
                    Program.WriteLine(message);
                    DispOrders.WriteProtocolEDI("УКД", fileName, infoKag[0] + " - " + infoKag[1], 0, infoGpl[0] + " - " + infoGpl[1], "УКД сформирован", DateTime.Now, Convert.ToString(CurrDataUKD[6]), "EDISOFT");
                    DispOrders.WriteEDiSentDoc("8", fileName, Convert.ToString(CurrDataUKD[3]), Convert.ToString(infoSf[0]), "123", Convert.ToString(sumWthNds_V - sumWthNds_G), Convert.ToString(CurrDataUKD[7]),1);
                    //запись в лог о удаче
                }
                catch (Exception e)
                {
                    string message_error = "EDISOFT. Не могу создать xml файл УКД в " + pathUKDEDI + ". Нет доступа или диск переполнен.";
                    DispOrders.WriteProtocolEDI("УКД", fileName, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УКД не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUKD[6]), "EDISOFT");
                    Program.WriteLine(message_error);
                    //DispOrders.WriteErrorLog(e.Message);
                }
            }
            catch (Exception e)
            {
                string message_error = "EDISOFT. Не могу создать xml файл УКД в " + pathArchiveEDI + ". Нет доступа или диск переполнен.";
                DispOrders.WriteProtocolEDI("УКД", fileName, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УКД не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUKD[6]), "EDISOFT");
                Program.WriteLine(message_error);
                DispOrders.WriteErrorLog(e.Message);
                //запись в лог о неудаче
            }
        }
        /********************************************************** Конец УКД X5 **********************************************************/

        /*************************************************************************************************************************************/
        /********************************************************** Начало УПД Лента (Новый формат )************************************************************/
        /*************************************************************************************************************************************/
        public static void CreateEdiLenta_UPD(List<object> CurrDataUPD, string typeFunc) //список УПД, 0 ProviderOpt, 1 ProviderZkg, 2 NastDoc_Fmt, 3 SklSf_Rcd, 4 SklSf_TpOtg, 5 SklSfA_RcdCor, 6 PrdZkg_NmrExt, 7 PrdZkg_Rcd, 8 PrdZkg_Dt ,9 SklNk_TDrvNm
        {
            //получение путей
            string pathArchiveEDI = /*"D:\\Edi\\Archive\\"; //*/ DispOrders.GetValueOption("EDI-СОФТ.АРХИВ");
            string pathUPDEDI;

            //Запрос данных СФ
            object[] infoSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataUPD[3])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO

            //Запрос данных накладной по рсд заказа
            object[] infoNk = Verifiacation.GetNkDataFromZkg(Convert.ToInt64(CurrDataUPD[7])); //0 SklNk_Nmr, 1 SklNk_Dat

            //запрос данных спецификации
            object[,] Item = Verifiacation.GetItemsFromSF(Convert.ToString(CurrDataUPD[3]), true); //0 BarCode_Code, 1 SklN_Rcd, 2 SklN_Cd, 3 SklN_Nm, 4 Кол-во, 5 Цена без НДC, 6 Цена с НДС, 7 Код ЕИ EDI, 8 ОКЕЙ, 9 Ставка, 10 'S', 11 Сумма НДС, 12 Сумма с НДС, 13 шифр ЕИ, 14 Вес

            //Запрос данных покупателя
            object[] infoKag = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(infoSf[2]), Convert.ToInt64(infoSf[3])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование
            //Запрос данных покупателя
            object[] infoGpl = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(infoSf[4]), Convert.ToInt64(infoSf[5])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование

            //string codeByBuyer = Verifiacation.GetFldFromEdiExch(Convert.ToInt64(CurrDataUPD[7]), "Exch_USelCdByer"); //Код поставщика для Ашана

            //какой gln номер использовать
            bool useMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(infoSf[4]));
            string ilnFirm;

            object[] infoFirm;
            object[] infoFirmAdr;
            object[] infoFirmGrOt;
            object[] infoFirmAdrGrOt;
            if (useMasterGLN == false)//используем данные текущего предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ИЛН");
                pathUPDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/ DispOrders.GetValueOption("EDI-СОФТ.УПД");
                infoFirm = Verifiacation.GetFirmInfo(); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO
                infoFirmAdr = Verifiacation.GetFirmAdr(); // 0 CrtAdr_StrNm+','+CrtAdr_House, 1 CrtAdr_TowNm, 2 CrtAdr_RegNm, 3 CrtAdr_Ind, 4 CrtAdr_RegCd
                infoFirmGrOt = infoFirm;
                infoFirmAdrGrOt = infoFirmAdr;

            }
            else//используем данные головного предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");
                infoFirm = Verifiacation.GetMasterFirmInfo();
                infoFirmAdr = Verifiacation.GetMasterFirmAdr();
                infoFirmGrOt = Verifiacation.GetFirmInfo(); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO
                infoFirmAdrGrOt = Verifiacation.GetFirmAdr(); // 0 CrtAdr_StrNm+','+CrtAdr_House, 1 CrtAdr_TowNm, 2 CrtAdr_RegNm, 3 CrtAdr_Ind, 4 CrtAdr_RegCd, 5 CrtAdr_StrNm, 6 CrtAdr_House 
                try
                {
                    pathUPDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/ DispOrders.GetValueOption("EDI-СОФТ.ЭКСПОРТ");
                }
                catch
                {
                    pathUPDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/ DispOrders.GetValueOption("EDI-СОФТ.УПД");
                }
            }

            string idEdo = DispOrders.GetValueOption("EDI-СОФТ.ИДЭДО");  //"2IJ"; //ИдЭДО

            string idOtpr = idEdo + ilnFirm; //ИдОтпр
            string idPol = idEdo + infoGpl[2].ToString(); //ИдПол

            string guid = Convert.ToString(Guid.NewGuid());
            string fileName;
            if (CurrDataUPD[2].ToString().Equals("LentaMark"))
            {
                fileName = "ON_NSCHFDOPPRMARK_" + idPol + "_" + idOtpr + "_" + DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid;//ИдФайл
            }
            else
            {
                fileName = "ON_NSCHFDOPPR_" + idPol + "_" + idOtpr + "_" + DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid;//ИдФайл
            }
            

            /************************** 1 уровень. <Файл> ******************************/

            XDocument xdoc = new XDocument(new XDeclaration("1.0", "", ""));

            XElement File = new XElement("Файл");
            XAttribute IdFile = new XAttribute("ИдФайл", "000000000000000000000000000000000000"/*fileName*/);
            XAttribute VersForm = new XAttribute("ВерсФорм", "5.01");
            XAttribute VersProg = new XAttribute("ВерсПрог", "Destiny");

            xdoc.Add(File);
            File.Add(IdFile);
            File.Add(VersForm);
            File.Add(VersProg);

            /************************** 2 уровень. <СвУчДокОбор> Сведения об участниках электронного документооборота   ************************/

            XElement ID = new XElement("СвУчДокОбор");

            XAttribute IdSender = new XAttribute("ИдОтпр", "0000000000"/*idOtpr*/);
            XAttribute IdReciever = new XAttribute("ИдПол", "0000000000"/*idPol*/);

            File.Add(ID);
            ID.Add(IdSender);
            ID.Add(IdReciever);

            //<СвУчДокОбор><СвОЭДОтпр>
            XElement InfOrg = new XElement("СвОЭДОтпр");
            string providerNm = DispOrders.GetValueOption("EDI-СОФТ.НМ");
            string providerInn = DispOrders.GetValueOption("EDI-СОФТ.ИНН");
            XAttribute NaimOrg = new XAttribute("НаимОрг", providerNm/*"Эдисофт, ООО"*/);
            XAttribute INNUL = new XAttribute("ИННЮЛ", providerInn/*"7801471082"*/);
            XAttribute IDEDO = new XAttribute("ИдЭДО", idEdo);

            ID.Add(InfOrg);
            InfOrg.Add(NaimOrg);
            InfOrg.Add(INNUL);
            InfOrg.Add(IDEDO);

            /************************** 2 уровень. <Документ> ************************/

            XElement DOC = new XElement("Документ");
            XAttribute KND = new XAttribute("КНД", "1115131");
            XAttribute Function = new XAttribute("Функция", typeFunc);
            XAttribute PoFakt = new XAttribute("ПоФактХЖ", "Документ об отгрузке товаров (выполнении работ), передаче имущественных прав (документ об оказании услуг)");
            XAttribute NaimDocOpr;
            if (typeFunc == "СЧФДОП")
                NaimDocOpr = new XAttribute("НаимДокОпр", "Счет-фактура и документ об отгрузке товаров (выполнении работ), передаче имущественных прав (документ об оказании услуг)");
            else if (typeFunc == "ДОП")
                NaimDocOpr = new XAttribute("НаимДокОпр", "Документ об отгрузке товаров (выполнении работ), передаче имущественных прав (документ об оказании услуг)");
            else
                NaimDocOpr = new XAttribute("НаимДокОпр", "");
            XAttribute DateF = new XAttribute("ДатаИнфПр", DateTime.Today.ToString(@"dd.MM.yyyy"));
            XAttribute TimeF = new XAttribute("ВремИнфПр", DateTime.Today.ToString(@"hh.mm.ss"));
            XAttribute NameOrg = new XAttribute("НаимЭконСубСост", infoFirm[0].ToString() + ", ИНН-КПП: " + infoFirm[1].ToString() + "-" + infoFirm[2].ToString());

            File.Add(DOC);
            DOC.Add(KND);
            DOC.Add(Function);
            DOC.Add(PoFakt);
            DOC.Add(NaimDocOpr);
            DOC.Add(DateF);
            DOC.Add(TimeF);
            DOC.Add(NameOrg);

            //<Документ><СвСчФакт>
            XElement SVSF = new XElement("СвСчФакт");
            XAttribute NomSF = new XAttribute("НомерСчФ", infoSf[0].ToString());
            XAttribute DateSF = new XAttribute("ДатаСчФ", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));
            XAttribute Kod = new XAttribute("КодОКВ", infoSf[6].ToString());

            DOC.Add(SVSF);
            SVSF.Add(NomSF);
            SVSF.Add(DateSF);
            SVSF.Add(Kod);

            //<Документ><СвСчФакт><ИспрСчФ>
            //исправление **********************************
            XElement IsprSF = new XElement("ИспрСчФ");
            SVSF.Add(IsprSF);
            //Проверяем необходимо ли отправить исправление, либо же это обычная отправка
            if (CurrDataUPD[11].ToString() == "0") //Обычная отправка
            {

                //если нет исправления
                XAttribute DfNISF = new XAttribute("ДефНомИспрСчФ", "-");
                XAttribute DfDISF = new XAttribute("ДефДатаИспрСчФ", "-");
                IsprSF.Add(DfNISF);
                IsprSF.Add(DfDISF);
            }
            else //нет исправления
            {
                XAttribute NmIsprSF = new XAttribute("НомИспрСчФ", CurrDataUPD[11].ToString());
                XAttribute DtIsprSF = new XAttribute("ДатаИспрСчФ", DateTime.Today.ToString(@"dd.MM.yyyy"));
                IsprSF.Add(NmIsprSF);
                IsprSF.Add(DtIsprSF);
            }
            //конец исправления **********************************

            //<Документ><СвСчФакт><СвПрод>            
            XElement SvProd = new XElement("СвПрод");
            //XAttribute SvProdOKPO = new XAttribute("ОКПО", infoFirm[3].ToString());
            SVSF.Add(SvProd);
            //SvProd.Add(SvProdOKPO);

            //<Документ><СвСчФакт><СвПрод><ИдСв>
            XElement SvProdIdSv = new XElement("ИдСв");
            SvProd.Add(SvProdIdSv);

            //<Документ><СвСчФакт><СвПрод><ИдСв><СвЮЛУч>
            XElement SvProdSvUluchh = new XElement("СвЮЛУч");
            XAttribute SvProdIdSvName = new XAttribute("НаимОрг", infoFirm[0].ToString());
            XAttribute SvProdIdSvINN = new XAttribute("ИННЮЛ", infoFirm[1].ToString());
            XAttribute SvProdIdSvKPP = new XAttribute("КПП", infoFirm[2].ToString());
            if (useMasterGLN)  //используем данные головного предприятия
            {
                SvProdIdSvKPP = new XAttribute("КПП", infoFirmGrOt[2].ToString());
            }

            SvProdIdSv.Add(SvProdSvUluchh);
            SvProdSvUluchh.Add(SvProdIdSvName);
            SvProdSvUluchh.Add(SvProdIdSvINN);
            SvProdSvUluchh.Add(SvProdIdSvKPP);


            //<Документ><СвСчФакт><СвПрод><Адрес>
            XElement SvProdAdres = new XElement("Адрес");
            SvProd.Add(SvProdAdres);
            //<Документ><СвСчФакт><СвПрод><Адрес>
            XElement SvProdAdrRF = new XElement("АдрРФ");
            SvProdAdres.Add(SvProdAdrRF);
            if (infoFirmAdr[3].ToString() != "")
            {
                XAttribute SvProdIndex = new XAttribute("Индекс", infoFirmAdr[3].ToString());
                SvProdAdrRF.Add(SvProdIndex);
            }
            //   if (infoFirmAdr[4].ToString() != "")
            //    {
            XAttribute SvProdKodReg = new XAttribute("КодРегион", infoFirmAdr[4].ToString());
            SvProdAdrRF.Add(SvProdKodReg);
            //            }

            //<Документ><СвСчФакт><ГрузОт>
            XElement GruzOt = new XElement("ГрузОт");
            if (useMasterGLN)
            {
                XElement GruzOtOther = new XElement("ГрузОтпр");
                SVSF.Add(GruzOt);
                GruzOt.Add(GruzOtOther);

                XElement SvGrOtpIdSv = new XElement("ИдСв");
                GruzOtOther.Add(SvGrOtpIdSv);

                //<Документ><СвСчФакт><СвПрод><ИдСв><СвЮЛУч>
                XElement SvGrOtpSvUluchh = new XElement("СвЮЛУч");
                XAttribute SvGrOtpIdSvName = new XAttribute("НаимОрг", infoFirmGrOt[0].ToString());
                XAttribute SvGrOtpIdSvINN = new XAttribute("ИННЮЛ", infoFirmGrOt[1].ToString());
                XAttribute SvGrOtpIdSvKPP = new XAttribute("КПП", infoFirmGrOt[2].ToString());

                SvGrOtpIdSv.Add(SvGrOtpSvUluchh);
                SvGrOtpSvUluchh.Add(SvGrOtpIdSvName);
                SvGrOtpSvUluchh.Add(SvGrOtpIdSvINN);
                SvGrOtpSvUluchh.Add(SvGrOtpIdSvKPP);

                //<Документ><СвСчФакт><СвПрод><Адрес>
                XElement SvGrOtpAdres = new XElement("Адрес");
                GruzOtOther.Add(SvGrOtpAdres);

                XElement SvGrOtpAdrRF = new XElement("АдрРФ");
                SvGrOtpAdres.Add(SvGrOtpAdrRF);
                if (infoFirmAdrGrOt[3].ToString() != "")
                {
                    XAttribute SvGrOtpIndex = new XAttribute("Индекс", infoFirmAdrGrOt[3].ToString());
                    SvGrOtpAdrRF.Add(SvGrOtpIndex);
                }
                XAttribute SvGrOtpKodReg = new XAttribute("КодРегион", infoFirmAdrGrOt[4].ToString());
                SvGrOtpAdrRF.Add(SvGrOtpKodReg);
                if (infoFirmAdrGrOt[1].ToString() != "")
                {
                    XAttribute SvGrOtpGorod = new XAttribute("Город", infoFirmAdrGrOt[1].ToString());
                    SvGrOtpAdrRF.Add(SvGrOtpGorod);
                }
                if (infoFirmAdrGrOt[5].ToString() != "")
                {
                    XAttribute SvGrOtpStreet = new XAttribute("Улица", infoFirmAdrGrOt[5].ToString());
                    SvGrOtpAdrRF.Add(SvGrOtpStreet);
                }
                if (infoFirmAdrGrOt[6].ToString() != "")
                {
                    XAttribute SvGrOtpHouse = new XAttribute("Дом", infoFirmAdrGrOt[6].ToString());
                    SvGrOtpAdrRF.Add(SvGrOtpHouse);
                }

            }
            else
            {
                XElement GruzOtOnJe = new XElement("ОнЖе", "он же");
                SVSF.Add(GruzOt);
                GruzOt.Add(GruzOtOnJe);
            }

            //<Документ><СвСчФакт><ГрузПолуч>
            XElement GruzPoluch = new XElement("ГрузПолуч");
            SVSF.Add(GruzPoluch);

            //<Документ><СвСчФакт><ГрузПолуч><ИдСв>
            XElement GruzPoluchIdSv = new XElement("ИдСв");
            GruzPoluch.Add(GruzPoluchIdSv);

            //<Документ><СвСчФакт><ГрузПолуч><ИдСв><СвЮЛУч>
            XElement GruzPoluchSvUluch = new XElement("СвЮЛУч");
            XAttribute GruzPoluchName = new XAttribute("НаимОрг", "#NAME#"/*infoGpl[1]*/);
            XAttribute GruzPoluchINN = new XAttribute("ИННЮЛ", infoGpl[3]);
            XAttribute GruzPoluchKPP = new XAttribute("КПП", infoGpl[4]);

            GruzPoluchIdSv.Add(GruzPoluchSvUluch);
            GruzPoluchSvUluch.Add(GruzPoluchName);
            GruzPoluchSvUluch.Add(GruzPoluchINN);
            GruzPoluchSvUluch.Add(GruzPoluchKPP);

            //<Документ><СвСчФакт><ГрузПолуч><Адрес>
            XElement GruzPoluchAdres = new XElement("Адрес");
            GruzPoluch.Add(GruzPoluchAdres);

            //<Документ><СвСчФакт><ГрузПолуч><Адрес><АдресРФ>
            XElement GruzPoluchAdrRF = new XElement("АдрРФ");
            GruzPoluchAdres.Add(GruzPoluchAdrRF);
            if (infoGpl[7].ToString().Length > 0)
            {
                XAttribute GruzPoluchIndex = new XAttribute("Индекс", infoGpl[7]);
                GruzPoluchAdrRF.Add(GruzPoluchIndex);
            }
            if (infoGpl[8].ToString().Length > 0)
            {
                XAttribute GruzPoluchKodReg = new XAttribute("КодРегион", infoGpl[8]);
                GruzPoluchAdrRF.Add(GruzPoluchKodReg);
            }
            if (infoGpl[9].ToString().Length > 0)
            {
                XAttribute GruzPoluchCity = new XAttribute("Город", infoGpl[9]);
                GruzPoluchAdrRF.Add(GruzPoluchCity);
            }
            if (infoGpl[10].ToString().Length > 0)
            {
                XAttribute GruzPoluchStreet = new XAttribute("Улица", infoGpl[10]);
                GruzPoluchAdrRF.Add(GruzPoluchStreet);
            }
            if (infoGpl[11].ToString().Length > 0)
            {
                XAttribute GruzPoluchHouse = new XAttribute("Дом", infoGpl[11]);
                GruzPoluchAdrRF.Add(GruzPoluchHouse);
            }

            //<Документ><СвСчФакт><СвПокуп>
            XElement SvPokup = new XElement("СвПокуп");
            SVSF.Add(SvPokup);

            //<Документ><СвСчФакт><СвПокуп><ИдСв>
            XElement SvPokupIdSv = new XElement("ИдСв");
            SvPokup.Add(SvPokupIdSv);

            //<Документ><СвСчФакт><СвПокуп><ИдСв><СвЮЛУч>             
            XElement SvPokupSvUluch = new XElement("СвЮЛУч");
            XAttribute SvPokupName = new XAttribute("НаимОрг", "#NAME#"/*infoKag[1]*/);
            XAttribute SvPokupINN = new XAttribute("ИННЮЛ", infoKag[3]);
            XAttribute SvPokupKPP = new XAttribute("КПП", infoKag[4]);
            SvPokupIdSv.Add(SvPokupSvUluch);
            SvPokupSvUluch.Add(SvPokupName);
            SvPokupSvUluch.Add(SvPokupINN);
            SvPokupSvUluch.Add(SvPokupKPP);

            //<Документ><СвСчФакт><СвПокуп><Адрес>
            XElement SvPokupAdres = new XElement("Адрес");
            SvPokup.Add(SvPokupAdres);

            //<Документ><СвСчФакт><СвПокуп><Адрес><АдресРФ>
            XElement SvPokupAdrRF = new XElement("АдрРФ");
            SvPokupAdres.Add(SvPokupAdrRF);
            if (infoKag[7].ToString().Length > 0)
            {
                XAttribute SvPokupIndex = new XAttribute("Индекс", infoKag[7]);
                SvPokupAdrRF.Add(SvPokupIndex);
            }
            if (infoKag[8].ToString().Length > 0)
            {
                XAttribute SvPokupKodReg = new XAttribute("КодРегион", infoKag[8]);
                SvPokupAdrRF.Add(SvPokupKodReg);
            }
            if (infoKag[9].ToString().Length > 0)
            {
                XAttribute SvPokupCity = new XAttribute("Город", infoKag[9]);
                SvPokupAdrRF.Add(SvPokupCity);
            }
            if (infoKag[10].ToString().Length > 0)
            {
                XAttribute SvPokupStreet = new XAttribute("Улица", infoKag[10]);
                SvPokupAdrRF.Add(SvPokupStreet);
            }
            if (infoKag[11].ToString().Length > 0)
            {
                XAttribute SvPokupHouse = new XAttribute("Дом", infoKag[11]);
                SvPokupAdrRF.Add(SvPokupHouse);
            }
            //убрано по просьбе Ленты 18102020
            //<Документ><СвСчФакт><ИнфПолФХЖ1>
            //XElement DopSvFHJ1 = new XElement("ДопСвФХЖ1");
            //XAttribute NaimOKV = new XAttribute("НаимОКВ", "Российский рубль");
            //XAttribute ObstFormSchf = new XAttribute("ОбстФормСЧФ", "1");
           // SVSF.Add(DopSvFHJ1);
            //DopSvFHJ1.Add(NaimOKV);
            //DopSvFHJ1.Add(ObstFormSchf);

            //<Документ><СвСчФакт><ДокПодтвОтгр>
           /* XElement DocPodtvOtgr = new XElement("ДокПодтвОтгр");
            SVSF.Add(DocPodtvOtgr);
            XAttribute NaimDocOtgr = new XAttribute("НаимДокОтгр", "Товарная накладная");
            DocPodtvOtgr.Add(NaimDocOtgr);
            XAttribute NomDocOtgr = new XAttribute("НомДокОтгр", Convert.ToString(infoNk[0]));
            DocPodtvOtgr.Add(NomDocOtgr);
            XAttribute DateDocOtgr = new XAttribute("ДатаДокОтгр", Convert.ToDateTime(infoNk[1]).ToString(@"dd.MM.yyyy"));
            DocPodtvOtgr.Add(DateDocOtgr);
            */
            //<Документ><СвСчФакт><ИнфПолФХЖ1>
            XElement InfPolFHJ1 = new XElement("ИнфПолФХЖ1");
            SVSF.Add(InfPolFHJ1);

            /*//<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf1 = new XElement("ТекстИнф");
            XAttribute TxtInf1Identif = new XAttribute("Идентиф", "номер_заказа");
            XAttribute TxtInf1Znachen = new XAttribute("Значен", CurrDataUPD[6]);
            InfPolFHJ1.Add(TxtInf1);
            TxtInf1.Add(TxtInf1Identif);
            TxtInf1.Add(TxtInf1Znachen);*/

            //<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf2 = new XElement("ТекстИнф");
            XAttribute TxtInf2Identif = new XAttribute("Идентиф", "отправитель");
            XAttribute TxtInf2Znachen = new XAttribute("Значен", ilnFirm);
            InfPolFHJ1.Add(TxtInf2);
            TxtInf2.Add(TxtInf2Identif);
            TxtInf2.Add(TxtInf2Znachen);

            /*//<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf3 = new XElement("ТекстИнф");
            XAttribute TxtInf3Identif = new XAttribute("Идентиф", "получатель");
            XAttribute TxtInf3Znachen = new XAttribute("Значен", infoKag[2]);
            InfPolFHJ1.Add(TxtInf3);
            TxtInf3.Add(TxtInf3Identif);
            TxtInf3.Add(TxtInf3Znachen); */

            /*//<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf4 = new XElement("ТекстИнф");
            XAttribute TxtInf4Identif = new XAttribute("Идентиф", "код_поставщика");
            XAttribute TxtInf4Znachen = new XAttribute("Значен", codeByBuyer);
            InfPolFHJ1.Add(TxtInf4);
            TxtInf4.Add(TxtInf4Identif);
            TxtInf4.Add(TxtInf4Znachen);*/

            //<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf5 = new XElement("ТекстИнф");
            XAttribute TxtInf5Identif = new XAttribute("Идентиф", "грузополучатель");
            XAttribute TxtInf5Znachen = new XAttribute("Значен", infoGpl[2]);
            InfPolFHJ1.Add(TxtInf5);
            TxtInf5.Add(TxtInf5Identif);
            TxtInf5.Add(TxtInf5Znachen);

            /************************** 3 уровень. <ТаблСчФакт> ************************/
            XElement TabSF = new XElement("ТаблСчФакт");
            DOC.Add(TabSF);

            decimal sumWthNds = 0;
            decimal sumNds = 0;
            decimal sumWeight = 0;

            for (int i = 0; i < Item.GetLongLength(0); i++) //Item[] //0 BarCode_Code, 1 SklN_Rcd, 2 SklN_Cd, 3 SklN_Nm, 4 Кол-во, 5 Цена без НДC, 6 Цена с НДС, 7 Код ЕИ EDI, 8 ОКЕЙ, 9 Ставка, 10 'S', 11 Сумма НДС, 12 Сумма с НДС, 13 шифр ЕИ, 14 Вес
            {
                sumWthNds = sumWthNds + Convert.ToDecimal(Item[i, 12]);
                sumNds = sumNds + Convert.ToDecimal(Item[i, 11]);
                sumWeight = sumWeight + Convert.ToDecimal(Item[i, 14]);
                //<Документ><ТаблСчФакт><СведТов>
                XElement SvedTov = new XElement("СведТов");
                XAttribute NomStr = new XAttribute("НомСтр", Convert.ToString(i + 1));
                XAttribute NaimTov = new XAttribute("НаимТов", Item[i, 3]);
                XAttribute OKEI = new XAttribute("ОКЕИ_Тов", Item[i, 8]);
                XAttribute KolTov = new XAttribute("КолТов", Math.Round(Convert.ToDecimal(Item[i, 4]), 2));
                XAttribute CenaTov = new XAttribute("ЦенаТов", Item[i, 5]);
                XAttribute BezNDS = new XAttribute("СтТовБезНДС", Math.Round(Convert.ToDecimal(Item[i, 12]) - Convert.ToDecimal(Item[i, 11]), 2));
                XAttribute NalSt = new XAttribute("НалСт", Convert.ToString(Item[i, 9]));
                XAttribute sNDS = new XAttribute("СтТовУчНал", Math.Round(Convert.ToDecimal(Item[i, 12]), 2));
                TabSF.Add(SvedTov);
                SvedTov.Add(NomStr);
                SvedTov.Add(NaimTov);
                SvedTov.Add(OKEI);
                SvedTov.Add(KolTov);
                SvedTov.Add(CenaTov);
                SvedTov.Add(BezNDS);
                SvedTov.Add(NalSt);
                SvedTov.Add(sNDS);

                //<Документ><ТаблСчФакт><СведТов><Акциз>
                XElement Akciz = new XElement("Акциз");
                XElement bezAkciz = new XElement("БезАкциз", "без акциза");
                SvedTov.Add(Akciz);
                Akciz.Add(bezAkciz);

                //<Документ><ТаблСчФакт><СведТов><СумНал>
                XElement SumNal = new XElement("СумНал");
                SvedTov.Add(SumNal);
                if (Convert.ToString(Item[i, 9]) == "без НДС")
                {
                    //<Документ><ТаблСчФакт><СведТов><СумНал><БезНДС>
                    XElement BezNds = new XElement("БезНДС", "без НДС");
                    SumNal.Add(BezNds);
                }
                else
                {
                    //<Документ><ТаблСчФакт><СведТов><СумНал><СумНал>
                    XElement SumNalSum = new XElement("СумНал", Math.Round(Convert.ToDecimal(Item[i, 11]), 2));
                    SumNal.Add(SumNalSum);
                }

                //<Документ><ТаблСчФакт><СведТов><СвТД>
                XElement SvTd = new XElement("СвТД");
                XAttribute DefKodProish = new XAttribute("ДефКодПроисх", "-");
                SvedTov.Add(SvTd);
                SvTd.Add(DefKodProish);

                //<Документ><ТаблСчФакт><СведТов><ДопСведТов>
                XElement DopSvedTov = new XElement("ДопСведТов");
                XAttribute PrTovRav = new XAttribute("ПрТовРаб", "1");
                XAttribute NaimEdIzm = new XAttribute("НаимЕдИзм", Item[i, 13]);
                SvedTov.Add(DopSvedTov);
                DopSvedTov.Add(PrTovRav);
                DopSvedTov.Add(NaimEdIzm);

                //<Документ><ТаблСчФакт><СведТов><ДопСведТов><НомСредИдентТов><НомУпак>     маркировка
                if (Item[i, 9].ToString().Contains("10") && CurrDataUPD[2].ToString().Equals("LentaMark"))
                {
                    string nomUpakValue = "020" + Item[i, 0] + "37";
                    nomUpakValue += (Math.Round(Convert.ToDecimal(Item[i, 4]))).ToString();
                    XElement NomSredIdent = new XElement("НомСредИдентТов");
                    XElement NomUpak = new XElement("НомУпак", nomUpakValue);
                    DopSvedTov.Add(NomSredIdent);
                    NomSredIdent.Add(NomUpak);
                }

                //<Документ><ТаблСчФакт><СведТов><ИнфПолФХЖ2>
                XElement InfPolFHJ23 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf3Identif = new XAttribute("Идентиф", "номер_заказа");
                XAttribute ItmTxtInf3Znachen = new XAttribute("Значен", CurrDataUPD[6]);
                SvedTov.Add(InfPolFHJ23);
                InfPolFHJ23.Add(ItmTxtInf3Identif);
                InfPolFHJ23.Add(ItmTxtInf3Znachen);


                //<Документ><ТаблСчФакт><СведТов><ИнфПолФХЖ2>
                string nomBuyerCd = Verifiacation.GetBuyerItemCodeRcd(Convert.ToString(infoKag[5]), Convert.ToInt64(Item[i, 1]));

                XElement InfPolFHJ21 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf1Identif = new XAttribute("Идентиф", "код_материала");
                XAttribute ItmTxtInf1Znachen = new XAttribute("Значен", nomBuyerCd);
                SvedTov.Add(InfPolFHJ21);
                InfPolFHJ21.Add(ItmTxtInf1Identif);
                InfPolFHJ21.Add(ItmTxtInf1Znachen);

                //<Документ><ТаблСчФакт><СведТов><ИнфПолФХЖ2>
                XElement InfPolFHJ22 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf2Identif = new XAttribute("Идентиф", "штрихкод");
                string EAN_F = "";
                EAN_F = Convert.ToString(Item[i, 0]).Substring(0, 13);  //Обрезаем штрих-код до 13 символов 
                //XAttribute ItmTxtInf2Znachen = new XAttribute("Значен", Item[i, 0]);
                XAttribute ItmTxtInf2Znachen = new XAttribute("Значен", EAN_F);
                SvedTov.Add(InfPolFHJ22);
                InfPolFHJ22.Add(ItmTxtInf2Identif);
                InfPolFHJ22.Add(ItmTxtInf2Znachen);

                //<Документ><ТаблКСчФ><СведТов><ИнфПолФХЖ2>
                XElement InfPolFHJ24 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf4Identif = new XAttribute("Идентиф", "номер_накладной");
                XAttribute ItmTxtInf4Znachen = new XAttribute("Значен", Verifiacation.GetSklnkNumber(Convert.ToInt32(CurrDataUPD[7])));
                SvedTov.Add(InfPolFHJ24);
                InfPolFHJ24.Add(ItmTxtInf4Identif);
                InfPolFHJ24.Add(ItmTxtInf4Znachen);
            }

            //<Документ><ТаблСчФакт><ВсегоОпл>
            XElement VsegoOpl = new XElement("ВсегоОпл");
            XAttribute VsegoBezNds = new XAttribute("СтТовБезНДСВсего", Math.Round(sumWthNds - sumNds, 2));
            XAttribute VsegoSNDS = new XAttribute("СтТовУчНалВсего", Math.Round(sumWthNds, 2));
            TabSF.Add(VsegoOpl);
            VsegoOpl.Add(VsegoBezNds);
            VsegoOpl.Add(VsegoSNDS);

            //<Документ><ТаблСчФакт><ВсегоОпл><СумНалВсего>
            XElement SumNalVsego = new XElement("СумНалВсего");
            VsegoOpl.Add(SumNalVsego);

            if (Convert.ToString(Item[0, 9]) == "без НДС")
            {
                //<Документ><ТаблСчФакт><ВсегоОпл><СумНалВсего><БезНДС>
                XElement SumNalVsegoSumNal = new XElement("БезНДС", "без НДС");
                SumNalVsego.Add(SumNalVsegoSumNal);
            }
            else
            {
                //<Документ><ТаблСчФакт><ВсегоОпл><СумНалВсего><СумНал>
                XElement SumNalVsegoSumNal = new XElement("СумНал", Math.Round(sumNds, 2));
                SumNalVsego.Add(SumNalVsegoSumNal);
            }

            //<Документ><ТаблСчФакт><ВсегоОпл><КолНеттоВс>
            XElement NettoVes = new XElement("КолНеттоВс", Math.Round(sumWeight, 3));
            VsegoOpl.Add(NettoVes);

            /************************** 3 уровень. <СвПродПер> ************************/
            XElement ProdPer = new XElement("СвПродПер");
            DOC.Add(ProdPer);

            //<Документ><СвПродПер><СвПер>
            XElement SvPer = new XElement("СвПер");
            XAttribute SodOper = new XAttribute("СодОпер", "Товары переданы");
            ProdPer.Add(SvPer);
            SvPer.Add(SodOper);

            //<Документ><СвПродПер><СвПер><ОснПер>
            XElement OsnPer = new XElement("ОснПер");
            XAttribute NaimOsn = new XAttribute("НаимОсн", "Договор");
            XAttribute NomOsn = new XAttribute("НомОсн",infoKag[14]); //CurrDataUPD[6] номер договора
            XAttribute DataOsn = new XAttribute("ДатаОсн", infoKag[15]);//Convert.ToDateTime(CurrDataUPD[8]).ToString(@"dd.MM.yyyy") дата договора
            SvPer.Add(OsnPer);
            OsnPer.Add(NaimOsn);
            OsnPer.Add(NomOsn);
            OsnPer.Add(DataOsn);

            //<Документ><СвПродПер><СвПер><СвЛицПер>
            XElement SvLicPer = new XElement("СвЛицПер");
            SvPer.Add(SvLicPer);

            //<Документ><СвПродПер><СвПер><СвЛицПер><РабОргПрод>
            XElement RabOrgProd = new XElement("РабОргПрод");
            XAttribute Doljnost = new XAttribute("Должность", "Водитель");
            SvLicPer.Add(RabOrgProd);
            RabOrgProd.Add(Doljnost);

            //<Документ><СвПродПер><СвПер><СвЛицПер><РабОргПрод><ФИО>
            XElement pFIO = new XElement("ФИО");

            string sFIO = Convert.ToString(CurrDataUPD[9]);
            int len = sFIO.Length;
            string sF;
            string sI;
            string sO;
            int p1 = sFIO.IndexOf(" ");
            int p2 = sFIO.LastIndexOf(" ");
            if (len > 0)
            {
                try { sF = sFIO.Remove(p1); }
                catch { sF = "НеУказано"; }
                //sF = sFIO.Remove(p1);
                try { sI = sFIO.Substring(p1, (p2 - p1)); }
                catch { sI = "НеУказано"; }
                //I = sFIO.Substring(p1, (p2 - p1));
                try { sO = sFIO.Substring(p2, len - p2); }
                catch { sO = "НеУказано"; }
                //sO = sFIO.Substring(p2, len - p2);
                if (sF.Length <= 0) sF = "НеУказано";
                if (sI.Length <= 0) sF = "НеУказано";
                if (sO.Length <= 0) sF = "НеУказано";
            }
            else
            {
                sF = "НеУказано";
                sI = "НеУказано";
                sO = "НеУказано";
            }

            XAttribute pFIOF = new XAttribute("Фамилия", sF.Trim());
            XAttribute pFIOI = new XAttribute("Имя", sI.Trim());
            XAttribute pFIOO = new XAttribute("Отчество", sO.Trim());

            RabOrgProd.Add(pFIO);
            pFIO.Add(pFIOF);
            pFIO.Add(pFIOI);
            pFIO.Add(pFIOO);

            /************************** 3 уровень. <Подписант> ************************/
            string[] infoSigner = Verifiacation.GetSignerOpt(); //0 ОБЩИЕ.ЭЦПДОЛЖНОСТЬ, 1 ОБЩИЕ.ЭЦПФАМИЛИЯ, 2 ОБЩИЕ.ЭЦПИМЯ, 3 ОБЩИЕ.ЭЦПОТЧЕСТВО, 4 ОБЩИЕ.ЭЦПДОЛЖНОБЯЗ

            XElement Podp = new XElement("Подписант");
            XAttribute obl = new XAttribute("ОблПолн", "6");
            XAttribute status = new XAttribute("Статус", "1");
            XAttribute osn = new XAttribute("ОснПолн", infoSigner[4]);
            DOC.Add(Podp);
            Podp.Add(obl);
            Podp.Add(status);
            Podp.Add(osn);

            //<Документ><Подписант><ЮЛ>
            XElement UL = new XElement("ЮЛ");
            XAttribute innUl = new XAttribute("ИННЮЛ", infoFirm[1]);
            XAttribute naimOrg = new XAttribute("НаимОрг", infoFirm[0]);
            XAttribute dolj = new XAttribute("Должн", infoSigner[0]);
            Podp.Add(UL);
            UL.Add(innUl);
            UL.Add(naimOrg);
            UL.Add(dolj);

            //<Документ><Подписант><ЮЛ><ФИО>
            XElement FIO = new XElement("ФИО");
            XAttribute famdir = new XAttribute("Фамилия", infoSigner[1]);
            XAttribute namedir = new XAttribute("Имя", infoSigner[2]);
            XAttribute otchesdir = new XAttribute("Отчество", infoSigner[3]);
            UL.Add(FIO);
            FIO.Add(famdir);
            FIO.Add(namedir);
            FIO.Add(otchesdir);

            //------сохранение документа-----------
            fileName = fileName + ".xml";
            try
            {
                xdoc.Save(pathArchiveEDI + fileName);
                try
                {
                    xdoc.Save(pathUPDEDI + fileName);
                    /*//Load the XmlSchemaSet.
                    XmlSchemaSet schemaSet = new XmlSchemaSet();
                    schemaSet.Add("urn:bookstore-schema", "ON_SCHFDOPPR_1_995_01_05_01_02.xsd");

                    //Validate the file using the schema stored in the schema set.
                    //Any elements belonging to the namespace "urn:cd-schema" generate
                    //a warning because there is no schema matching that namespace.
                    Validate(pathUPDEDI + fileName, schemaSet);
                    Console.ReadLine();*/
                    string message = "EDISOFT. УПД " + typeFunc + " " + fileName + " создан в " + pathUPDEDI;
                    Program.WriteLine(message);
                    DispOrders.WriteProtocolEDI("УПД " + typeFunc, fileName, infoKag[0] + " - " + infoKag[1], 0, infoGpl[0] + " - " + infoGpl[1], "УПД " + typeFunc + " сформирован", DateTime.Now, Convert.ToString(CurrDataUPD[6]), "EDISOFT");
                    if (typeFunc == "ДОП")
                        DispOrders.WriteEDiSentDoc("10", fileName, Convert.ToString(CurrDataUPD[3]), Convert.ToString(infoSf[0]), "123", Convert.ToString(sumWthNds), Convert.ToString(CurrDataUPD[7]), 1, CurrDataUPD[11].ToString());
                    else
                        DispOrders.WriteEDiSentDoc("8", fileName, Convert.ToString(CurrDataUPD[3]), Convert.ToString(infoSf[0]), "123", Convert.ToString(sumWthNds), Convert.ToString(CurrDataUPD[7]), 1, CurrDataUPD[11].ToString());

                }
                catch (Exception e)
                {
                    string message_error = "EDISOFT. Не могу создать xml файл УПД " + typeFunc + " в " + pathUPDEDI + ". Нет доступа или диск переполнен.";
                    DispOrders.WriteProtocolEDI("УПД " + typeFunc, fileName, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УПД " + typeFunc + " не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUPD[6]), "EDISOFT");
                    Program.WriteLine(message_error);
                    DispOrders.WriteErrorLog(e.Message);
                }
            }
            catch (Exception e)
            {
                string message_error = "EDISOFT. Не могу создать xml файл УПД " + typeFunc + " в " + pathArchiveEDI + ". Нет доступа или диск переполнен.";
                DispOrders.WriteProtocolEDI("УПД " + typeFunc, fileName, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УПД не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUPD[6]), "EDISOFT");
                Program.WriteLine(message_error);
                DispOrders.WriteErrorLog(e.Message);
                //запись в лог о неудаче
            }
        }

        /********************************************************** УПД Лента (Новый формат ) ************************************************************/

        /*************************************************************************************************************************************/
        /********************************************************** Начало УКД Лента  ********************************************************/
        /*************************************************************************************************************************************/
        public static void CreateEdiLenta_UKD(List<object> CurrDataUKD) //список УКД, 0 ProviderOpt, 1 ProviderZkg, 2 NastDoc_Fmt, 3 SklSf_Rcd, 4 SklSf_TpOtg, 5 SklSfA_RcdCor, 6 PrdZkg_NmrExt, 7 PrdZkg_Rcd, 8 PrdZkg_Dt ,9 SklNk_TDrvNm
        {
            //получение путей
            string pathArchiveEDI = /*"D:\\Edi\\Archive\\"; //*/DispOrders.GetValueOption("EDI-СОФТ.АРХИВ");
            string pathUKDEDI;

            //Запрос данных КорректировочнойСФ
            object[] infoSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataUKD[3])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO
            //Запрос данных Корректируемой (отгрузочной) СФ
            object[] infoCorSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataUKD[5])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO
            //Запрос предыдущих Корректировочных СФ корректируемые текущей корректировочной СФ
            object[,] InfoPrevSf = Verifiacation.GetPrevSfToKSFAsOb(Convert.ToString(CurrDataUKD[3]), Convert.ToString(CurrDataUKD[5])); //0 SklSf_Rcd, 1 SklSf_Nmr, 2 SklSf_Dt

            //запрос данных спецификации
            object[,] Item = Verifiacation.GetItemsFromKSF(Convert.ToString(CurrDataUKD[3]), Convert.ToString(CurrDataUKD[5]), true); //0 BarCode_Code, 1 SklN_Rcd, 2 SklN_Cd, 3 SklN_Nm, 4 Кол-во, 5 Цена без НДC, 6 Цена с НДС, 7 Код ЕИ EDI, 8 ОКЕЙ, 9 Ставка, 10 'S', 11 Сумма НДС, 12 Сумма с НДС, 13 шифр ЕИ, 14 Вес

            //Запрос данных покупателя
            object[] infoKag = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(infoSf[2]), Convert.ToInt64(infoSf[3])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование
            //Запрос данных покупателя
            object[] infoGpl = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(infoSf[4]), Convert.ToInt64(infoSf[5])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование

            //string codeByBuyer = "";  // почему-то он её не видит, когда по ошибке выходит. Хотя должен вернуть "ошибка"
            //codeByBuyer = Verifiacation.GetFldFromEdiExch(Convert.ToInt64(CurrDataUKD[7]), "Exch_USelCdByer"); //Код поставщика для Ашана

            //какой gln номер использовать
            bool useMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(infoSf[4]));
            string ilnFirm;

            object[] infoFirm;
            object[] infoFirmAdr;
            object[] infoFirmGrOt;
            object[] infoFirmAdrGrOt;
            if (useMasterGLN == false)//используем данные текущего предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ИЛН");
                pathUKDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/DispOrders.GetValueOption("EDI-СОФТ.УКД");
                infoFirm = Verifiacation.GetFirmInfo(); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO
                infoFirmAdr = Verifiacation.GetFirmAdr(); // 0 CrtAdr_StrNm+','+CrtAdr_House, 1 CrtAdr_TowNm, 2 CrtAdr_RegNm, 3 CrtAdr_Ind, 4 CrtAdr_RegCd
                infoFirmGrOt = infoFirm;
                infoFirmAdrGrOt = infoFirmAdr;

            }
            else//используем данные головного предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");
                infoFirm = Verifiacation.GetMasterFirmInfo();
                infoFirmAdr = Verifiacation.GetMasterFirmAdr();
                infoFirmGrOt = Verifiacation.GetFirmInfo();
                infoFirmAdrGrOt = Verifiacation.GetFirmAdr();
                try
                {
                    pathUKDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/DispOrders.GetValueOption("EDI-СОФТ.ЭКСПОРТ");
                }
                catch
                {
                    pathUKDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/DispOrders.GetValueOption("EDI-СОФТ.УКД");
                }
            }

            string idEdo = DispOrders.GetValueOption("EDI-СОФТ.ИДЭДО");  //"2IJ"; //ИдЭДО

            string idOtpr = idEdo + ilnFirm; //ИдОтпр
            string idPol = idEdo + infoGpl[2].ToString(); //ИдПол

            string guid = Convert.ToString(Guid.NewGuid());
            string fileName;
            if (CurrDataUKD[2].ToString().Equals("LentaMark"))
            {
                fileName = "ON_NKORSCHFDOPPRMARK_" + idPol + "_" + idOtpr + "_" + DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid;//ИдФайл   
            }
            else
            {
                fileName = "ON_NKORSCHFDOPPR_" + idPol + "_" + idOtpr + "_" + DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid;//ИдФайл
            }
            

            /************************** 1 уровень. <Файл> ******************************/

            XDocument xdoc = new XDocument(new XDeclaration("1.0", "", ""));

            XElement File = new XElement("Файл");
            XAttribute IdFile = new XAttribute("ИдФайл", "000000000000000000000000000000000000"/*fileName*/);
            XAttribute VersForm = new XAttribute("ВерсФорм", "5.01");
            XAttribute VersProg = new XAttribute("ВерсПрог", "Edisoft");

            xdoc.Add(File);
            File.Add(IdFile);
            File.Add(VersForm);
            File.Add(VersProg);

            /************************** 2 уровень. <СвУчДокОбор> ************************/

            XElement ID = new XElement("СвУчДокОбор");

            XAttribute IdSender = new XAttribute("ИдОтпр", "0000000000"/*idOtpr*/);
            XAttribute IdReciever = new XAttribute("ИдПол", "0000000000"/*idPol*/);

            File.Add(ID);
            ID.Add(IdSender);
            ID.Add(IdReciever);

            //<СвУчДокОбор><СвОЭДОтпр>
            XElement InfOrg = new XElement("СвОЭДОтпр");
            string providerNm = DispOrders.GetValueOption("EDI-СОФТ.НМ");
            string providerInn = DispOrders.GetValueOption("EDI-СОФТ.ИНН");
            XAttribute NaimOrg = new XAttribute("НаимОрг", providerNm/*"Эдисофт, ООО"*/);
            XAttribute INNUL = new XAttribute("ИННЮЛ", providerInn/*"7801471082"*/);
            XAttribute IDEDO = new XAttribute("ИдЭДО", idEdo);

            ID.Add(InfOrg);
            InfOrg.Add(NaimOrg);
            InfOrg.Add(INNUL);
            InfOrg.Add(IDEDO);

            /************************** 2 уровень. <Документ> ************************/

            XElement DOC = new XElement("Документ");
            XAttribute KND = new XAttribute("КНД", "1115133");
            XAttribute Function = new XAttribute("Функция", "КСЧФ");
            XAttribute PoFakt = new XAttribute("ПоФактХЖ", "Документ, подтверждающий согласие (факт уведомления) покупателя на изменение стоимости отгруженных товаров (выполненных работ, оказанных услуг), переданных имущественных прав");
            XAttribute NaimDocOpr = new XAttribute("НаимДокОпр", "Документ, подтверждающий согласие (факт уведомления) покупателя на изменение стоимости отгруженных товаров (выполненных работ, оказанных услуг), переданных имущественных прав");
            XAttribute DateF = new XAttribute("ДатаИнфПр", DateTime.Today.ToString(@"dd.MM.yyyy"));
            XAttribute TimeF = new XAttribute("ВремИнфПр", DateTime.Today.ToString(@"hh.mm.ss"));
            XAttribute NameOrg = new XAttribute("НаимЭконСубСост", infoFirm[0].ToString() /*+ ", ИНН-КПП: " + infoFirm[1].ToString() + "-" + infoFirm[2].ToString()*/);

            File.Add(DOC);
            DOC.Add(KND);
            DOC.Add(Function);
            DOC.Add(PoFakt);
            DOC.Add(NaimDocOpr);
            DOC.Add(DateF);
            DOC.Add(TimeF);
            DOC.Add(NameOrg);

            //<Документ><СвКСчФ>
            XElement SVSF = new XElement("СвКСчФ");
            XAttribute NomSF = new XAttribute("НомерКСчФ", infoSf[0].ToString());
            XAttribute DateSF = new XAttribute("ДатаКСчФ", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));
            XAttribute Kod = new XAttribute("КодОКВ", infoSf[6].ToString());

            DOC.Add(SVSF);
            SVSF.Add(NomSF);
            SVSF.Add(DateSF);
            SVSF.Add(Kod);

            //<Документ><СвКСчФ><СчФ>
            XElement SchF = new XElement("СчФ");
            SVSF.Add(SchF);

            if (InfoPrevSf.GetLength(0) > 0)
            {
                XAttribute NomSchF = new XAttribute("НомерСчФ", InfoPrevSf[0, 1].ToString());
                XAttribute DateSchF = new XAttribute("ДатаСчФ", Convert.ToDateTime(InfoPrevSf[0, 2]).ToString(@"dd.MM.yyyy"));
                SchF.Add(NomSchF);
                SchF.Add(DateSchF);
            }
            else
            {
                XAttribute NomSchF = new XAttribute("НомерСчФ", infoCorSf[0].ToString());
                XAttribute DateSchF = new XAttribute("ДатаСчФ", Convert.ToDateTime(infoCorSf[1]).ToString(@"dd.MM.yyyy"));
                SchF.Add(NomSchF);
                SchF.Add(DateSchF);
            }

            //<Документ><<СвКСчФ>><СвПрод>
            XElement SvProd = new XElement("СвПрод");
            //XAttribute SvProdOKPO = new XAttribute("ОКПО", infoFirm[3].ToString());
            SVSF.Add(SvProd);
            //SvProd.Add(SvProdOKPO);

            //<Документ><<СвКСчФ>><СвПрод><ИдСв>
            XElement SvProdIdSv = new XElement("ИдСв");
            SvProd.Add(SvProdIdSv);

            //<Документ><<СвКСчФ>><СвПрод><ИдСв><СвЮЛУч>
            XElement SvProdSvUluchh = new XElement("СвЮЛУч");
            XAttribute SvProdIdSvName = new XAttribute("НаимОрг", infoFirm[0].ToString());
            XAttribute SvProdIdSvINN = new XAttribute("ИННЮЛ", infoFirm[1].ToString());
            XAttribute SvProdIdSvKPP = new XAttribute("КПП", infoFirm[2].ToString());
            if (useMasterGLN)
                SvProdIdSvKPP = new XAttribute("КПП", infoFirmGrOt[2].ToString());

            SvProdIdSv.Add(SvProdSvUluchh);
            SvProdSvUluchh.Add(SvProdIdSvName);
            SvProdSvUluchh.Add(SvProdIdSvINN);
            SvProdSvUluchh.Add(SvProdIdSvKPP);

            //<Документ><<СвКСчФ>><СвПрод><Адрес>
            XElement SvProdAdres = new XElement("Адрес");
            SvProd.Add(SvProdAdres);

            //<Документ><<СвКСчФ>><СвПрод><Адрес><АдресРФ>
            //Адрес
            XElement SvProdAdrRF = new XElement("АдрРФ");
            SvProdAdres.Add(SvProdAdrRF);
            if (infoFirmAdr[3].ToString() != "")
            {
                XAttribute SvProdIndex = new XAttribute("Индекс", infoFirmAdr[3].ToString());
                SvProdAdrRF.Add(SvProdIndex);
            }
            if (infoFirmAdr[4].ToString() != "")
            {
                XAttribute SvProdKodReg = new XAttribute("КодРегион", infoFirmAdr[4].ToString());
                SvProdAdrRF.Add(SvProdKodReg);
            }

            /*//<Документ><<СвКСчФ>><ГрузОт>
            XElement GruzOt = new XElement("ГрузОт");
            XElement GruzOtOnJe = new XElement("ОнЖе", "он же");
            SVSF.Add(GruzOt);
            GruzOt.Add(GruzOtOnJe);

            //<Документ><<СвКСчФ>><ГрузПолуч>
            XElement GruzPoluch = new XElement("ГрузПолуч");
            SVSF.Add(GruzPoluch);

            //<Документ><<СвКСчФ>><ГрузПолуч><ИдСв>
            XElement GruzPoluchIdSv = new XElement("ИдСв");
            GruzPoluch.Add(GruzPoluchIdSv);

            //<Документ><<СвКСчФ>><ГрузПолуч><ИдСв><СвЮЛУч>
            XElement GruzPoluchSvUluch = new XElement("СвЮЛУч");
            XAttribute GruzPoluchName = new XAttribute("НаимОрг", infoGpl[1]);
            XAttribute GruzPoluchINN = new XAttribute("ИННЮЛ", infoGpl[3]);
            XAttribute GruzPoluchKPP = new XAttribute("КПП", infoGpl[4]);

            GruzPoluchIdSv.Add(GruzPoluchSvUluch);
            GruzPoluchSvUluch.Add(GruzPoluchName);
            GruzPoluchSvUluch.Add(GruzPoluchINN);
            GruzPoluchSvUluch.Add(GruzPoluchKPP);

            //<Документ><<СвКСчФ>><ГрузПолуч><Адрес>
            XElement GruzPoluchAdres = new XElement("Адрес");
            GruzPoluch.Add(GruzPoluchAdres);

            //<Документ><<СвКСчФ>><ГрузПолуч><Адрес><АдресРФ>
            XElement GruzPoluchAdrRF = new XElement("АдрРФ");
            XAttribute GruzPoluchIndex = new XAttribute("Индекс", infoGpl[7]);
            XAttribute GruzPoluchKodReg = new XAttribute("КодРегион", infoGpl[8]);
            GruzPoluchAdres.Add(GruzPoluchAdrRF);
            GruzPoluchAdrRF.Add(GruzPoluchIndex);
            GruzPoluchAdrRF.Add(GruzPoluchKodReg);*/

            //<Документ><<СвКСчФ>><СвПокуп>
            XElement SvPokup = new XElement("СвПокуп");
            SVSF.Add(SvPokup);

            //<Документ><<СвКСчФ>><СвПокуп><ИдСв>
            XElement SvPokupIdSv = new XElement("ИдСв");
            SvPokup.Add(SvPokupIdSv);

            //<Документ><СвСчФакт><СвПокуп><ИдСв><СвЮЛУч>             
            XElement SvPokupSvUluch = new XElement("СвЮЛУч");
            XAttribute SvPokupName = new XAttribute("НаимОрг", "#NAME#"/*infoKag[1]*/);
            XAttribute SvPokupINN = new XAttribute("ИННЮЛ", infoKag[3]);
            XAttribute SvPokupKPP = new XAttribute("КПП", infoKag[4]);
            SvPokupIdSv.Add(SvPokupSvUluch);
            SvPokupSvUluch.Add(SvPokupName);
            SvPokupSvUluch.Add(SvPokupINN);
            SvPokupSvUluch.Add(SvPokupKPP);

            //<Документ><<СвКСчФ>><СвПокуп><Адрес>
            XElement SvPokupAdres = new XElement("Адрес");
            SvPokup.Add(SvPokupAdres);

            //<Документ><<СвКСчФ>><СвПокуп><Адрес><АдресРФ>
            XElement SvPokupAdrRF = new XElement("АдрРФ");
            SvPokupAdres.Add(SvPokupAdrRF);
            if (infoGpl[7].ToString().Length > 0)
            {
                XAttribute SvPokupIndex = new XAttribute("Индекс", infoKag[7]);
                SvPokupAdrRF.Add(SvPokupIndex);
            }
            if (infoGpl[8].ToString().Length > 0)
            {
                XAttribute SvPokupKodReg = new XAttribute("КодРегион", infoKag[8]);
                SvPokupAdrRF.Add(SvPokupKodReg);
            }
            if (infoGpl[9].ToString().Length > 0)
            {
                XAttribute SvPokupCity = new XAttribute("Город", infoKag[9]);
                SvPokupAdrRF.Add(SvPokupCity);
            }
            if (infoGpl[10].ToString().Length > 0)
            {
                XAttribute SvPokupStreet = new XAttribute("Улица", infoKag[10]);
                SvPokupAdrRF.Add(SvPokupStreet);
            }
            if (infoGpl[11].ToString().Length > 0)
            {
                XAttribute SvPokupHouse = new XAttribute("Дом", infoKag[11]);
                SvPokupAdrRF.Add(SvPokupHouse);
            }

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1>
            //Убран по просьбе Ленты 18102020
            /*XElement DopSvFHJ1 = new XElement("ДопСвФХЖ1");
            XAttribute NaimOKV = new XAttribute("НаимОКВ", "Российский рубль");
            SVSF.Add(DopSvFHJ1);
            DopSvFHJ1.Add(NaimOKV);*/

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1>
            XElement InfPolFHJ1 = new XElement("ИнфПолФХЖ1");
            SVSF.Add(InfPolFHJ1);

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1><ТекстИнф>
            /*XElement TxtInf1 = new XElement("ТекстИнф");
            XAttribute TxtInf1Identif = new XAttribute("Идентиф", "номер_заказа");
            XAttribute TxtInf1Znachen = new XAttribute("Значен", CurrDataUKD[6]);
            InfPolFHJ1.Add(TxtInf1);
            TxtInf1.Add(TxtInf1Identif);
            TxtInf1.Add(TxtInf1Znachen);*/

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf2 = new XElement("ТекстИнф");
            XAttribute TxtInf2Identif = new XAttribute("Идентиф", "отправитель");
            XAttribute TxtInf2Znachen = new XAttribute("Значен", ilnFirm);
            InfPolFHJ1.Add(TxtInf2);
            TxtInf2.Add(TxtInf2Identif);
            TxtInf2.Add(TxtInf2Znachen);

            //Добавить <ТекстИнф Идентиф="грузополучатель" Значен="4606068901929"/>
            XElement TxtInf3 = new XElement("ТекстИнф");
            XAttribute TxtInf3Identif = new XAttribute("Идентиф", "грузополучатель");
            XAttribute TxtInf3Znachen = new XAttribute("Значен", infoGpl[2]);
            InfPolFHJ1.Add(TxtInf3);
            TxtInf3.Add(TxtInf3Identif);
            TxtInf3.Add(TxtInf3Znachen);

            /************************** 3 уровень. <ТаблКСчФ> ************************/
            XElement TabSF = new XElement("ТаблКСчФ");
            DOC.Add(TabSF);

            decimal sumW0Nds_V = 0;
            decimal sumNds_V = 0;
            decimal sumWthNds_V = 0;

            decimal sumW0Nds_G = 0;
            decimal sumNds_G = 0;
            decimal sumWthNds_G = 0;

            decimal summWoNds_A = 0;
            decimal summWoNds_B = 0;

            //Item[] 0-Рсд, 1-ШК, 2-Название,  3-A_EiSh,  4-A_EiOkei,  5-A_EiEdi,  6-A_EiAcc,  7-A_QtOsn,  8-A_QtCn,  9-A_CnWoNds, 10-A_Tax, 11-A_SummNds, 12-A_SummWthNds 
            //                              , 13-B_EiSh, 14-B_EiOkei, 15-B_EiEdi, 16-B_EiAcc, 17-B_QtOsn, 18-B_QtCn, 19-B_CnWoNds, 20-B_Tax, 21-B_SummNds, 22-B_SummWthNds
            for (int i = 0; i < Item.GetLongLength(0); i++)
            {
                //<Документ><ТаблКСчФ><СведТов>
                XElement SvedTov = new XElement("СведТов");
                XAttribute NomStr = new XAttribute("НомСтр", Convert.ToString(i + 1));
                XAttribute NomTovVStr = new XAttribute("ПорНомТовВСЧФ", Convert.ToString(i + 1));
                XAttribute NaimTov = new XAttribute("НаимТов", Item[i, 2]);
                if (Item[i, 4] == null)
                    Item[i, 4] = Item[i, 14];         // то, что было ДО он не знает, не нашёл ... нолики не нравяться при проверке эдисофта, считаем, что не поменялось
                XAttribute OKEI_TovDo = new XAttribute("ОКЕИ_ТовДо", Item[i, 4]);
                XAttribute OKEI_TovPosle = new XAttribute("ОКЕИ_ТовПосле", Item[i, 14]);
                if (Item[i, 8] == null)
                    Item[i, 8] = "0";
                XAttribute KolTovDo = new XAttribute("КолТовДо", Math.Round(Convert.ToDecimal(Item[i, 8]), 2));
                XAttribute KolTovPosle = new XAttribute("КолТовПосле", Math.Round(Convert.ToDecimal(Item[i, 18]), 2));
                if (Item[i, 9] == null)
                    Item[i, 9] = "0";
                XAttribute CenaTovDo = new XAttribute("ЦенаТовДо", Item[i, 9]);
                XAttribute CenaTovPosle = new XAttribute("ЦенаТовПосле", Item[i, 19]);
                if (Item[i, 10] == null)
                    Item[i, 10] = Item[i, 20];
                XAttribute NalStDo = new XAttribute("НалСтДо", Convert.ToString(Item[i, 10]));     // а тут тоже ... только одна ставка берёт, но её он не видит .. ругается эдисофт "Value '0' is not facet-valid with respect to enumeration '[0%, 10%, 18%, 10/110, 18/118, без НДС]
                XAttribute NalStPosle = new XAttribute("НалСтПосле", Convert.ToString(Item[i, 20]));
                TabSF.Add(SvedTov);
                SvedTov.Add(NomStr);
                SvedTov.Add(NomTovVStr);
                SvedTov.Add(NaimTov);
                SvedTov.Add(OKEI_TovDo);
                SvedTov.Add(OKEI_TovPosle);
                SvedTov.Add(KolTovDo);
                SvedTov.Add(KolTovPosle);
                SvedTov.Add(CenaTovDo);
                SvedTov.Add(CenaTovPosle);
                SvedTov.Add(NalStDo);
                SvedTov.Add(NalStPosle);

                //<Документ><ТаблКСчФ><СведТов><СтТовБезНДС>
                XElement StTovBezNds = new XElement("СтТовБезНДС");
                summWoNds_A = Math.Round(Convert.ToDecimal(Item[i, 12]) - Convert.ToDecimal(Item[i, 11]), 2);
                summWoNds_B = Math.Round(Convert.ToDecimal(Item[i, 22]) - Convert.ToDecimal(Item[i, 21]), 2);
                XAttribute StoimDoIzm = new XAttribute("СтоимДоИзм", summWoNds_A);
                XAttribute StoimPosleIzm = new XAttribute("СтоимПослеИзм", summWoNds_B);
                XAttribute StoimUvel = new XAttribute("СтоимУвел", Math.Round(summWoNds_B - summWoNds_A, 2));
                XAttribute StoimUm = new XAttribute("СтоимУм", Math.Round(summWoNds_A - summWoNds_B, 2));
                SvedTov.Add(StTovBezNds);
                StTovBezNds.Add(StoimDoIzm);
                StTovBezNds.Add(StoimPosleIzm);
                if (summWoNds_A < summWoNds_B)
                {
                    StTovBezNds.Add(StoimUvel);
                    sumW0Nds_V = sumW0Nds_V + Math.Round(summWoNds_B - summWoNds_A, 2);
                }
                if (summWoNds_A > summWoNds_B)
                {
                    StTovBezNds.Add(StoimUm);
                    sumW0Nds_G = sumW0Nds_G + Math.Round(summWoNds_A - summWoNds_B, 2);
                }

                //<Документ><ТаблКСчФ><СведТов><АкцизДо>
                XElement AkcizDo = new XElement("АкцизДо");
                XElement bezAkcizDo = new XElement("БезАкциз", "без акциза");
                SvedTov.Add(AkcizDo);
                AkcizDo.Add(bezAkcizDo);

                //<Документ><ТаблКСчФ><СведТов><АкцизПосле>
                XElement AkcizPosle = new XElement("АкцизПосле");
                XElement bezAkcizPosle = new XElement("БезАкциз", "без акциза");
                SvedTov.Add(AkcizPosle);
                AkcizPosle.Add(bezAkcizPosle);

                //<Документ><ТаблКСчФ><СведТов><АкцизРазн>
                /*XElement AkcizRazn = new XElement("АкцизРазн");
                XElement AkcizRaznSumUvel = new XElement("СумУвел", "0.00");
                XElement AkcizRaznSumUm = new XElement("СумУм", "0.00");
                SvedTov.Add(AkcizRazn);
                if (summWoNds_A < summWoNds_B) AkcizRazn.Add(AkcizRaznSumUvel);
                if (summWoNds_A > summWoNds_B) AkcizRazn.Add(AkcizRaznSumUm);*/

                //<Документ><ТаблКСчФ><СведТов><СумНалДо>
                XElement SumNalDo = new XElement("СумНалДо");
                SvedTov.Add(SumNalDo);
                if (Convert.ToString(Item[i, 10]) == "без НДС")
                {
                    //<Документ><ТаблКСчФ><СведТов><СумНалДо><БезНДС>
                    XElement BezNdsDo = new XElement("БезНДС", "без НДС");
                    SumNalDo.Add(BezNdsDo);
                }
                else
                {
                    //<Документ><ТаблКСчФ><СведТов><СумНалДо><СумНДС>
                    XElement SumNalSumNdsDo = new XElement("СумНДС", Math.Round(Convert.ToDecimal(Item[i, 11]), 2));
                    SumNalDo.Add(SumNalSumNdsDo);
                }

                //<Документ><ТаблКСчФ><СведТов><СумНалПосле>
                XElement SumNalPosle = new XElement("СумНалПосле");
                SvedTov.Add(SumNalPosle);
                if (Convert.ToString(Item[i, 20]) == "без НДС")
                {
                    //<Документ><ТаблКСчФ><СведТов><СумНалПосле><БезНДС>
                    XElement BezNdsPosle = new XElement("БезНДС", "без НДС");
                    SumNalPosle.Add(BezNdsPosle);
                }
                else
                {
                    //<Документ><ТаблКСчФ><СведТов><СумНалПосле><СумНДС>
                    XElement SumNalSumNdsPosle = new XElement("СумНДС", Math.Round(Convert.ToDecimal(Item[i, 21]), 2));
                    SumNalPosle.Add(SumNalSumNdsPosle);
                }

                //<Документ><ТаблКСчФ><СведТов><СумНалРазн>
                XElement SumNalRazn = new XElement("СумНалРазн");
                XElement SumNalRaznSumUvel = new XElement("СумУвел", Math.Round(Convert.ToDecimal(Item[i, 21]) - Convert.ToDecimal(Item[i, 11]), 2));
                XElement SumNalRaznSumUm = new XElement("СумУм", Math.Round(Convert.ToDecimal(Item[i, 11]) - Convert.ToDecimal(Item[i, 21]), 2));
                SvedTov.Add(SumNalRazn);
                if (summWoNds_A < summWoNds_B)
                {
                    SumNalRazn.Add(SumNalRaznSumUvel);
                    sumNds_V = sumNds_V + Math.Round(Convert.ToDecimal(Item[i, 21]) - Convert.ToDecimal(Item[i, 11]), 2);
                }
                if (summWoNds_A > summWoNds_B)
                {
                    SumNalRazn.Add(SumNalRaznSumUm);
                    sumNds_G = sumNds_G + Math.Round(Convert.ToDecimal(Item[i, 11]) - Convert.ToDecimal(Item[i, 21]), 2);
                }

                //<Документ><ТаблКСчФ><СведТов><СтТовУчНал>
                XElement StTovUchNal = new XElement("СтТовУчНал");
                XAttribute StTovUchNalStoimDoIzm = new XAttribute("СтоимДоИзм", Math.Round(Convert.ToDecimal(Item[i, 12]), 2));
                XAttribute StTovUchNalStoimPosleIzm = new XAttribute("СтоимПослеИзм", Math.Round(Convert.ToDecimal(Item[i, 22]), 2));
                XAttribute StTovUchNalStoimUvel = new XAttribute("СтоимУвел", Math.Round(Convert.ToDecimal(Item[i, 22]) - Convert.ToDecimal(Item[i, 12]), 2));
                XAttribute StTovUchNalStoimUm = new XAttribute("СтоимУм", Math.Round(Convert.ToDecimal(Item[i, 12]) - Convert.ToDecimal(Item[i, 22]), 2));
                SvedTov.Add(StTovUchNal);
                StTovUchNal.Add(StTovUchNalStoimDoIzm);
                StTovUchNal.Add(StTovUchNalStoimPosleIzm);
                if (summWoNds_A < summWoNds_B)
                    if (summWoNds_A > summWoNds_B)

                        if (summWoNds_A < summWoNds_B)
                        {
                            StTovUchNal.Add(StTovUchNalStoimUvel);
                            sumWthNds_V = sumWthNds_V + Math.Round(Convert.ToDecimal(Item[i, 22]) - Convert.ToDecimal(Item[i, 12]), 2);
                        }
                if (summWoNds_A > summWoNds_B)
                {
                    StTovUchNal.Add(StTovUchNalStoimUm);
                    sumWthNds_G = sumWthNds_G + Math.Round(Convert.ToDecimal(Item[i, 12]) - Convert.ToDecimal(Item[i, 22]), 2);
                }

                //<Документ><ТаблКСчФ><СведТов><ИнфПолФХЖ2>
                XElement InfPolFHJ23 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf3Identif = new XAttribute("Идентиф", "номер_заказа");
                XAttribute ItmTxtInf3Znachen = new XAttribute("Значен", CurrDataUKD[6]);
                SvedTov.Add(InfPolFHJ23);
                InfPolFHJ23.Add(ItmTxtInf3Identif);
                InfPolFHJ23.Add(ItmTxtInf3Znachen);

                //<Документ><ТаблКСчФ><СведТов><ИнфПолФХЖ2>
                string nomBuyerCd = Verifiacation.GetBuyerItemCodeRcd(Convert.ToString(infoKag[5]), Convert.ToInt64(Item[i, 0]));

                XElement InfPolFHJ21 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf1Identif = new XAttribute("Идентиф", "код_материала");
                XAttribute ItmTxtInf1Znachen = new XAttribute("Значен", nomBuyerCd);
                SvedTov.Add(InfPolFHJ21);
                InfPolFHJ21.Add(ItmTxtInf1Identif);
                InfPolFHJ21.Add(ItmTxtInf1Znachen);

                //<Документ><ТаблКСчФ><СведТов><ИнфПолФХЖ2>
                XElement InfPolFHJ22 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf2Identif = new XAttribute("Идентиф", "штрихкод");
                string EAN_F = "";
                EAN_F = Convert.ToString(Item[i, 1]).Substring(0, 13);  //Обрезаем штрих-код до 13 символов 
                //XAttribute ItmTxtInf2Znachen = new XAttribute("Значен", Item[i, 1]);
                XAttribute ItmTxtInf2Znachen = new XAttribute("Значен", EAN_F);
                SvedTov.Add(InfPolFHJ22);
                InfPolFHJ22.Add(ItmTxtInf2Identif);
                InfPolFHJ22.Add(ItmTxtInf2Znachen);

                //<Документ><ТаблКСчФ><СведТов><ИнфПолФХЖ2>
                XElement InfPolFHJ24 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf4Identif = new XAttribute("Идентиф", "номер_накладной");
                XAttribute ItmTxtInf4Znachen = new XAttribute("Значен", Verifiacation.GetSklnkNumber(Convert.ToInt32(CurrDataUKD[7])));
                SvedTov.Add(InfPolFHJ24);
                InfPolFHJ24.Add(ItmTxtInf4Identif);
                InfPolFHJ24.Add(ItmTxtInf4Znachen);

                //<Документ><ТаблКСчФ><СведТов><ДопСведТов>
                XElement DopInfo = new XElement("ДопСведТов");
                XAttribute NmEiBefore = new XAttribute("НаимЕдИзмДо", Item[i, 3]);
                XAttribute NmEiAfter = new XAttribute("НаимЕдИзмПосле", Item[i, 13]);
                SvedTov.Add(DopInfo);
                DopInfo.Add(NmEiBefore);
                DopInfo.Add(NmEiAfter);

                //<Документ><ТаблКСчФ><СведТов><ДопСведТов><НомСредИдентТов[До/После]><НомУпак>
                if (Item[i, 20].ToString().Contains("10") && CurrDataUKD[2].ToString().Equals("LentaMark"))    //Item[i, 20] - ставка НДС после
                {
                    string nomUpakValueDo = "020" + Item[i, 0] + "37" + (Math.Round(Convert.ToDecimal(Item[i, 8]))).ToString();             //Item[i, 8] - количество до
                    string nomUpakValuePosle = "020" + Item[i, 0] + "37" + (Math.Round(Convert.ToDecimal(Item[i, 18]))).ToString();         //Item[i, 18] - количество после
                    XElement NomSredIdentDo = new XElement("НомСредИдентТовДо");
                    XElement NomUpakDo = new XElement("НомУпак", nomUpakValueDo);
                    XElement NomSredIdentPosle = new XElement("НомСредИдентТовПосле");
                    XElement NomUpakPosle = new XElement("НомУпак", nomUpakValuePosle);
                    DopInfo.Add(NomSredIdentDo);
                    NomSredIdentDo.Add(NomUpakDo);
                    DopInfo.Add(NomSredIdentPosle);
                    NomSredIdentPosle.Add(NomUpakPosle);
                }
            }

            if (sumWthNds_V > 0)
            {
                //<Документ><ТаблКСчФ><ВсегоУвел>
                XElement VsegoUvel = new XElement("ВсегоУвел");
                XAttribute VsegoUvelBezNds = new XAttribute("СтТовБезНДСВсего", Math.Round(sumW0Nds_V, 2));
                XAttribute VsegoUvelSNDS = new XAttribute("СтТовУчНалВсего", Math.Round(sumWthNds_V, 2));
                TabSF.Add(VsegoUvel);
                VsegoUvel.Add(VsegoUvelBezNds);
                VsegoUvel.Add(VsegoUvelSNDS);

                //<Документ><ТаблКСчФ><ВсегоУвел><СумНал>
                XElement VsegoUvelSumNal = new XElement("СумНал");
                VsegoUvel.Add(VsegoUvelSumNal);
                if (Convert.ToString(Item[0, 10]) == "без НДС")
                {
                    //<Документ><ТаблКСчФ><ВсегоУвел><СумНал><БезНДС>
                    XElement VsegoUvelSumNalBezNds = new XElement("БезНДС", "без НДС");
                    VsegoUvelSumNal.Add(VsegoUvelSumNalBezNds);
                }
                else
                {
                    //<Документ><ТаблКСчФ><ВсегоОпл><СумНал><СумНал>
                    XElement VsegoUvelSumNalSumMal = new XElement("СумНДС", Math.Round(sumNds_V, 2));
                    VsegoUvelSumNal.Add(VsegoUvelSumNalSumMal);
                }
            }

            if (sumWthNds_G > 0)
            {
                //<Документ><ТаблКСчФ><ВсегоУм>
                XElement VsegoUm = new XElement("ВсегоУм");
                XAttribute VsegoUmBezNds = new XAttribute("СтТовБезНДСВсего", Math.Round(sumW0Nds_G, 2));
                XAttribute VsegoUmSNDS = new XAttribute("СтТовУчНалВсего", Math.Round(sumWthNds_G, 2));
                TabSF.Add(VsegoUm);
                VsegoUm.Add(VsegoUmBezNds);
                VsegoUm.Add(VsegoUmSNDS);

                //<Документ><ТаблКСчФ><ВсегоУвел><СумНал>
                XElement VsegoUmSumNal = new XElement("СумНал");
                VsegoUm.Add(VsegoUmSumNal);
                if (Convert.ToString(Item[0, 10]) == "без НДС")
                {
                    //<Документ><ТаблКСчФ><ВсегоУвел><СумНал><БезНДС>
                    XElement VsegoUmSumNalBezNds = new XElement("БезНДС", "без НДС");
                    VsegoUmSumNal.Add(VsegoUmSumNalBezNds);
                }
                else
                {
                    //<Документ><ТаблКСчФ><ВсегоОпл><СумНал><СумНал>
                    XElement VsegoUmSumNalSumMal = new XElement("СумНДС", Math.Round(sumNds_G, 2));
                    VsegoUmSumNal.Add(VsegoUmSumNalSumMal);
                }
            }

            /************************** 3 уровень. <СодФХЖ3> ************************/
            /*XElement SodFHJ3 = new XElement("СодФХЖ3");
            DOC.Add(SodFHJ3);
            XAttribute InieSvIzmStoim = new XAttribute("ИныеСвИзмСтоим", "Изменения");
            SodFHJ3.Add(InieSvIzmStoim);

            string allPeredSf = string.Empty;

            if (InfoPrevSf.GetLength(0) > 0)
                allPeredSf = InfoPrevSf[0, 1].ToString() + " от " + Convert.ToDateTime(InfoPrevSf[0, 2]).ToString(@"dd.MM.yyyy");
            else
                allPeredSf = infoCorSf[0].ToString() + " от " + Convert.ToDateTime(infoCorSf[1]).ToString(@"dd.MM.yyyy");

            XAttribute PeredatDocum = new XAttribute("ПередатДокум", allPeredSf);
            SodFHJ3.Add(PeredatDocum);
            XAttribute SodOper = new XAttribute("СодОпер", "Иные");
            SodFHJ3.Add(SodOper);

            XAttribute DataNapr = new XAttribute("ДатаНапр", Convert.ToDateTime(infoCorSf[1]).ToString(@"dd.MM.yyyy"));
            SodFHJ3.Add(DataNapr);

            //<Документ><СодФХЖ3><ОснКор>
            XElement OsnKorr = new XElement("ОснКор");
            XAttribute NaimOsn = new XAttribute("НаимОсн", "Иные");
            XAttribute DataOsn = new XAttribute("ДатаОсн", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));
            XAttribute DopSvedOsn = new XAttribute("ДопСвОсн", "Отсутствуют");
            SodFHJ3.Add(OsnKorr);
            OsnKorr.Add(NaimOsn);
            OsnKorr.Add(DataOsn);
            OsnKorr.Add(DopSvedOsn);*/

            XElement SodFHJ3 = new XElement("СодФХЖ3");
            XAttribute SodOper = new XAttribute("СодОпер", "Корректировка");
            XAttribute DataNapr = new XAttribute("ДатаНапр", DateTime.Today.ToString(@"dd.MM.yyyy"));
            DOC.Add(SodFHJ3);
            SodFHJ3.Add(SodOper);
            SodFHJ3.Add(DataNapr);

            //<Документ><СодФХЖ3><ПередатДокум>
            XElement PeredDoc = new XElement("ПередатДокум");
            XAttribute PeredDocNmOsn = new XAttribute("НаимОсн", "Универсальный передаточный документ");
            XAttribute PeredDocDataOsn = new XAttribute("ДатаОсн", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));
            XAttribute PeredDocNmrOsn = new XAttribute("НомОсн", infoCorSf[0].ToString());
            SodFHJ3.Add(PeredDoc);
            PeredDoc.Add(PeredDocNmOsn);
            PeredDoc.Add(PeredDocDataOsn);
            PeredDoc.Add(PeredDocNmrOsn);

            //<Документ><СодФХЖ3><ДокумОснКор>
            XElement PeredDocOsnKorr = new XElement("ДокумОснКор");
            XAttribute NaimOsn = new XAttribute("НаимОсн", "Иные");
            XAttribute DataOsn = new XAttribute("ДатаОсн", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));
            XAttribute DopSvedOsn = new XAttribute("ДопСвОсн", "Отсутствуют");
            SodFHJ3.Add(PeredDocOsnKorr);
            PeredDocOsnKorr.Add(NaimOsn);
            PeredDocOsnKorr.Add(DataOsn);
            PeredDocOsnKorr.Add(DopSvedOsn);

            /************************** 3 уровень. <Подписант> ************************/
            string[] infoSigner = Verifiacation.GetSignerOpt(); //0 ОБЩИЕ.ЭЦПДОЛЖНОСТЬ, 1 ОБЩИЕ.ЭЦПФАМИЛИЯ, 2 ОБЩИЕ.ЭЦПИМЯ, 3 ОБЩИЕ.ЭЦПОТЧЕСТВО, 4 ОБЩИЕ.ЭЦПДОЛЖНОБЯЗ

            XElement Podp = new XElement("Подписант");
            XAttribute obl = new XAttribute("ОблПолн", "6");
            XAttribute status = new XAttribute("Статус", "1");
            XAttribute osn = new XAttribute("ОснПолн", infoSigner[4]);
            DOC.Add(Podp);
            Podp.Add(obl);
            Podp.Add(status);
            Podp.Add(osn);

            //<Документ><Подписант><ЮЛ>
            XElement UL = new XElement("ЮЛ");
            XAttribute innUl = new XAttribute("ИННЮЛ", infoFirm[1]);
            XAttribute naimOrg = new XAttribute("НаимОрг", infoFirm[0]);
            XAttribute dolj = new XAttribute("Должн", infoSigner[0]);
            Podp.Add(UL);
            UL.Add(innUl);
            UL.Add(naimOrg);
            UL.Add(dolj);

            //<Документ><Подписант><ЮЛ><ФИО>
            XElement FIO = new XElement("ФИО");
            XAttribute famdir = new XAttribute("Фамилия", infoSigner[1]);
            XAttribute namedir = new XAttribute("Имя", infoSigner[2]);
            XAttribute otchesdir = new XAttribute("Отчество", infoSigner[3]);
            UL.Add(FIO);
            FIO.Add(famdir);
            FIO.Add(namedir);
            FIO.Add(otchesdir);

            //------сохранение документа-----------
            fileName = fileName + ".xml";
            try
            {
                xdoc.Save(pathArchiveEDI + fileName);
                try
                {
                    xdoc.Save(pathUKDEDI + fileName);
                    string message = "EDISOFT. УКД " + fileName + " создан в " + pathUKDEDI;
                    Program.WriteLine(message);
                    DispOrders.WriteProtocolEDI("УКД", fileName, infoKag[0] + " - " + infoKag[1], 0, infoGpl[0] + " - " + infoGpl[1], "УКД сформирован", DateTime.Now, Convert.ToString(CurrDataUKD[6]), "EDISOFT");
                    DispOrders.WriteEDiSentDoc("8", fileName, Convert.ToString(CurrDataUKD[3]), Convert.ToString(infoSf[0]), "123", Convert.ToString(sumWthNds_V - sumWthNds_G), Convert.ToString(CurrDataUKD[7]), 1);
                    //запись в лог о удаче
                }
                catch (Exception e)
                {
                    string message_error = "EDISOFT. Не могу создать xml файл УКД в " + pathUKDEDI + ". Нет доступа или диск переполнен.";
                    DispOrders.WriteProtocolEDI("УКД", fileName, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УКД не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUKD[6]), "EDISOFT");
                    Program.WriteLine(message_error);
                    //DispOrders.WriteErrorLog(e.Message);
                }
            }
            catch (Exception e)
            {
                string message_error = "EDISOFT. Не могу создать xml файл УКД в " + pathArchiveEDI + ". Нет доступа или диск переполнен.";
                DispOrders.WriteProtocolEDI("УКД", fileName, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УКД не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUKD[6]), "EDISOFT");
                Program.WriteLine(message_error);
                DispOrders.WriteErrorLog(e.Message);
                //запись в лог о неудаче
            }
        }
        /********************************************************** Конец УКД Лента **********************************************************/


        /*************************************************************************************************************************************/
        /********************************************************** Начало УПД X5 ************************************************************/
        /*************************************************************************************************************************************/
        public static void CreateEdiTander_UPD(List<object> CurrDataUPD, string typeFunc) //список УПД, 0 ProviderOpt, 1 ProviderZkg, 2 NastDoc_Fmt, 3 SklSf_Rcd, 4 SklSf_TpOtg, 5 SklSfA_RcdCor, 6 PrdZkg_NmrExt, 7 PrdZkg_Rcd, 8 PrdZkg_Dt ,9 SklNk_TDrvNm
        {
            //получение путей
            string pathArchiveEDI = /*"D:\\Edi\\Archive\\"; //*/ DispOrders.GetValueOption("EDI-СОФТ.АРХИВ");
            string pathUPDEDI;

            //Запрос данных СФ
            object[] infoSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataUPD[3])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO

            //запрос данных спецификации
            object[,] Item = Verifiacation.GetItemsFromSF(Convert.ToString(CurrDataUPD[3]), true); //0 BarCode_Code, 1 SklN_Rcd, 2 SklN_Cd, 3 SklN_NmAlt, 4 Кол-во, 5 Цена без НДC, 6 Цена с НДС, 7 Код ЕИ EDI, 8 ОКЕЙ, 9 Ставка, 10 'S', 11 Сумма НДС, 12 Сумма с НДС, 13 шифр ЕИ, 14 Вес

            //Запрос данных покупателя
            object[] infoKag = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(infoSf[2]), Convert.ToInt64(infoSf[3])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование
            //Запрос данных покупателя
            object[] infoGpl = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(infoSf[4]), Convert.ToInt64(infoSf[5])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование

            string codeByBuyer = Verifiacation.GetFldFromEdiExch(Convert.ToInt64(CurrDataUPD[7]), "Exch_USelCdByer"); //Код поставщика для Ашана

            //какой gln номер использовать
            bool useMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(infoSf[4]));
            string ilnFirm;

            object[] infoFirm;
            object[] infoFirmAdr;
            object[] infoFirmGrOt; //данные грузоотправителя
            object[] infoFirmAdrGrOt; //адрес грузоотправителя
            if (useMasterGLN == false)//используем данные текущего предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ИЛН");
                pathUPDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/ DispOrders.GetValueOption("EDI-СОФТ.УПД");
                infoFirm = Verifiacation.GetFirmInfo(); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO
                infoFirmAdr = Verifiacation.GetFirmAdr(); // 0 CrtAdr_StrNm+','+CrtAdr_House, 1 CrtAdr_TowNm, 2 CrtAdr_RegNm, 3 CrtAdr_Ind, 4 CrtAdr_RegCd
                infoFirmGrOt = infoFirm;
                infoFirmAdrGrOt = infoFirmAdr;

            }
            else//используем данные головного предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");
                infoFirm = Verifiacation.GetMasterFirmInfo();
                infoFirmAdr = Verifiacation.GetMasterFirmAdr();
                infoFirmGrOt = Verifiacation.GetFirmInfo(); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO
                infoFirmAdrGrOt = Verifiacation.GetFirmAdr(); // 0 CrtAdr_StrNm+','+CrtAdr_House, 1 CrtAdr_TowNm, 2 CrtAdr_RegNm, 3 CrtAdr_Ind, 4 CrtAdr_RegCd, 5 CrtAdr_StrNm, 6 CrtAdr_House 
                try
                {
                    pathUPDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/ DispOrders.GetValueOption("EDI-СОФТ.ЭКСПОРТ");
                }
                catch
                {
                    pathUPDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/ DispOrders.GetValueOption("EDI-СОФТ.УПД");
                }
            }



            string idEdo = DispOrders.GetValueOption("EDI-СОФТ.ИДЭДО");  //"2IJ"; //ИдЭДО

            string idOtpr = idEdo + ilnFirm; //ИдОтпр
            string idPol = idEdo + infoGpl[2].ToString(); //ИдПол

            string guid = Convert.ToString(Guid.NewGuid());
            string fileName = "ON_NSCHFDOPPR_" + idPol + "_" + idOtpr + "_" + DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid;//ИдФайл

            /************************** 1 уровень. <Файл> ******************************/

            XDocument xdoc = new XDocument(new XDeclaration("1.0", "", ""));

            XElement File = new XElement("Файл");
            XAttribute IdFile = new XAttribute("ИдФайл", "000000000000000000000000000000000000");
            XAttribute VersForm = new XAttribute("ВерсФорм", "5.01");
            XAttribute VersProg = new XAttribute("ВерсПрог", "Эдисофт");

            xdoc.Add(File);
            File.Add(IdFile);
            File.Add(VersForm);
            File.Add(VersProg);

            /************************** 2 уровень. <СвУчДокОбор> ************************/

            XElement ID = new XElement("СвУчДокОбор");

            XAttribute IdSender = new XAttribute("ИдОтпр", "0000000000"/*idOtpr*/);
            XAttribute IdReciever = new XAttribute("ИдПол", "0000000000"/*idPol*/);

            File.Add(ID);
            ID.Add(IdSender);
            ID.Add(IdReciever);

            //<СвУчДокОбор><СвОЭДОтпр>
            XElement InfOrg = new XElement("СвОЭДОтпр");
            string providerNm = DispOrders.GetValueOption("EDI-СОФТ.НМ");
            string providerInn = DispOrders.GetValueOption("EDI-СОФТ.ИНН");
            XAttribute NaimOrg = new XAttribute("НаимОрг", providerNm/*"Эдисофт, ООО"*/);
            XAttribute INNUL = new XAttribute("ИННЮЛ", providerInn/*"7801471082"*/);
            XAttribute IDEDO = new XAttribute("ИдЭДО", idEdo);

            ID.Add(InfOrg);
            InfOrg.Add(NaimOrg);
            InfOrg.Add(INNUL);
            InfOrg.Add(IDEDO);

            /************************** 2 уровень. <Документ> ************************/

            XElement DOC = new XElement("Документ");
            XAttribute KND = new XAttribute("КНД", "1115131");
            XAttribute Function = new XAttribute("Функция", typeFunc);
            XAttribute PoFakt = new XAttribute("ПоФактХЖ", "Документ об отгрузке товаров (выполнении работ), передаче имущественных прав (документ об оказании услуг)");
            XAttribute NaimDocOpr;
            if (typeFunc == "СЧФДОП")
                NaimDocOpr = new XAttribute("НаимДокОпр", "Счет-фактура и документ об отгрузке товаров (выполнении работ), передаче имущественных прав (документ об оказании услуг)");
            else if (typeFunc == "ДОП")
                NaimDocOpr = new XAttribute("НаимДокОпр", "Документ об отгрузке товаров (выполнении работ), передаче имущественных прав (документ об оказании услуг)");
            else
                NaimDocOpr = new XAttribute("НаимДокОпр", "");
            XAttribute DateF = new XAttribute("ДатаИнфПр", DateTime.Today.ToString(@"dd.MM.yyyy"));
            XAttribute TimeF = new XAttribute("ВремИнфПр", DateTime.Today.ToString(@"hh.mm.ss"));
            XAttribute NameOrg = new XAttribute("НаимЭконСубСост", infoFirm[0].ToString() + ", ИНН-КПП: " + infoFirm[1].ToString() + "-" + infoFirm[2].ToString());

            File.Add(DOC);
            DOC.Add(KND);
            DOC.Add(Function);
            if (typeFunc != "СЧФ")
            {
                DOC.Add(PoFakt);
                DOC.Add(NaimDocOpr);
            }
            DOC.Add(DateF);
            DOC.Add(TimeF);
            DOC.Add(NameOrg);

            //<Документ><СвСчФакт>
            XElement SVSF = new XElement("СвСчФакт");
            XAttribute NomSF = new XAttribute("НомерСчФ", infoSf[0].ToString());
            XAttribute DateSF = new XAttribute("ДатаСчФ", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));
            XAttribute Kod = new XAttribute("КодОКВ", infoSf[6].ToString());

            DOC.Add(SVSF);
            SVSF.Add(NomSF);
            SVSF.Add(DateSF);
            SVSF.Add(Kod);

            //исправление
            XElement IsprSF = new XElement("ИспрСчФ");
            SVSF.Add(IsprSF);
            //Проверяем необходимо ли отправить исправление, либо же это обычная отправка
            if (CurrDataUPD[11].ToString() == "0") //Обычная отправка
            {

                //если нет исправления
                XAttribute DfNISF = new XAttribute("ДефНомИспрСчФ", "-");
                XAttribute DfDISF = new XAttribute("ДефДатаИспрСчФ", "-");
                IsprSF.Add(DfNISF);
                IsprSF.Add(DfDISF);
            }
            else //есть исправление
            {
                XAttribute NmIsprSF = new XAttribute("НомИспрСчФ", CurrDataUPD[11].ToString());
                XAttribute DtIsprSF = new XAttribute("ДатаИспрСчФ", DateTime.Today.ToString(@"dd.MM.yyyy"));
                IsprSF.Add(NmIsprSF);
                IsprSF.Add(DtIsprSF);
            }

            //<Документ><СвСчФакт><СвПрод>            
            XElement SvProd = new XElement("СвПрод");
            //XAttribute SvProdOKPO = new XAttribute("ОКПО", infoFirm[3].ToString());
            SVSF.Add(SvProd);
            //SvProd.Add(SvProdOKPO);

            //<Документ><СвСчФакт><СвПрод><ИдСв>
            XElement SvProdIdSv = new XElement("ИдСв");
            SvProd.Add(SvProdIdSv);

            //<Документ><СвСчФакт><СвПрод><ИдСв><СвЮЛУч>
            XElement SvProdSvUluchh = new XElement("СвЮЛУч");
            XAttribute SvProdIdSvName = new XAttribute("НаимОрг", infoFirm[0].ToString());
            XAttribute SvProdIdSvINN = new XAttribute("ИННЮЛ", infoFirm[1].ToString());
            XAttribute SvProdIdSvKPP = new XAttribute("КПП", infoFirm[2].ToString());
            if (useMasterGLN)  //используем данные головного предприятия, поэтому КПП указываем свой
            {
                SvProdIdSvKPP = new XAttribute("КПП", infoFirmGrOt[2].ToString());
            }

            SvProdIdSv.Add(SvProdSvUluchh);
            SvProdSvUluchh.Add(SvProdIdSvName);
            SvProdSvUluchh.Add(SvProdIdSvINN);
            SvProdSvUluchh.Add(SvProdIdSvKPP);

            //<Документ><СвСчФакт><СвПрод><Адрес>
            XElement SvProdAdres = new XElement("Адрес");
            SvProd.Add(SvProdAdres);

            //<Документ><СвСчФакт><СвПрод><Адрес><АдресРФ>
            //Адрес
            XElement SvProdAdrRF = new XElement("АдрРФ");
            SvProdAdres.Add(SvProdAdrRF);
            if (infoFirmAdr[3].ToString() != "")
            {
                XAttribute SvProdIndex = new XAttribute("Индекс", infoFirmAdr[3].ToString());
                SvProdAdrRF.Add(SvProdIndex);
            }
            XAttribute SvProdKodReg = new XAttribute("КодРегион", infoFirmAdr[4].ToString());
            SvProdAdrRF.Add(SvProdKodReg);

            //<Документ><СвСчФакт><ГрузОт>

            XElement GruzOt = new XElement("ГрузОт");
            if (useMasterGLN)
            {
                XElement GruzOtOther = new XElement("ГрузОтпр");
                SVSF.Add(GruzOt);
                GruzOt.Add(GruzOtOther);

                XElement SvGrOtpIdSv = new XElement("ИдСв");
                GruzOtOther.Add(SvGrOtpIdSv);

                //<Документ><СвСчФакт><СвПрод><ИдСв><СвЮЛУч>
                XElement SvGrOtpSvUluchh = new XElement("СвЮЛУч");
                XAttribute SvGrOtpIdSvName = new XAttribute("НаимОрг", infoFirmGrOt[0].ToString());
                XAttribute SvGrOtpIdSvINN = new XAttribute("ИННЮЛ", infoFirmGrOt[1].ToString());
                XAttribute SvGrOtpIdSvKPP = new XAttribute("КПП", infoFirmGrOt[2].ToString());

                SvGrOtpIdSv.Add(SvGrOtpSvUluchh);
                SvGrOtpSvUluchh.Add(SvGrOtpIdSvName);
                SvGrOtpSvUluchh.Add(SvGrOtpIdSvINN);
                SvGrOtpSvUluchh.Add(SvGrOtpIdSvKPP);

                //<Документ><СвСчФакт><СвПрод><Адрес>
                XElement SvGrOtpAdres = new XElement("Адрес");
                GruzOtOther.Add(SvGrOtpAdres);

                XElement SvGrOtpAdrRF = new XElement("АдрРФ");
                SvGrOtpAdres.Add(SvGrOtpAdrRF);
                if (infoFirmAdrGrOt[3].ToString() != "")
                {
                    XAttribute SvGrOtpIndex = new XAttribute("Индекс", infoFirmAdrGrOt[3].ToString());
                    SvGrOtpAdrRF.Add(SvGrOtpIndex);
                }
                XAttribute SvGrOtpKodReg = new XAttribute("КодРегион", infoFirmAdrGrOt[4].ToString());
                SvGrOtpAdrRF.Add(SvGrOtpKodReg);
                if (infoFirmAdrGrOt[1].ToString() != "")
                {
                    XAttribute SvGrOtpGorod = new XAttribute("Город", infoFirmAdrGrOt[1].ToString());
                    SvGrOtpAdrRF.Add(SvGrOtpGorod);
                }
                if (infoFirmAdrGrOt[5].ToString() != "")
                {
                    XAttribute SvGrOtpStreet = new XAttribute("Улица", infoFirmAdrGrOt[5].ToString());
                    SvGrOtpAdrRF.Add(SvGrOtpStreet);
                }
                if (infoFirmAdrGrOt[6].ToString() != "")
                {
                    XAttribute SvGrOtpHouse = new XAttribute("Дом", infoFirmAdrGrOt[6].ToString());
                    SvGrOtpAdrRF.Add(SvGrOtpHouse);
                }

            }
            else
            {
                XElement GruzOtOnJe = new XElement("ОнЖе", "он же");
                SVSF.Add(GruzOt);
                GruzOt.Add(GruzOtOnJe);
            }

            //<Документ><СвСчФакт><ГрузПолуч>
            XElement GruzPoluch = new XElement("ГрузПолуч");
            SVSF.Add(GruzPoluch);

            //<Документ><СвСчФакт><ГрузПолуч><ИдСв>
            XElement GruzPoluchIdSv = new XElement("ИдСв");
            GruzPoluch.Add(GruzPoluchIdSv);

            //<Документ><СвСчФакт><ГрузПолуч><ИдСв><СвЮЛУч>
            XElement GruzPoluchSvUluch = new XElement("СвЮЛУч");
            XAttribute GruzPoluchName = new XAttribute("НаимОрг", infoGpl[1]);
            XAttribute GruzPoluchINN = new XAttribute("ИННЮЛ", infoGpl[3]);
            GruzPoluchIdSv.Add(GruzPoluchSvUluch);
            GruzPoluchSvUluch.Add(GruzPoluchName);
            GruzPoluchSvUluch.Add(GruzPoluchINN);
            if (infoGpl[4].ToString().Trim().Length > 0)
            {
                XAttribute GruzPoluchKPP = new XAttribute("КПП", infoGpl[4]);
                GruzPoluchSvUluch.Add(GruzPoluchKPP);
            }


            //<Документ><СвСчФакт><ГрузПолуч><Адрес>
            XElement GruzPoluchAdres = new XElement("Адрес");
            GruzPoluch.Add(GruzPoluchAdres);

            //<Документ><СвСчФакт><ГрузПолуч><Адрес><АдресРФ>
            XElement GruzPoluchAdrRF = new XElement("АдрРФ");
            GruzPoluchAdres.Add(GruzPoluchAdrRF);
            if (infoGpl[7].ToString().Trim().Length > 0)
            {
                XAttribute GruzPoluchIndex = new XAttribute("Индекс", infoGpl[7]);
                GruzPoluchAdrRF.Add(GruzPoluchIndex);
            }
            // if (infoGpl[8].ToString().Length > 0) Атрибут обязательный
            // {
            XAttribute GruzPoluchKodReg = new XAttribute("КодРегион", infoGpl[8]);
            GruzPoluchAdrRF.Add(GruzPoluchKodReg);
            // }
            if (infoGpl[9].ToString().Length > 0)
            {
                XAttribute GruzPoluchCity = new XAttribute("Город", infoGpl[9]);
                GruzPoluchAdrRF.Add(GruzPoluchCity);
            }
            if (infoGpl[10].ToString().Length > 0)
            {
                XAttribute GruzPoluchStreet = new XAttribute("Улица", infoGpl[10]);
                GruzPoluchAdrRF.Add(GruzPoluchStreet);
            }
            if (infoGpl[11].ToString().Length > 0)
            {
                XAttribute GruzPoluchHouse = new XAttribute("Дом", infoGpl[11]);
                GruzPoluchAdrRF.Add(GruzPoluchHouse);
            }


            //<Документ><СвСчФакт><СвПокуп>
            XElement SvPokup = new XElement("СвПокуп");
            SVSF.Add(SvPokup);

            //<Документ><СвСчФакт><СвПокуп><ИдСв>
            XElement SvPokupIdSv = new XElement("ИдСв");
            SvPokup.Add(SvPokupIdSv);

            //<Документ><СвСчФакт><СвПокуп><ИдСв><СвЮЛУч>             
            XElement SvPokupSvUluch = new XElement("СвЮЛУч");
            XAttribute SvPokupName = new XAttribute("НаимОрг", infoKag[1]);
            XAttribute SvPokupINN = new XAttribute("ИННЮЛ", infoKag[3]);
            XAttribute SvPokupKPP = new XAttribute("КПП", infoKag[4]);
            SvPokupIdSv.Add(SvPokupSvUluch);
            SvPokupSvUluch.Add(SvPokupName);
            SvPokupSvUluch.Add(SvPokupINN);
            SvPokupSvUluch.Add(SvPokupKPP);

            //<Документ><СвСчФакт><СвПокуп><Адрес>
            XElement SvPokupAdres = new XElement("Адрес");
            SvPokup.Add(SvPokupAdres);

            //<Документ><СвСчФакт><СвПокуп><Адрес><АдресРФ>
            XElement SvPokupAdrRF = new XElement("АдрРФ");
            SvPokupAdres.Add(SvPokupAdrRF);
            if (infoKag[7].ToString().Trim().Length > 0)
            {
                XAttribute SvPokupIndex = new XAttribute("Индекс", infoKag[7]);
                SvPokupAdrRF.Add(SvPokupIndex);
            }
            //    if (infoGpl[8].ToString().Length > 0)  атрибут обязательный
            //    {
            XAttribute SvPokupKodReg = new XAttribute("КодРегион", infoKag[8]);
            SvPokupAdrRF.Add(SvPokupKodReg);
            //    }
            if (infoKag[9].ToString().Length > 0)
            {
                XAttribute SvPokupCity = new XAttribute("Город", infoKag[9]);
                SvPokupAdrRF.Add(SvPokupCity);
            }
            if (infoKag[10].ToString().Length > 0)
            {
                XAttribute SvPokupStreet = new XAttribute("Улица", infoKag[10]);
                SvPokupAdrRF.Add(SvPokupStreet);
            }
            if (infoKag[11].ToString().Length > 0)
            {
                XAttribute SvPokupHouse = new XAttribute("Дом", infoKag[11]);
                SvPokupAdrRF.Add(SvPokupHouse);
            }


            //<Документ><СвСчФакт><ИнфПолФХЖ1>
            XElement DopSvFHJ1 = new XElement("ДопСвФХЖ1");
            XAttribute NaimOKV = new XAttribute("НаимОКВ", "643");
            SVSF.Add(DopSvFHJ1);
            DopSvFHJ1.Add(NaimOKV);

            //<Документ><СвСчФакт><ИнфПолФХЖ1>
            XElement InfPolFHJ1 = new XElement("ИнфПолФХЖ1");
            SVSF.Add(InfPolFHJ1);

            /*//<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf1 = new XElement("ТекстИнф");
            XAttribute TxtInf1Identif = new XAttribute("Идентиф", "номер_заказа");
            XAttribute TxtInf1Znachen = new XAttribute("Значен", CurrDataUPD[6]);
            InfPolFHJ1.Add(TxtInf1);
            TxtInf1.Add(TxtInf1Identif);
            TxtInf1.Add(TxtInf1Znachen);*/

            //<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf2 = new XElement("ТекстИнф");
            XAttribute TxtInf2Identif = new XAttribute("Идентиф", "отправитель");
            XAttribute TxtInf2Znachen = new XAttribute("Значен", ilnFirm);
            InfPolFHJ1.Add(TxtInf2);
            TxtInf2.Add(TxtInf2Identif);
            TxtInf2.Add(TxtInf2Znachen);

            //<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf3 = new XElement("ТекстИнф");
            XAttribute TxtInf3Identif = new XAttribute("Идентиф", "получатель");
            XAttribute TxtInf3Znachen = new XAttribute("Значен", infoKag[16]);
            InfPolFHJ1.Add(TxtInf3);
            TxtInf3.Add(TxtInf3Identif);
            TxtInf3.Add(TxtInf3Znachen);

            /*//<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf4 = new XElement("ТекстИнф");
            XAttribute TxtInf4Identif = new XAttribute("Идентиф", "код_поставщика");
            XAttribute TxtInf4Znachen = new XAttribute("Значен", codeByBuyer);
            InfPolFHJ1.Add(TxtInf4);
            TxtInf4.Add(TxtInf4Identif);
            TxtInf4.Add(TxtInf4Znachen);*/

            //<Документ><СвСчФакт><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf5 = new XElement("ТекстИнф");
            XAttribute TxtInf5Identif = new XAttribute("Идентиф", "грузополучатель");
            XAttribute TxtInf5Znachen = new XAttribute("Значен", infoGpl[2]);
            InfPolFHJ1.Add(TxtInf5);
            TxtInf5.Add(TxtInf5Identif);
            TxtInf5.Add(TxtInf5Znachen);

            //номер заказа
            XElement TxtInf6 = new XElement("ТекстИнф");
            XAttribute TxtInf6Identif = new XAttribute("Идентиф", "номер_заказа");
            XAttribute TxtInf6Znachen = new XAttribute("Значен", CurrDataUPD[6]);
            InfPolFHJ1.Add(TxtInf6);
            TxtInf6.Add(TxtInf6Identif);
            TxtInf6.Add(TxtInf6Znachen);

            //дата заказа
            XElement TxtInf7 = new XElement("ТекстИнф");
            XAttribute TxtInf7Identif = new XAttribute("Идентиф", "дата_заказа");
            XAttribute TxtInf7Znachen = new XAttribute("Значен", Convert.ToDateTime(CurrDataUPD[8]).ToString(@"dd.MM.yyyy"));
            InfPolFHJ1.Add(TxtInf7);
            TxtInf7.Add(TxtInf7Identif);
            TxtInf7.Add(TxtInf7Znachen);

            //номер накладной
            XElement TxtInf8 = new XElement("ТекстИнф");
            XAttribute TxtInf8Identif = new XAttribute("Идентиф", "номер_накладной");
            XAttribute TxtInf8Znachen = new XAttribute("Значен", CurrDataUPD[13]);
            InfPolFHJ1.Add(TxtInf8);
            TxtInf8.Add(TxtInf8Identif);
            TxtInf8.Add(TxtInf8Znachen);

            //дата поставки
            XElement TxtInf9 = new XElement("ТекстИнф");
            XAttribute TxtInf9Identif = new XAttribute("Идентиф", "дата_поставки");
            XAttribute TxtInf9Znachen = new XAttribute("Значен", Convert.ToDateTime(CurrDataUPD[14]).ToString(@"dd.MM.yyyy"));
            InfPolFHJ1.Add(TxtInf9);
            TxtInf9.Add(TxtInf9Identif);
            TxtInf9.Add(TxtInf9Znachen);

            //дата накладной
            XElement TxtInf10 = new XElement("ТекстИнф");
            XAttribute TxtInf10Identif = new XAttribute("Идентиф", "дата_накладной");
            XAttribute TxtInf10Znachen = new XAttribute("Значен", Convert.ToDateTime(CurrDataUPD[12]).ToString(@"dd.MM.yyyy"));
            InfPolFHJ1.Add(TxtInf10);
            TxtInf10.Add(TxtInf10Identif);
            TxtInf10.Add(TxtInf10Znachen);

            /************************** 3 уровень. <ТаблСчФакт> ************************/
            XElement TabSF = new XElement("ТаблСчФакт");
            DOC.Add(TabSF);

            decimal sumWthNds = 0;
            decimal sumNds = 0;
            decimal sumWeight = 0;

            for (int i = 0; i < Item.GetLongLength(0); i++) //Item[] //0 BarCode_Code, 1 SklN_Rcd, 2 SklN_Cd, 3 SklN_NmAlt, 4 Кол-во, 5 Цена без НДC, 6 Цена с НДС, 7 Код ЕИ EDI, 8 ОКЕЙ, 9 Ставка, 10 'S', 11 Сумма НДС, 12 Сумма с НДС, 13 шифр ЕИ, 14 Вес
            {
                sumWthNds = sumWthNds + Convert.ToDecimal(Item[i, 12]);
                sumNds = sumNds + Convert.ToDecimal(Item[i, 11]);
                sumWeight = sumWeight + Convert.ToDecimal(Item[i, 14]);
                //<Документ><ТаблСчФакт><СведТов>
                XElement SvedTov = new XElement("СведТов");
                XAttribute NomStr = new XAttribute("НомСтр", Convert.ToString(i + 1));
                XAttribute NaimTov = new XAttribute("НаимТов", Item[i, 3]);
                XAttribute OKEI = new XAttribute("ОКЕИ_Тов", Item[i, 8]);
                XAttribute KolTov = new XAttribute("КолТов", Math.Round(Convert.ToDecimal(Item[i, 4]), 2));
                XAttribute CenaTov = new XAttribute("ЦенаТов", Item[i, 5]);
                XAttribute BezNDS = new XAttribute("СтТовБезНДС", Math.Round(Convert.ToDecimal(Item[i, 12]) - Convert.ToDecimal(Item[i, 11]), 2));
                XAttribute NalSt = new XAttribute("НалСт", Convert.ToString(Item[i, 9]));
                XAttribute sNDS = new XAttribute("СтТовУчНал", Math.Round(Convert.ToDecimal(Item[i, 12]), 2));
                TabSF.Add(SvedTov);
                SvedTov.Add(NomStr);
                SvedTov.Add(NaimTov);
                SvedTov.Add(OKEI);
                SvedTov.Add(KolTov);
                SvedTov.Add(CenaTov);
                SvedTov.Add(BezNDS);
                SvedTov.Add(NalSt);
                SvedTov.Add(sNDS);

                //<Документ><ТаблСчФакт><СведТов><Акциз>
                XElement Akciz = new XElement("Акциз");
                XElement bezAkciz = new XElement("БезАкциз", "без акциза");
                SvedTov.Add(Akciz);
                Akciz.Add(bezAkciz);

                //<Документ><ТаблСчФакт><СведТов><СумНал>
                XElement SumNal = new XElement("СумНал");
                SvedTov.Add(SumNal);
                if (Convert.ToString(Item[i, 9]) == "без НДС")
                {
                    //<Документ><ТаблСчФакт><СведТов><СумНал><БезНДС>
                    XElement BezNds = new XElement("БезНДС", "без НДС");
                    SumNal.Add(BezNds);
                }
                else
                {
                    //<Документ><ТаблСчФакт><СведТов><СумНал><СумНал>
                    XElement SumNalSum = new XElement("СумНал", Math.Round(Convert.ToDecimal(Item[i, 11]), 2));
                    SumNal.Add(SumNalSum);
                }

                //<Документ><ТаблСчФакт><СведТов><СвТД>
                XElement SvTD = new XElement("СвТД");
                XAttribute DfKP = new XAttribute("ДефКодПроисх", "-");
                SvedTov.Add(SvTD);
                SvTD.Add(DfKP);

                //<Документ><ТаблСчФакт><СведТов><ДопСведТов>
                XElement DopSvedTov = new XElement("ДопСведТов");
                XAttribute PrTovRav = new XAttribute("ПрТовРаб", "1");
                XAttribute NaimEdIzm = new XAttribute("НаимЕдИзм", Item[i, 13]);
                SvedTov.Add(DopSvedTov);
                DopSvedTov.Add(PrTovRav);
                DopSvedTov.Add(NaimEdIzm);

                //<Документ><ТаблСчФакт><СведТов><ИнфПолФХЖ2>
                XElement InfPolFHJ22 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf2Identif = new XAttribute("Идентиф", "штрихкод");
                XAttribute ItmTxtInf2Znachen = new XAttribute("Значен", Item[i, 0]);
                SvedTov.Add(InfPolFHJ22);
                InfPolFHJ22.Add(ItmTxtInf2Identif);
                InfPolFHJ22.Add(ItmTxtInf2Znachen);
            }

            //<Документ><ТаблСчФакт><ВсегоОпл>
            XElement VsegoOpl = new XElement("ВсегоОпл");
            XAttribute VsegoBezNds = new XAttribute("СтТовБезНДСВсего", Math.Round(sumWthNds - sumNds, 2));
            XAttribute VsegoSNDS = new XAttribute("СтТовУчНалВсего", Math.Round(sumWthNds, 2));
            TabSF.Add(VsegoOpl);
            VsegoOpl.Add(VsegoBezNds);
            VsegoOpl.Add(VsegoSNDS);

            //<Документ><ТаблСчФакт><ВсегоОпл><СумНалВсего>
            XElement SumNalVsego = new XElement("СумНалВсего");
            VsegoOpl.Add(SumNalVsego);

            if (Convert.ToString(Item[0, 9]) == "без НДС")
            {
                //<Документ><ТаблСчФакт><ВсегоОпл><СумНалВсего><БезНДС>
                XElement SumNalVsegoSumNal = new XElement("БезНДС", "без НДС");
                SumNalVsego.Add(SumNalVsegoSumNal);
            }
            else
            {
                //<Документ><ТаблСчФакт><ВсегоОпл><СумНалВсего><СумНал>
                XElement SumNalVsegoSumNal = new XElement("СумНал", Math.Round(sumNds, 2));
                SumNalVsego.Add(SumNalVsegoSumNal);
            }

            //<Документ><ТаблСчФакт><ВсегоОпл><НеттоВс>
            XElement NettoVes = new XElement("КолНеттоВс", Math.Round(sumWeight, 3));
            VsegoOpl.Add(NettoVes);

            /************************** 3 уровень. <СвПродПер> ************************/
            XElement ProdPer = new XElement("СвПродПер");
            DOC.Add(ProdPer);

            //<Документ><СвПродПер><СвПер>
            XElement SvPer = new XElement("СвПер");
            XAttribute SodOper = new XAttribute("СодОпер", "Товары переданы");
            XAttribute DtOper = new XAttribute("ДатаПер", Convert.ToDateTime(CurrDataUPD[14]).ToString(@"dd.MM.yyyy"));
            ProdPer.Add(SvPer);
            SvPer.Add(SodOper);
            SvPer.Add(DtOper);

            //<Документ><СвПродПер><СвПер><ОснПер>
            XElement OsnPer = new XElement("ОснПер");
            XAttribute NaimOsn = new XAttribute("НаимОсн", "Заказ");
            XAttribute NomOsn = new XAttribute("НомОсн", CurrDataUPD[6]);
            XAttribute DataOsn = new XAttribute("ДатаОсн", Convert.ToDateTime(CurrDataUPD[8]).ToString(@"dd.MM.yyyy"));
            SvPer.Add(OsnPer);
            OsnPer.Add(NaimOsn);
            OsnPer.Add(NomOsn);
            OsnPer.Add(DataOsn);

            //<Документ><СвПродПер><СвПер><СвЛицПер>
            XElement SvLicPer = new XElement("СвЛицПер");
            SvPer.Add(SvLicPer);

            //<Документ><СвПродПер><СвПер><СвЛицПер><РабОргПрод>
            XElement RabOrgProd = new XElement("РабОргПрод");
            XAttribute Doljnost = new XAttribute("Должность", "Водитель");
            SvLicPer.Add(RabOrgProd);
            RabOrgProd.Add(Doljnost);

            //<Документ><СвПродПер><СвПер><СвЛицПер><РабОргПрод><ФИО>
            XElement pFIO = new XElement("ФИО");

            string sFIO = Convert.ToString(CurrDataUPD[9]);
            int len = sFIO.Length;
            string sF;
            string sI;
            string sO;
            int p1 = sFIO.IndexOf(" ");
            int p2 = sFIO.LastIndexOf(" ");
            if (len > 0)
            {
                sF = sFIO.Remove(p1);
                sI = sFIO.Substring(p1, (p2 - p1));
                sO = sFIO.Substring(p2, len - p2);
                if (sF.Length <= 0) sF = "НеУказано";
                if (sI.Length <= 0) sF = "НеУказано";
                if (sO.Length <= 0) sF = "НеУказано";
            }
            else
            {
                sF = "НеУказано";
                sI = "НеУказано";
                sO = "НеУказано";
            }

            XAttribute pFIOF = new XAttribute("Фамилия", sF.Trim());
            XAttribute pFIOI = new XAttribute("Имя", sI.Trim());
            XAttribute pFIOO = new XAttribute("Отчество", sO.Trim());

            RabOrgProd.Add(pFIO);
            pFIO.Add(pFIOF);
            pFIO.Add(pFIOI);
            pFIO.Add(pFIOO);

            /************************** 3 уровень. <Подписант> ************************/
            string[] infoSigner = Verifiacation.GetSignerOpt(); //0 ОБЩИЕ.ЭЦПДОЛЖНОСТЬ, 1 ОБЩИЕ.ЭЦПФАМИЛИЯ, 2 ОБЩИЕ.ЭЦПИМЯ, 3 ОБЩИЕ.ЭЦПОТЧЕСТВО, 4 ОБЩИЕ.ЭЦПДОЛЖНОБЯЗ

            XElement Podp = new XElement("Подписант");
            XAttribute obl = new XAttribute("ОблПолн", "6");
            XAttribute status = new XAttribute("Статус", "1");
            XAttribute osn = new XAttribute("ОснПолн", infoSigner[4]);
            DOC.Add(Podp);
            Podp.Add(obl);
            Podp.Add(status);
            Podp.Add(osn);

            //<Документ><Подписант><ЮЛ>
            XElement UL = new XElement("ЮЛ");
            XAttribute innUl = new XAttribute("ИННЮЛ", infoFirm[1]);
            XAttribute naimOrg = new XAttribute("НаимОрг", infoFirm[0]);
            XAttribute dolj = new XAttribute("Должн", infoSigner[0]);
            Podp.Add(UL);
            UL.Add(innUl);
            UL.Add(naimOrg);
            UL.Add(dolj);

            //<Документ><Подписант><ЮЛ><ФИО>
            XElement FIO = new XElement("ФИО");
            XAttribute famdir = new XAttribute("Фамилия", infoSigner[1]);
            XAttribute namedir = new XAttribute("Имя", infoSigner[2]);
            XAttribute otchesdir = new XAttribute("Отчество", infoSigner[3]);
            UL.Add(FIO);
            FIO.Add(famdir);
            FIO.Add(namedir);
            FIO.Add(otchesdir);


            //------сохранение документа-----------
            fileName = fileName + ".xml";
            try
            {
                xdoc.Save(pathArchiveEDI + fileName);
                try
                {
                    xdoc.Save(pathUPDEDI + fileName);
                    /*//Load the XmlSchemaSet.
                    XmlSchemaSet schemaSet = new XmlSchemaSet();
                    schemaSet.Add("urn:bookstore-schema", "ON_SCHFDOPPR_1_995_01_05_01_02.xsd");

                    //Validate the file using the schema stored in the schema set.
                    //Any elements belonging to the namespace "urn:cd-schema" generate
                    //a warning because there is no schema matching that namespace.
                    Validate(pathUPDEDI + fileName, schemaSet);
                    Console.ReadLine();*/
                    string message = "EDISOFT. УПД " + typeFunc + " " + fileName + " создан в " + pathUPDEDI;
                    Program.WriteLine(message);
                    DispOrders.WriteProtocolEDI("УПД " + typeFunc, fileName, infoKag[0] + " - " + infoKag[1], 0, infoGpl[0] + " - " + infoGpl[1], "УПД " + typeFunc + " сформирован", DateTime.Now, Convert.ToString(CurrDataUPD[6]), "EDISOFT");
                    if (typeFunc == "ДОП")
                        DispOrders.WriteEDiSentDoc("10", fileName, Convert.ToString(CurrDataUPD[3]), Convert.ToString(infoSf[0]), "123", Convert.ToString(sumWthNds), Convert.ToString(CurrDataUPD[7]), 1, CurrDataUPD[11].ToString());
                    else
                        DispOrders.WriteEDiSentDoc("8", fileName, Convert.ToString(CurrDataUPD[3]), Convert.ToString(infoSf[0]), "123", Convert.ToString(sumWthNds), Convert.ToString(CurrDataUPD[7]), 1, CurrDataUPD[11].ToString());

                }
                catch (Exception e)
                {
                    string message_error = "EDISOFT. Не могу создать xml файл УПД " + typeFunc + " в " + pathUPDEDI + ". Нет доступа или диск переполнен.";
                    DispOrders.WriteProtocolEDI("УПД " + typeFunc, fileName, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УПД " + typeFunc + " не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUPD[6]), "EDISOFT");
                    Program.WriteLine(message_error);
                    DispOrders.WriteErrorLog(e.Message);
                }
            }
            catch (Exception e)
            {
                string message_error = "EDISOFT. Не могу создать xml файл УПД " + typeFunc + " в " + pathArchiveEDI + ". Нет доступа или диск переполнен.";
                DispOrders.WriteProtocolEDI("УПД " + typeFunc, fileName, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УПД не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUPD[6]), "EDISOFT");
                Program.WriteLine(message_error);
                DispOrders.WriteErrorLog(e.Message);
                //запись в лог о неудаче
            }
        }

        /*************************************************************************************************************************************/
        /********************************************************** Начало УКД Tander ************************************************************/
        /*************************************************************************************************************************************/
        public static void CreateEdiTanderUKD(List<object> CurrDataUKD) //список УКД, 0 ProviderOpt, 1 ProviderZkg, 2 NastDoc_Fmt, 3 SklSf_Rcd, 4 SklSf_TpOtg, 5 SklSfA_RcdCor, 6 PrdZkg_NmrExt, 7 PrdZkg_Rcd, 8 PrdZkg_Dt ,9 SklNk_TDrvNm
        {
            //получение путей
            string pathArchiveEDI = /*"D:\\Edi\\Archive\\"; //*/DispOrders.GetValueOption("EDI-СОФТ.АРХИВ");
            string pathUKDEDI;

            //Запрос данных КорректировочнойСФ
            object[] infoSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataUKD[3])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO
            //Запрос данных Корректируемой (отгрузочной) СФ
            object[] infoCorSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataUKD[5])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO
            //Запрос предыдущих Корректировочных СФ корректируемые текущей корректировочной СФ
            string InfoPrevSf = Verifiacation.GetPrevSfToKSF(Convert.ToString(CurrDataUKD[3]), Convert.ToString(CurrDataUKD[5]));

            //запрос данных спецификации
            object[,] Item = Verifiacation.GetItemsFromKSF(Convert.ToString(CurrDataUKD[3]), Convert.ToString(CurrDataUKD[5]), true); //0 BarCode_Code, 1 SklN_Rcd, 2 SklN_Cd, 3 SklN_NmAlt, 4 Кол-во, 5 Цена без НДC, 6 Цена с НДС, 7 Код ЕИ EDI, 8 ОКЕЙ, 9 Ставка, 10 'S', 11 Сумма НДС, 12 Сумма с НДС, 13 шифр ЕИ, 14 Вес

            //Запрос данных покупателя
            object[] infoKag = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(infoSf[2]), Convert.ToInt64(infoSf[3])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование
            //Запрос данных покупателя
            object[] infoGpl = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(infoSf[4]), Convert.ToInt64(infoSf[5])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование

            string codeByBuyer = Verifiacation.GetFldFromEdiExch(Convert.ToInt64(CurrDataUKD[7]), "Exch_USelCdByer"); //Код поставщика для Ашана

            //какой gln номер использовать
            bool useMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(infoSf[4]));
            string ilnFirm;

            object[] infoFirm;
            object[] infoFirmAdr;
            object[] infoFirmGrOt; //грузоотправитель
            object[] infoFirmAdrGrOt;  //адрес грузоотправителя
            if (useMasterGLN == false)//используем данные текущего предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ИЛН");
                pathUKDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/DispOrders.GetValueOption("EDI-СОФТ.УКД");
                infoFirm = Verifiacation.GetFirmInfo(); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO
                infoFirmAdr = Verifiacation.GetFirmAdr(); // 0 CrtAdr_StrNm+','+CrtAdr_House, 1 CrtAdr_TowNm, 2 CrtAdr_RegNm, 3 CrtAdr_Ind, 4 CrtAdr_RegCd
                infoFirmGrOt = infoFirm;
                infoFirmAdrGrOt = infoFirmAdr;

            }
            else//используем данные головного предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");
                infoFirm = Verifiacation.GetMasterFirmInfo();
                infoFirmAdr = Verifiacation.GetMasterFirmAdr();
                infoFirmGrOt = Verifiacation.GetFirmInfo(); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO
                infoFirmAdrGrOt = Verifiacation.GetFirmAdr(); // 0 CrtAdr_StrNm+','+CrtAdr_House, 1 CrtAdr_TowNm, 2 CrtAdr_RegNm, 3 CrtAdr_Ind, 4 CrtAdr_RegCd, 5 CrtAdr_StrNm, 6 CrtAdr_House
                try
                {
                    pathUKDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/DispOrders.GetValueOption("EDI-СОФТ.ЭКСПОРТ");
                }
                catch
                {
                    pathUKDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/DispOrders.GetValueOption("EDI-СОФТ.УКД");
                }
            }



            string idEdo = DispOrders.GetValueOption("EDI-СОФТ.ИДЭДО");  //"2IJ"; //ИдЭДО

            string idOtpr = idEdo + ilnFirm; //ИдОтпр
            string idPol = idEdo + infoGpl[2].ToString(); //ИдПол

            string guid = Convert.ToString(Guid.NewGuid());
            string fileName = "ON_NKORSCHFDOPPR_" + idPol + "_" + idOtpr + "_" + DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid;//ИдФайл


            /************************** 1 уровень. <Файл> ******************************/

            XDocument xdoc = new XDocument(new XDeclaration("1.0", "", ""));

            XElement File = new XElement("Файл");
            XAttribute IdFile = new XAttribute("ИдФайл", "000000000000000000000000000000000000"/*fileName*/);
            XAttribute VersForm = new XAttribute("ВерсФорм", "5.01");
            XAttribute VersProg = new XAttribute("ВерсПрог", "Edisoft");

            xdoc.Add(File);
            File.Add(IdFile);
            File.Add(VersForm);
            File.Add(VersProg);

            /************************** 2 уровень. <СвУчДокОбор> ************************/

            XElement ID = new XElement("СвУчДокОбор");

            XAttribute IdSender = new XAttribute("ИдОтпр", "0000000000"/*idOtpr*/);
            XAttribute IdReciever = new XAttribute("ИдПол", "0000000000"/*idPol*/);

            File.Add(ID);
            ID.Add(IdSender);
            ID.Add(IdReciever);

            //<СвУчДокОбор><СвОЭДОтпр>
            XElement InfOrg = new XElement("СвОЭДОтпр");
            string providerNm = DispOrders.GetValueOption("EDI-СОФТ.НМ");
            string providerInn = DispOrders.GetValueOption("EDI-СОФТ.ИНН");
            XAttribute NaimOrg = new XAttribute("НаимОрг", providerNm/*"Эдисофт, ООО"*/);
            XAttribute INNUL = new XAttribute("ИННЮЛ", providerInn/*"7801471082"*/);
            XAttribute IDEDO = new XAttribute("ИдЭДО", idEdo);

            ID.Add(InfOrg);
            InfOrg.Add(NaimOrg);
            InfOrg.Add(INNUL);
            InfOrg.Add(IDEDO);

            /************************** 2 уровень. <Документ> ************************/

            XElement DOC = new XElement("Документ");
            XAttribute KND = new XAttribute("КНД", "1115133");
            XAttribute Function = new XAttribute("Функция", "КСЧФДИС");
            XAttribute PoFakt = new XAttribute("ПоФактХЖ", "Документ, подтверждающий согласие (факт уведомления) покупателя на изменение стоимости отгруженных товаров (выполненных работ, оказанных услуг), переданных имущественных прав");
            XAttribute NaimDocOpr = new XAttribute("НаимДокОпр", "Документ, подтверждающий согласие (факт уведомления) покупателя на изменение стоимости отгруженных товаров (выполненных работ, оказанных услуг), переданных имущественных прав");
            XAttribute DateF = new XAttribute("ДатаИнфПр", DateTime.Today.ToString(@"dd.MM.yyyy"));
            XAttribute TimeF = new XAttribute("ВремИнфПр", DateTime.Today.ToString(@"hh.mm.ss"));
            XAttribute NameOrg = new XAttribute("НаимЭконСубСост", infoFirm[0].ToString() /*+ ", ИНН-КПП: " + infoFirm[1].ToString() + "-" + infoFirm[2].ToString()*/);

            File.Add(DOC);
            DOC.Add(KND);
            DOC.Add(Function);
            DOC.Add(PoFakt);
            DOC.Add(NaimDocOpr);
            DOC.Add(DateF);
            DOC.Add(TimeF);
            DOC.Add(NameOrg);

            //<Документ><СвКСчФ>
            XElement SVSF = new XElement("СвКСчФ");
            XAttribute NomSF = new XAttribute("НомерКСчФ", infoSf[0].ToString());
            XAttribute DateSF = new XAttribute("ДатаКСчФ", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));
            XAttribute Kod = new XAttribute("КодОКВ", infoSf[6].ToString());

            DOC.Add(SVSF);
            SVSF.Add(NomSF);
            SVSF.Add(DateSF);
            SVSF.Add(Kod);

            //<Документ><СвКСчФ><СчФ>
            XElement SchF = new XElement("СчФ");
            XAttribute NomSchF = new XAttribute("НомерСчФ", infoCorSf[0].ToString());
            XAttribute DateSchF = new XAttribute("ДатаСчФ", Convert.ToDateTime(infoCorSf[1]).ToString(@"dd.MM.yyyy"));
            SVSF.Add(SchF);
            SchF.Add(NomSchF);
            SchF.Add(DateSchF);


            //<Документ><<СвКСчФ>><СвПрод>
            XElement SvProd = new XElement("СвПрод");
            //XAttribute SvProdOKPO = new XAttribute("ОКПО", infoFirm[3].ToString());
            SVSF.Add(SvProd);
            //SvProd.Add(SvProdOKPO);

            //<Документ><<СвКСчФ>><СвПрод><ИдСв>
            XElement SvProdIdSv = new XElement("ИдСв");
            SvProd.Add(SvProdIdSv);

            //<Документ><<СвКСчФ>><СвПрод><ИдСв><СвЮЛУч>
            XElement SvProdSvUluchh = new XElement("СвЮЛУч");
            XAttribute SvProdIdSvName = new XAttribute("НаимОрг", infoFirm[0].ToString());
            XAttribute SvProdIdSvINN = new XAttribute("ИННЮЛ", infoFirm[1].ToString());
            XAttribute SvProdIdSvKPP = new XAttribute("КПП", infoFirm[2].ToString());
            if (useMasterGLN)
                SvProdIdSvKPP = new XAttribute("КПП", infoFirmGrOt[2].ToString());

            SvProdIdSv.Add(SvProdSvUluchh);
            SvProdSvUluchh.Add(SvProdIdSvName);
            SvProdSvUluchh.Add(SvProdIdSvINN);
            SvProdSvUluchh.Add(SvProdIdSvKPP);

            //<Документ><<СвКСчФ>><СвПрод><Адрес>
            XElement SvProdAdres = new XElement("Адрес");
            SvProd.Add(SvProdAdres);

            //<Документ><<СвКСчФ>><СвПрод><Адрес><АдресРФ>
            //Адрес
            XElement SvProdAdrRF = new XElement("АдрРФ");
            SvProdAdres.Add(SvProdAdrRF);
            if (infoFirmAdr[3].ToString() != "")
            {
                XAttribute SvProdIndex = new XAttribute("Индекс", infoFirmAdr[3].ToString());
                SvProdAdrRF.Add(SvProdIndex);
            }
            //        if (infoFirmAdr[4].ToString() != "")
            //        {
            XAttribute SvProdKodReg = new XAttribute("КодРегион", infoFirmAdr[4].ToString());
            SvProdAdrRF.Add(SvProdKodReg);
            //        }

            /*//<Документ><<СвКСчФ>><ГрузОт>
            XElement GruzOt = new XElement("ГрузОт");
            XElement GruzOtOnJe = new XElement("ОнЖе", "он же");
            SVSF.Add(GruzOt);
            GruzOt.Add(GruzOtOnJe);

            //<Документ><<СвКСчФ>><ГрузПолуч>
            XElement GruzPoluch = new XElement("ГрузПолуч");
            SVSF.Add(GruzPoluch);

            //<Документ><<СвКСчФ>><ГрузПолуч><ИдСв>
            XElement GruzPoluchIdSv = new XElement("ИдСв");
            GruzPoluch.Add(GruzPoluchIdSv);

            //<Документ><<СвКСчФ>><ГрузПолуч><ИдСв><СвЮЛУч>
            XElement GruzPoluchSvUluch = new XElement("СвЮЛУч");
            XAttribute GruzPoluchName = new XAttribute("НаимОрг", infoGpl[1]);
            XAttribute GruzPoluchINN = new XAttribute("ИННЮЛ", infoGpl[3]);
            XAttribute GruzPoluchKPP = new XAttribute("КПП", infoGpl[4]);

            GruzPoluchIdSv.Add(GruzPoluchSvUluch);
            GruzPoluchSvUluch.Add(GruzPoluchName);
            GruzPoluchSvUluch.Add(GruzPoluchINN);
            GruzPoluchSvUluch.Add(GruzPoluchKPP);

            //<Документ><<СвКСчФ>><ГрузПолуч><Адрес>
            XElement GruzPoluchAdres = new XElement("Адрес");
            GruzPoluch.Add(GruzPoluchAdres);

            //<Документ><<СвКСчФ>><ГрузПолуч><Адрес><АдресРФ>
            XElement GruzPoluchAdrRF = new XElement("АдрРФ");
            XAttribute GruzPoluchIndex = new XAttribute("Индекс", infoGpl[7]);
            XAttribute GruzPoluchKodReg = new XAttribute("КодРегион", infoGpl[8]);
            GruzPoluchAdres.Add(GruzPoluchAdrRF);
            GruzPoluchAdrRF.Add(GruzPoluchIndex);
            GruzPoluchAdrRF.Add(GruzPoluchKodReg);*/

            //<Документ><<СвКСчФ>><СвПокуп>
            XElement SvPokup = new XElement("СвПокуп");
            SVSF.Add(SvPokup);

            //<Документ><<СвКСчФ>><СвПокуп><ИдСв>
            XElement SvPokupIdSv = new XElement("ИдСв");
            SvPokup.Add(SvPokupIdSv);

            //<Документ><СвСчФакт><СвПокуп><ИдСв><СвЮЛУч>             
            XElement SvPokupSvUluch = new XElement("СвЮЛУч");
            XAttribute SvPokupName = new XAttribute("НаимОрг", infoKag[1]);
            XAttribute SvPokupINN = new XAttribute("ИННЮЛ", infoKag[3]);
            XAttribute SvPokupKPP = new XAttribute("КПП", infoKag[4]);
            SvPokupIdSv.Add(SvPokupSvUluch);
            SvPokupSvUluch.Add(SvPokupName);
            SvPokupSvUluch.Add(SvPokupINN);
            SvPokupSvUluch.Add(SvPokupKPP);

            //<Документ><<СвКСчФ>><СвПокуп><Адрес>
            XElement SvPokupAdres = new XElement("Адрес");
            SvPokup.Add(SvPokupAdres);

            //<Документ><<СвКСчФ>><СвПокуп><Адрес><АдресРФ>
            XElement SvPokupAdrRF = new XElement("АдрРФ");
            SvPokupAdres.Add(SvPokupAdrRF);
            if (infoGpl[7].ToString().Trim().Length > 0)
            {
                XAttribute SvPokupIndex = new XAttribute("Индекс", infoKag[7]);
                SvPokupAdrRF.Add(SvPokupIndex);
            }
            //     if (infoGpl[8].ToString().Length > 0) Атрибут обязательный
            //     {
            XAttribute SvPokupKodReg = new XAttribute("КодРегион", infoKag[8]);
            SvPokupAdrRF.Add(SvPokupKodReg);
            //      }
            if (infoGpl[9].ToString().Length > 0)
            {
                XAttribute SvPokupCity = new XAttribute("Город", infoKag[9]);
                SvPokupAdrRF.Add(SvPokupCity);
            }
            if (infoGpl[10].ToString().Length > 0)
            {
                XAttribute SvPokupStreet = new XAttribute("Улица", infoKag[10]);
                SvPokupAdrRF.Add(SvPokupStreet);
            }
            if (infoGpl[11].ToString().Length > 0)
            {
                XAttribute SvPokupHouse = new XAttribute("Дом", infoKag[11]);
                SvPokupAdrRF.Add(SvPokupHouse);
            }


            //<Документ><<СвКСчФ>><ИнфПолФХЖ1>
            XElement DopSvFHJ1 = new XElement("ДопСвФХЖ1");
            XAttribute NaimOKV = new XAttribute("НаимОКВ", "Российский рубль");
            SVSF.Add(DopSvFHJ1);
            DopSvFHJ1.Add(NaimOKV);

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1>
            XElement InfPolFHJ1 = new XElement("ИнфПолФХЖ1");
            SVSF.Add(InfPolFHJ1);

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1><ТекстИнф>
            /*XElement TxtInf1 = new XElement("ТекстИнф");
            XAttribute TxtInf1Identif = new XAttribute("Идентиф", "номер_заказа");
            XAttribute TxtInf1Znachen = new XAttribute("Значен", CurrDataUKD[6]);
            InfPolFHJ1.Add(TxtInf1);
            TxtInf1.Add(TxtInf1Identif);
            TxtInf1.Add(TxtInf1Znachen);*/

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf2 = new XElement("ТекстИнф");
            XAttribute TxtInf2Identif = new XAttribute("Идентиф", "отправитель");
            XAttribute TxtInf2Znachen = new XAttribute("Значен", ilnFirm);
            InfPolFHJ1.Add(TxtInf2);
            TxtInf2.Add(TxtInf2Identif);
            TxtInf2.Add(TxtInf2Znachen);

            //номер заказа
            XElement TxtInf6 = new XElement("ТекстИнф");
            XAttribute TxtInf6Identif = new XAttribute("Идентиф", "номер_заказа");
            XAttribute TxtInf6Znachen = new XAttribute("Значен", CurrDataUKD[6]);
            InfPolFHJ1.Add(TxtInf6);
            TxtInf6.Add(TxtInf6Identif);
            TxtInf6.Add(TxtInf6Znachen);

            //дата заказа
            XElement TxtInf7 = new XElement("ТекстИнф");
            XAttribute TxtInf7Identif = new XAttribute("Идентиф", "дата_заказа");
            XAttribute TxtInf7Znachen = new XAttribute("Значен", Convert.ToDateTime(CurrDataUKD[8]).ToString(@"dd.MM.yyyy"));
            InfPolFHJ1.Add(TxtInf7);
            TxtInf7.Add(TxtInf7Identif);
            TxtInf7.Add(TxtInf7Znachen);

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf5 = new XElement("ТекстИнф");
            XAttribute TxtInf5Identif = new XAttribute("Идентиф", "грузополучатель");
            XAttribute TxtInf5Znachen = new XAttribute("Значен", infoGpl[2]);
            InfPolFHJ1.Add(TxtInf5);
            TxtInf5.Add(TxtInf5Identif);
            TxtInf5.Add(TxtInf5Znachen);

            //дата поставки
            XElement TxtInf9 = new XElement("ТекстИнф");
            XAttribute TxtInf9Identif = new XAttribute("Идентиф", "дата_поставки");
            XAttribute TxtInf9Znachen = new XAttribute("Значен", Convert.ToDateTime(CurrDataUKD[14]).ToString(@"dd.MM.yyyy"));
            InfPolFHJ1.Add(TxtInf9);
            TxtInf9.Add(TxtInf9Identif);
            TxtInf9.Add(TxtInf9Znachen);

            //<Документ><<СвКСчФ>><ИнфПолФХЖ1><ТекстИнф>
            XElement TxtInf3 = new XElement("ТекстИнф");
            XAttribute TxtInf3Identif = new XAttribute("Идентиф", "получатель");
            XAttribute TxtInf3Znachen = new XAttribute("Значен", infoKag[16]);
            InfPolFHJ1.Add(TxtInf3);
            TxtInf3.Add(TxtInf3Identif);
            TxtInf3.Add(TxtInf3Znachen);

            /************************** 3 уровень. <ТаблКСчФ> ************************/
            XElement TabSF = new XElement("ТаблКСчФ");
            DOC.Add(TabSF);

            decimal sumW0Nds_V = 0;
            decimal sumNds_V = 0;
            decimal sumWthNds_V = 0;

            decimal sumW0Nds_G = 0;
            decimal sumNds_G = 0;
            decimal sumWthNds_G = 0;

            decimal summWoNds_A = 0;
            decimal summWoNds_B = 0;

            //Item[] 0-Рсд, 1-ШК, 2-Название,  3-A_EiSh,  4-A_EiOkei,  5-A_EiEdi,  6-A_EiAcc,  7-A_QtOsn,  8-A_QtCn,  9-A_CnWoNds, 10-A_Tax, 11-A_SummNds, 12-A_SummWthNds 
            //                              , 13-B_EiSh, 14-B_EiOkei, 15-B_EiEdi, 16-B_EiAcc, 17-B_QtOsn, 18-B_QtCn, 19-B_CnWoNds, 20-B_Tax, 21-B_SummNds, 22-B_SummWthNds
            for (int i = 0; i < Item.GetLongLength(0); i++)
            {
                //<Документ><ТаблКСчФ><СведТов>
                XElement SvedTov = new XElement("СведТов");
                XAttribute NomStr = new XAttribute("НомСтр", Convert.ToString(i + 1));
                XAttribute NomTovVStr = new XAttribute("ПорНомТовВСЧФ", Convert.ToString(i + 1));
                XAttribute NaimTov = new XAttribute("НаимТов", Item[i, 2]);
                XAttribute OKEI_TovDo = new XAttribute("ОКЕИ_ТовДо", Item[i, 4]);
                XAttribute OKEI_TovPosle = new XAttribute("ОКЕИ_ТовПосле", Item[i, 14]);
                XAttribute KolTovDo = new XAttribute("КолТовДо", Math.Round(Convert.ToDecimal(Item[i, 8]), 2));
                XAttribute KolTovPosle = new XAttribute("КолТовПосле", Math.Round(Convert.ToDecimal(Item[i, 18]), 2));
                XAttribute CenaTovDo = new XAttribute("ЦенаТовДо", Item[i, 9]);
                XAttribute CenaTovPosle = new XAttribute("ЦенаТовПосле", Item[i, 19]);
                XAttribute NalStDo = new XAttribute("НалСтДо", Convert.ToString(Item[i, 10]));
                XAttribute NalStPosle = new XAttribute("НалСтПосле", Convert.ToString(Item[i, 10]));
                TabSF.Add(SvedTov);
                SvedTov.Add(NomStr);
                SvedTov.Add(NomTovVStr);
                SvedTov.Add(NaimTov);
                SvedTov.Add(OKEI_TovDo);
                SvedTov.Add(OKEI_TovPosle);
                SvedTov.Add(KolTovDo);
                SvedTov.Add(KolTovPosle);
                SvedTov.Add(CenaTovDo);
                SvedTov.Add(CenaTovPosle);
                SvedTov.Add(NalStDo);
                SvedTov.Add(NalStPosle);

                //<Документ><ТаблКСчФ><СведТов><СтТовБезНДС>
                XElement StTovBezNds = new XElement("СтТовБезНДС");
                summWoNds_A = Math.Round(Convert.ToDecimal(Item[i, 12]) - Convert.ToDecimal(Item[i, 11]), 2);
                summWoNds_B = Math.Round(Convert.ToDecimal(Item[i, 22]) - Convert.ToDecimal(Item[i, 21]), 2);
                XAttribute StoimDoIzm = new XAttribute("СтоимДоИзм", summWoNds_A);
                XAttribute StoimPosleIzm = new XAttribute("СтоимПослеИзм", summWoNds_B);
                XAttribute StoimUvel = new XAttribute("СтоимУвел", Math.Round(summWoNds_B - summWoNds_A, 2));
                XAttribute StoimUm = new XAttribute("СтоимУм", Math.Round(summWoNds_A - summWoNds_B, 2));
                SvedTov.Add(StTovBezNds);
                StTovBezNds.Add(StoimDoIzm);
                StTovBezNds.Add(StoimPosleIzm);
                if (summWoNds_A < summWoNds_B)
                {
                    StTovBezNds.Add(StoimUvel);
                    sumW0Nds_V = sumW0Nds_V + Math.Round(summWoNds_B - summWoNds_A, 2);
                }
                if (summWoNds_A > summWoNds_B)
                {
                    StTovBezNds.Add(StoimUm);
                    sumW0Nds_G = sumW0Nds_G + Math.Round(summWoNds_A - summWoNds_B, 2);
                }

                //<Документ><ТаблКСчФ><СведТов><АкцизДо>
                XElement AkcizDo = new XElement("АкцизДо");
                XElement bezAkcizDo = new XElement("БезАкциз", "без акциза");
                SvedTov.Add(AkcizDo);
                AkcizDo.Add(bezAkcizDo);

                //<Документ><ТаблКСчФ><СведТов><АкцизПосле>
                XElement AkcizPosle = new XElement("АкцизПосле");
                XElement bezAkcizPosle = new XElement("БезАкциз", "без акциза");
                SvedTov.Add(AkcizPosle);
                AkcizPosle.Add(bezAkcizPosle);

                //<Документ><ТаблКСчФ><СведТов><АкцизРазн>
                /*XElement AkcizRazn = new XElement("АкцизРазн");
                XElement AkcizRaznSumUvel = new XElement("СумУвел", "0.00");
                XElement AkcizRaznSumUm = new XElement("СумУм", "0.00");
                SvedTov.Add(AkcizRazn);
                if (summWoNds_A < summWoNds_B) AkcizRazn.Add(AkcizRaznSumUvel);
                if (summWoNds_A > summWoNds_B) AkcizRazn.Add(AkcizRaznSumUm);*/

                //<Документ><ТаблКСчФ><СведТов><СумНалДо>
                XElement SumNalDo = new XElement("СумНалДо");
                SvedTov.Add(SumNalDo);
                if (Convert.ToString(Item[i, 10]) == "без НДС")
                {
                    //<Документ><ТаблКСчФ><СведТов><СумНалДо><БезНДС>
                    XElement BezNdsDo = new XElement("БезНДС", "без НДС");
                    SumNalDo.Add(BezNdsDo);
                }
                else
                {
                    //<Документ><ТаблКСчФ><СведТов><СумНалДо><СумНДС>
                    XElement SumNalSumNdsDo = new XElement("СумНДС", Math.Round(Convert.ToDecimal(Item[i, 11]), 2));
                    SumNalDo.Add(SumNalSumNdsDo);
                }

                //<Документ><ТаблКСчФ><СведТов><СумНалПосле>
                XElement SumNalPosle = new XElement("СумНалПосле");
                SvedTov.Add(SumNalPosle);
                if (Convert.ToString(Item[i, 20]) == "без НДС")
                {
                    //<Документ><ТаблКСчФ><СведТов><СумНалПосле><БезНДС>
                    XElement BezNdsPosle = new XElement("БезНДС", "без НДС");
                    SumNalPosle.Add(BezNdsPosle);
                }
                else
                {
                    //<Документ><ТаблКСчФ><СведТов><СумНалПосле><СумНДС>
                    XElement SumNalSumNdsPosle = new XElement("СумНДС", Math.Round(Convert.ToDecimal(Item[i, 21]), 2));
                    SumNalPosle.Add(SumNalSumNdsPosle);
                }

                //<Документ><ТаблКСчФ><СведТов><СумНалРазн>
                XElement SumNalRazn = new XElement("СумНалРазн");
                XElement SumNalRaznSumUvel = new XElement("СумУвел", Math.Round(Convert.ToDecimal(Item[i, 21]) - Convert.ToDecimal(Item[i, 11]), 2));
                XElement SumNalRaznSumUm = new XElement("СумУм", Math.Round(Convert.ToDecimal(Item[i, 11]) - Convert.ToDecimal(Item[i, 21]), 2));
                SvedTov.Add(SumNalRazn);
                if (summWoNds_A < summWoNds_B)
                {
                    SumNalRazn.Add(SumNalRaznSumUvel);
                    sumNds_V = sumNds_V + Math.Round(Convert.ToDecimal(Item[i, 21]) - Convert.ToDecimal(Item[i, 11]), 2);
                }
                if (summWoNds_A > summWoNds_B)
                {
                    SumNalRazn.Add(SumNalRaznSumUm);
                    sumNds_G = sumNds_G + Math.Round(Convert.ToDecimal(Item[i, 11]) - Convert.ToDecimal(Item[i, 21]), 2);
                }


                //<Документ><ТаблКСчФ><СведТов><СтТовУчНал>
                XElement StTovUchNal = new XElement("СтТовУчНал");
                XAttribute StTovUchNalStoimDoIzm = new XAttribute("СтоимДоИзм", Math.Round(Convert.ToDecimal(Item[i, 12]), 2));
                XAttribute StTovUchNalStoimPosleIzm = new XAttribute("СтоимПослеИзм", Math.Round(Convert.ToDecimal(Item[i, 22]), 2));
                XAttribute StTovUchNalStoimUvel = new XAttribute("СтоимУвел", Math.Round(Convert.ToDecimal(Item[i, 22]) - Convert.ToDecimal(Item[i, 12]), 2));
                XAttribute StTovUchNalStoimUm = new XAttribute("СтоимУм", Math.Round(Convert.ToDecimal(Item[i, 12]) - Convert.ToDecimal(Item[i, 22]), 2));
                SvedTov.Add(StTovUchNal);
                StTovUchNal.Add(StTovUchNalStoimDoIzm);
                StTovUchNal.Add(StTovUchNalStoimPosleIzm);
                if (summWoNds_A < summWoNds_B)
                    if (summWoNds_A > summWoNds_B)

                        if (summWoNds_A < summWoNds_B)
                        {
                            StTovUchNal.Add(StTovUchNalStoimUvel);
                            sumWthNds_V = sumWthNds_V + Math.Round(Convert.ToDecimal(Item[i, 22]) - Convert.ToDecimal(Item[i, 12]), 2);
                        }
                if (summWoNds_A > summWoNds_B)
                {
                    StTovUchNal.Add(StTovUchNalStoimUm);
                    sumWthNds_G = sumWthNds_G + Math.Round(Convert.ToDecimal(Item[i, 12]) - Convert.ToDecimal(Item[i, 22]), 2);
                }

                //<Документ><ТаблКСчФ><СведТов><ИнфПолФХЖ2>
                XElement InfPolFHJ23 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf3Identif = new XAttribute("Идентиф", "номер_заказа");
                XAttribute ItmTxtInf3Znachen = new XAttribute("Значен", CurrDataUKD[6]);
                SvedTov.Add(InfPolFHJ23);
                InfPolFHJ23.Add(ItmTxtInf3Identif);
                InfPolFHJ23.Add(ItmTxtInf3Znachen);

                //<Документ><ТаблКСчФ><СведТов><ИнфПолФХЖ2>
                string nomBuyerCd = Verifiacation.GetBuyerItemCodeRcd(Convert.ToString(infoKag[5]), Convert.ToInt64(Item[i, 0]));

                XElement InfPolFHJ21 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf1Identif = new XAttribute("Идентиф", "код_материала");
                XAttribute ItmTxtInf1Znachen = new XAttribute("Значен", nomBuyerCd);
                SvedTov.Add(InfPolFHJ21);
                InfPolFHJ21.Add(ItmTxtInf1Identif);
                InfPolFHJ21.Add(ItmTxtInf1Znachen);

                //<Документ><ТаблКСчФ><СведТов><ИнфПолФХЖ2>
                XElement InfPolFHJ22 = new XElement("ИнфПолФХЖ2");
                XAttribute ItmTxtInf2Identif = new XAttribute("Идентиф", "штрихкод");
                XAttribute ItmTxtInf2Znachen = new XAttribute("Значен", Item[i, 1]);
                SvedTov.Add(InfPolFHJ22);
                InfPolFHJ22.Add(ItmTxtInf2Identif);
                InfPolFHJ22.Add(ItmTxtInf2Znachen);

                //<Документ><ТаблКСчФ><СведТов><ДопСведТов>
                XElement DopInfo = new XElement("ДопСведТов");
                XAttribute NmEiBefore = new XAttribute("НаимЕдИзмДо", Item[i, 3]);
                XAttribute NmEiAfter = new XAttribute("НаимЕдИзмПосле", Item[i, 13]);
                SvedTov.Add(DopInfo);
                DopInfo.Add(NmEiBefore);
                DopInfo.Add(NmEiAfter);
            }

            if (sumWthNds_V > 0)
            {
                //<Документ><ТаблКСчФ><ВсегоУвел>
                XElement VsegoUvel = new XElement("ВсегоУвел");
                XAttribute VsegoUvelBezNds = new XAttribute("СтТовБезНДСВсего", Math.Round(sumW0Nds_V, 2));
                XAttribute VsegoUvelSNDS = new XAttribute("СтТовУчНалВсего", Math.Round(sumWthNds_V, 2));
                TabSF.Add(VsegoUvel);
                VsegoUvel.Add(VsegoUvelBezNds);
                VsegoUvel.Add(VsegoUvelSNDS);

                //<Документ><ТаблКСчФ><ВсегоУвел><СумНал>
                XElement VsegoUvelSumNal = new XElement("СумНал");
                VsegoUvel.Add(VsegoUvelSumNal);
                if (Convert.ToString(Item[0, 10]) == "без НДС")
                {
                    //<Документ><ТаблКСчФ><ВсегоУвел><СумНал><БезНДС>
                    XElement VsegoUvelSumNalBezNds = new XElement("БезНДС", "без НДС");
                    VsegoUvelSumNal.Add(VsegoUvelSumNalBezNds);
                }
                else
                {
                    //<Документ><ТаблКСчФ><ВсегоОпл><СумНал><СумНал>
                    XElement VsegoUvelSumNalSumMal = new XElement("СумНДС", Math.Round(sumNds_V, 2));
                    VsegoUvelSumNal.Add(VsegoUvelSumNalSumMal);
                }
            }

            if (sumWthNds_G > 0)
            {
                //<Документ><ТаблКСчФ><ВсегоУм>
                XElement VsegoUm = new XElement("ВсегоУм");
                XAttribute VsegoUmBezNds = new XAttribute("СтТовБезНДСВсего", Math.Round(sumW0Nds_G, 2));
                XAttribute VsegoUmSNDS = new XAttribute("СтТовУчНалВсего", Math.Round(sumWthNds_G, 2));
                TabSF.Add(VsegoUm);
                VsegoUm.Add(VsegoUmBezNds);
                VsegoUm.Add(VsegoUmSNDS);

                //<Документ><ТаблКСчФ><ВсегоУвел><СумНал>
                XElement VsegoUmSumNal = new XElement("СумНал");
                VsegoUm.Add(VsegoUmSumNal);
                if (Convert.ToString(Item[0, 10]) == "без НДС")
                {
                    //<Документ><ТаблКСчФ><ВсегоУвел><СумНал><БезНДС>
                    XElement VsegoUmSumNalBezNds = new XElement("БезНДС", "без НДС");
                    VsegoUmSumNal.Add(VsegoUmSumNalBezNds);
                }
                else
                {
                    //<Документ><ТаблКСчФ><ВсегоОпл><СумНал><СумНал>
                    XElement VsegoUmSumNalSumMal = new XElement("СумНДС", Math.Round(sumNds_G, 2));
                    VsegoUmSumNal.Add(VsegoUmSumNalSumMal);
                }
            }

            /************************** 3 уровень. <СодФХЖ3> ************************/
            /*XElement SodFHJ3 = new XElement("СодФХЖ3");
            string allPeredSf = infoCorSf[0].ToString() + " от " + Convert.ToDateTime(infoCorSf[1]).ToString(@"dd.MM.yyyy");
            if (InfoPrevSf.Length > 2) allPeredSf = allPeredSf + "," + InfoPrevSf;
            XAttribute InieSvIzmStoim = new XAttribute("ИныеСвИзмСтоим", "Изменения");
            XAttribute PeredatDocum = new XAttribute("ПередатДокум", allPeredSf);
            XAttribute SodOper = new XAttribute("СодОпер", "Изменение стоимости товаров и услуг");
            XAttribute DataNapr = new XAttribute("ДатаНапр", DateTime.Today.ToString(@"dd.MM.yyyy"));
            DOC.Add(SodFHJ3);
            SodFHJ3.Add(InieSvIzmStoim);
            SodFHJ3.Add(PeredatDocum);
            SodFHJ3.Add(SodOper);
            SodFHJ3.Add(DataNapr);

            //<Документ><СодФХЖ3><ОснКор>
            XElement OsnKorr = new XElement("ОснКор");
            XAttribute NaimOsn = new XAttribute("НаимОсн", "Иные");
            XAttribute DataOsn = new XAttribute("ДатаОсн", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));
            XAttribute DopSvedOsn = new XAttribute("ДопСвОсн", "Отсутствуют");
            SodFHJ3.Add(OsnKorr);
            OsnKorr.Add(NaimOsn);
            OsnKorr.Add(DataOsn);
            OsnKorr.Add(DopSvedOsn);*/

            XElement SodFHJ3 = new XElement("СодФХЖ3");
            XAttribute SodOper = new XAttribute("СодОпер", "Корректировка");
            XAttribute DataNapr = new XAttribute("ДатаНапр", DateTime.Today.ToString(@"dd.MM.yyyy"));
            DOC.Add(SodFHJ3);
            SodFHJ3.Add(SodOper);
            SodFHJ3.Add(DataNapr);

            //<Документ><СодФХЖ3><ПередатДокум>
            XElement PeredDoc = new XElement("ПередатДокум");
            XAttribute PeredDocNmOsn = new XAttribute("НаимОсн", "Универсальный передаточный документ");
            XAttribute PeredDocDataOsn = new XAttribute("ДатаОсн", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));
            XAttribute PeredDocNmrOsn = new XAttribute("НомОсн", infoCorSf[0].ToString());
            SodFHJ3.Add(PeredDoc);
            PeredDoc.Add(PeredDocNmOsn);
            PeredDoc.Add(PeredDocDataOsn);
            PeredDoc.Add(PeredDocNmrOsn);

            //<Документ><СодФХЖ3><ДокумОснКор>
            XElement PeredDocOsnKorr = new XElement("ДокумОснКор");
            XAttribute NaimOsn = new XAttribute("НаимОсн", "Иные");
            XAttribute DataOsn = new XAttribute("ДатаОсн", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));
            XAttribute DopSvedOsn = new XAttribute("ДопСвОсн", "Отсутствуют");
            SodFHJ3.Add(PeredDocOsnKorr);
            PeredDocOsnKorr.Add(NaimOsn);
            PeredDocOsnKorr.Add(DataOsn);
            PeredDocOsnKorr.Add(DopSvedOsn);

            /************************** 3 уровень. <Подписант> ************************/
            string[] infoSigner = Verifiacation.GetSignerOpt(); //0 ОБЩИЕ.ЭЦПДОЛЖНОСТЬ, 1 ОБЩИЕ.ЭЦПФАМИЛИЯ, 2 ОБЩИЕ.ЭЦПИМЯ, 3 ОБЩИЕ.ЭЦПОТЧЕСТВО, 4 ОБЩИЕ.ЭЦПДОЛЖНОБЯЗ

            XElement Podp = new XElement("Подписант");
            XAttribute obl = new XAttribute("ОблПолн", "6");
            XAttribute status = new XAttribute("Статус", "1");
            XAttribute osn = new XAttribute("ОснПолн", infoSigner[4]);
            DOC.Add(Podp);
            Podp.Add(obl);
            Podp.Add(status);
            Podp.Add(osn);

            //<Документ><Подписант><ЮЛ>
            XElement UL = new XElement("ЮЛ");
            XAttribute innUl = new XAttribute("ИННЮЛ", infoFirm[1]);
            XAttribute naimOrg = new XAttribute("НаимОрг", infoFirm[0]);
            XAttribute dolj = new XAttribute("Должн", infoSigner[0]);
            Podp.Add(UL);
            UL.Add(innUl);
            UL.Add(naimOrg);
            UL.Add(dolj);

            //<Документ><Подписант><ЮЛ><ФИО>
            XElement FIO = new XElement("ФИО");
            XAttribute famdir = new XAttribute("Фамилия", infoSigner[1]);
            XAttribute namedir = new XAttribute("Имя", infoSigner[2]);
            XAttribute otchesdir = new XAttribute("Отчество", infoSigner[3]);
            UL.Add(FIO);
            FIO.Add(famdir);
            FIO.Add(namedir);
            FIO.Add(otchesdir);



            //------сохранение документа-----------
            fileName = fileName + ".xml";
            try
            {
                xdoc.Save(pathArchiveEDI + fileName);
                try
                {
                    xdoc.Save(pathUKDEDI + fileName);
                    string message = "EDISOFT. УКД " + fileName + " создан в " + pathUKDEDI;
                    Program.WriteLine(message);
                    DispOrders.WriteProtocolEDI("УКД", fileName, infoKag[0] + " - " + infoKag[1], 0, infoGpl[0] + " - " + infoGpl[1], "УКД сформирован", DateTime.Now, Convert.ToString(CurrDataUKD[6]), "EDISOFT");
                    DispOrders.WriteEDiSentDoc("8", fileName, Convert.ToString(CurrDataUKD[3]), Convert.ToString(infoSf[0]), "123", Convert.ToString(sumWthNds_V - sumWthNds_G), Convert.ToString(CurrDataUKD[7]), 1);
                    //запись в лог о удаче
                }
                catch (Exception e)
                {
                    string message_error = "EDISOFT. Не могу создать xml файл УКД в " + pathUKDEDI + ". Нет доступа или диск переполнен.";
                    DispOrders.WriteProtocolEDI("УКД", fileName, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УКД не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUKD[6]), "EDISOFT");
                    Program.WriteLine(message_error);
                    //DispOrders.WriteErrorLog(e.Message);
                }
            }
            catch (Exception e)
            {
                string message_error = "EDISOFT. Не могу создать xml файл УКД в " + pathArchiveEDI + ". Нет доступа или диск переполнен.";
                DispOrders.WriteProtocolEDI("УКД", fileName, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УКД не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUKD[6]), "EDISOFT");
                Program.WriteLine(message_error);
                DispOrders.WriteErrorLog(e.Message);
                //запись в лог о неудаче
            }
        }
        /********************************************************** Конец УКД Tander **********************************************************/
    }


}
