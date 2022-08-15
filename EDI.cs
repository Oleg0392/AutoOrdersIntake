using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using System.IO;

namespace AutoOrdersIntake
{
    class EDIXMLCreation
    {
        public static void CreateEdiInvoice(List <object> CurrDataInvoice)
        {
            
            //получение путей
            string ArchiveEDISOFT = DispOrders.GetValueOption("EDI-СОФТ.АРХИВ");

            //для ксф
            bool KSF = false;

            //string InvoiceEDISOFT = "\\\\fileshare\\EXPIMP\\OrderIntake\\EDISOFT\\OUTBOX\\";//test
            //string ArchiveEDISOFT = "\\\\fileshare\\EXPIMP\\OrderIntake\\EDISOFT\\ARCHIVE\\";

            //получение данных
            object[] DelivInfo = Verifiacation.GetDataFromPtnCD(Convert.ToString(CurrDataInvoice[8]));
            object[] PlatInfo = Verifiacation.GetDataFromPtnCD(Convert.ToString(CurrDataInvoice[9]));

            int CntLinesInvoice = Verifiacation.CountItemsInOrder(Convert.ToString(CurrDataInvoice[1]), 5);
            object[,] Item = DispOrders.GetItemsFromTrdS(Convert.ToString(CurrDataInvoice[1]), CntLinesInvoice, 5);

            //какой gln номер использовать
            bool UseMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(DelivInfo[8]));
            string ILN_Edi;
            string InvoiceEDISOFT;
            object[] FirmInfo;
            object[] FirmAdr;
            if (UseMasterGLN == false)//используем данные текущего предприятия
            {
                ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ILN_EDI");
                InvoiceEDISOFT = DispOrders.GetValueOption("EDI-СОФТ.СФ");
                FirmInfo = Verifiacation.GetFirmInfo();
                FirmAdr = Verifiacation.GetFirmAdr();

            }
            else//используем данные головного предприятия
            {
                ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");
                FirmInfo = Verifiacation.GetMasterFirmInfo();
                FirmAdr = Verifiacation.GetMasterFirmAdr();
                try
                {
                    InvoiceEDISOFT = DispOrders.GetValueOption("EDI-СОФТ.ЭКСПОРТ");
                }
                catch
                {
                    InvoiceEDISOFT = DispOrders.GetValueOption("EDI-СОФТ.СФ");
                }

            }
            
            //итоги для ксф
            decimal KSFTotalInvoicedAmount = 0;
            decimal KSFTotalNetAmount = 0;
            decimal KSFTotalTaxAmount = 0;
            decimal KSFTotalGrossAmount = 0;

            decimal KSFDecreaseTotalNetAmount = 0;
            decimal KSFDecreaseTotalTaxAmount = 0;
            decimal KSFDecreaseTotalGrossAmount = 0;

            decimal KSFNetAmount = 0;


            if (Convert.ToInt32(CurrDataInvoice[14]) != 0)
            {
                KSF = true;
            }
               
            object[] Total = DispOrders.GetTotal(Convert.ToString(CurrDataInvoice[1]), 5);

            object[] SignerInfo = Verifiacation.GetSigner();
            string GlnGr = Verifiacation.GetGLNGR(Convert.ToString(CurrDataInvoice[9]));

            XDocument xdoc = new XDocument();

            //основные элементы (1 уровень)
            XElement DocumentInvoice = new XElement("Document-Invoice");
            XElement InvoiceHeader = new XElement("Invoice-Header");
            XElement DocumentParties = new XElement("Document-Parties");
            XElement InvoiceParties = new XElement("Invoice-Parties");
            XElement InvoiceLines = new XElement("Invoice-Lines");
            XElement InvoiceSummary = new XElement("Invoice-Summary");
            XElement Signer = new XElement("Signer");
            
            xdoc.Add(DocumentInvoice);
            DocumentInvoice.Add(InvoiceHeader);
            DocumentInvoice.Add(DocumentParties);
            DocumentInvoice.Add(InvoiceParties);
            DocumentInvoice.Add(InvoiceLines);
            DocumentInvoice.Add(InvoiceSummary);
            DocumentInvoice.Add(Signer);

            //-----Invoice Header------
            XElement InvoiceNumber = new XElement("InvoiceNumber",CurrDataInvoice[2]);
            XElement InvoiceDate = new XElement("InvoiceDate", (Convert.ToDateTime(CurrDataInvoice[3])).ToString("yyyy-MM-dd"));
            XElement InvoiceCurrency = new XElement("InvoiceCurrency","RUB");
            XElement InvoicePaymentDueDate = new XElement("InvoicePaymentDueDate", (Convert.ToDateTime(CurrDataInvoice[4])).ToString("yyyy-MM-dd"));
            XElement DocumentFunctionCode = new XElement("DocumentFunctionCode","9");

            InvoiceHeader.Add(InvoiceNumber);
            InvoiceHeader.Add(InvoiceDate);
            InvoiceHeader.Add(InvoiceCurrency);
            InvoiceHeader.Add(InvoicePaymentDueDate);
            InvoiceHeader.Add(DocumentFunctionCode);
            
            XElement Order = new XElement("Order");
            XElement Delivery = new XElement("Delivery");

            XElement BuyerOrderNumber = new XElement("BuyerOrderNumber",CurrDataInvoice[5]);
            XElement BuyerOrderDate = new XElement("BuyerOrderDate", (Convert.ToDateTime(CurrDataInvoice[6])).ToString("yyyy-MM-dd"));
            XElement SellerOrderNumber = new XElement("SellerOrderNumber", CurrDataInvoice[7]);

            Order.Add(BuyerOrderNumber);
            Order.Add(BuyerOrderDate);
            Order.Add(SellerOrderNumber);

            XElement DeliveryLocationNumber = new XElement("DeliveryLocationNumber",DelivInfo[2]);
            XElement Name = new XElement("Name", DelivInfo[1]);
            XElement Country = new XElement("Country","RU");
            XElement CountryCode = new XElement("CountryCode","643");
            XElement DeliveryDate = new XElement("DeliveryDate", (Convert.ToDateTime(CurrDataInvoice[15])).ToString("yyyy-MM-dd"));
            XElement DespatchNumber = new XElement("DespatchNumber",CurrDataInvoice[12]);

            Delivery.Add(DeliveryLocationNumber);
            Delivery.Add(Name);
            Delivery.Add(Country);
            Delivery.Add(CountryCode);
            Delivery.Add(DeliveryDate);
            Delivery.Add(DespatchNumber);

            InvoiceHeader.Add(Order);

            if (KSF == true)
            {
                XElement Reference = new XElement("Reference");

                InvoiceHeader.Add(Reference);

                XElement InvoiceReferenceNumber = new XElement("InvoiceReferenceNumber", Convert.ToString(CurrDataInvoice[18]));
                XElement InvoiceReferenceDate = new XElement("InvoiceReferenceDate", Convert.ToDateTime(CurrDataInvoice[17]).ToString("yyyy-MM-dd"));

                Reference.Add(InvoiceReferenceNumber);
                Reference.Add(InvoiceReferenceDate);
            }

            InvoiceHeader.Add(Delivery);

            //----Docement Parties------------------
            XElement Sender = new XElement("Sender");
            XElement Receiver = new XElement("Receiver");

            DocumentParties.Add(Sender);
            DocumentParties.Add(Receiver);

            XElement ILNSender = new XElement("ILN",ILN_Edi);
            XElement NameSender = new XElement("Name",FirmInfo[0]);

            Sender.Add(ILNSender);
            Sender.Add(NameSender);

            XElement ILNReceiver = new XElement("ILN", GlnGr);
            XElement NameReceiver = new XElement("Name",PlatInfo[1]);

            Receiver.Add(ILNReceiver);
            Receiver.Add(NameReceiver);

            //----Invoice-Parties------------------
            XElement Buyer = new XElement("Buyer");
            XElement Payer = new XElement("Payer");
            XElement Seller = new XElement("Seller");

            InvoiceParties.Add(Buyer);
            InvoiceParties.Add(Payer);
            InvoiceParties.Add(Seller);

            XElement ILNBuyer = new XElement("ILN", PlatInfo[2]);
            XElement TaxID = new XElement("TaxID", PlatInfo[3]);
            XElement TaxRegistrationReasonCode = new XElement("TaxRegistrationReasonCode", PlatInfo[4]);
            XElement NameBuyer = new XElement("Name", PlatInfo[1]);
            XElement CountryBuyer = new XElement("Country","RU");

            Buyer.Add(ILNBuyer);
            Buyer.Add(TaxID);
            Buyer.Add(TaxRegistrationReasonCode);
            Buyer.Add(NameBuyer);
            Buyer.Add(CountryBuyer);

            XElement ILNPayer = new XElement("ILN", PlatInfo[2]);
            XElement TaxIDPayer = new XElement("TaxID", PlatInfo[3]);
            XElement TaxRegistrationReasonCodePayer = new XElement("TaxRegistrationReasonCode", PlatInfo[4]);
            XElement NamePayer = new XElement("Name", PlatInfo[1]);
            XElement CountryPayer = new XElement("Country", "RU");

            Payer.Add(ILNPayer);
            Payer.Add(TaxIDPayer);
            Payer.Add(TaxRegistrationReasonCodePayer);
            Payer.Add(NamePayer);
            Payer.Add(CountryPayer);

            XElement ILNSeller = new XElement("ILN",ILN_Edi);
            XElement TaxIDSeller = new XElement("TaxID",FirmInfo[1]);
            XElement TaxRegistrationReasonCodeSeller = new XElement("TaxRegistrationReasonCode",FirmInfo[2]);
            XElement NameSeller = new XElement("Name",FirmInfo[0]);
            XElement StreetAndNumber = new XElement("StreetAndNumber",Convert.ToString(FirmAdr[0]));
            XElement CityName = new XElement("CityName", Convert.ToString(FirmAdr[1]));
            XElement State = new XElement("State", Convert.ToString(FirmAdr[2]));
            XElement StateCode = new XElement("StateCode");
            XElement PostalCode = new XElement("PostalCode", Convert.ToString(FirmAdr[3]));
            XElement CountrySeller = new XElement("Country", "RU");

            Seller.Add(ILNSeller);
            Seller.Add(TaxIDSeller);
            Seller.Add(TaxRegistrationReasonCodeSeller);
            Seller.Add(NameSeller);
            Seller.Add(StreetAndNumber);
            Seller.Add(CityName);
            Seller.Add(State);
            Seller.Add(StateCode);
            Seller.Add(PostalCode);
            Seller.Add(CountrySeller);

            //--------<Invoice-Lines>---------------

            if (KSF == false)//обычная СФ
            {
                for (int i = 0; i < CntLinesInvoice; i++)
                {
                    
                    XElement Line = new XElement("Line");
                    XElement LineItem = new XElement("Line-Item");

                    InvoiceLines.Add(Line);
                    Line.Add(LineItem);

                    object[] BICode = Verifiacation.GetBuyerItemCode(Convert.ToString(PlatInfo[5]), Convert.ToString(Item[i, 1]));

                    XElement LineNumber = new XElement("LineNumber", i + 1);
                    XElement EAN = new XElement("EAN", Item[i, 0]);
                    XElement BuyerItemCode = new XElement("BuyerItemCode", BICode[0]);
                    XElement SupplierItemCode = new XElement("SupplierItemCode", Item[i, 2]);
                    XElement ItemDescription = new XElement("ItemDescription", Item[i, 3]);
                    XElement InvoiceQuantity = new XElement("InvoiceQuantity", Item[i, 4]);
                    XElement InvoiceUnitNetPrice = new XElement("InvoiceUnitNetPrice", Item[i, 5]);
                    XElement InvoiceUnitGrossPrice = new XElement("InvoiceUnitGrossPrice", Item[i, 6]);
                    XElement UnitOfMeasure = new XElement("UnitOfMeasure", Item[i, 7]);

                    XElement UnitOfMeasureCode = new XElement("UnitOfMeasureCode", Item[i, 8]);
                    XElement TaxRate = new XElement("TaxRate", Item[i, 9]);
                    XElement TaxCategoryCode = new XElement("TaxCategoryCode", Item[i, 10]);
                    XElement TaxAmount = new XElement("TaxAmount", Item[i, 11]);
                    XElement NetAmount = new XElement("NetAmount", Item[i, 12]);
                    XElement GrossAmount = new XElement("GrossAmount", Convert.ToDecimal(Item[i, 11]) + Convert.ToDecimal(Item[i,12]));

                    LineItem.Add(LineNumber);
                    LineItem.Add(EAN);
                    LineItem.Add(BuyerItemCode);
                    LineItem.Add(SupplierItemCode);
                    LineItem.Add(ItemDescription);
                    LineItem.Add(InvoiceQuantity);
                    LineItem.Add(InvoiceUnitNetPrice);
                    LineItem.Add(InvoiceUnitGrossPrice);
                    LineItem.Add(UnitOfMeasure);
                    LineItem.Add(UnitOfMeasureCode);
                    LineItem.Add(TaxRate);
                    LineItem.Add(TaxCategoryCode);
                    LineItem.Add(TaxAmount);
                    LineItem.Add(NetAmount);
                    LineItem.Add(GrossAmount);

                }
                //------InvoiceSummary----------------

                XElement TotalLines = new XElement("TotalLines", CntLinesInvoice);
                XElement TotalInvoicedAmount = new XElement("TotalInvoicedAmount", Total[0]);
                XElement TotalNetAmount = new XElement("TotalNetAmount", Total[5]);
                XElement TotalTaxAmount = new XElement("TotalTaxAmount", Convert.ToDecimal(Total[4]) - Convert.ToDecimal(Total[5]));
                XElement TotalGrossAmount = new XElement("TotalGrossAmount", Convert.ToDecimal(Total[4]));
                XElement TaxSummary = new XElement("Tax-Summary");

                InvoiceSummary.Add(TotalLines);
                InvoiceSummary.Add(TotalInvoicedAmount);
                InvoiceSummary.Add(TotalNetAmount);
                InvoiceSummary.Add(TotalTaxAmount);
                InvoiceSummary.Add(TotalGrossAmount);
                InvoiceSummary.Add(TaxSummary);

                XElement TaxSummaryLine = new XElement("Tax-Summary-Line");

                TaxSummary.Add(TaxSummaryLine);

                XElement TaxRateLine = new XElement("TaxRate", Item[0, 9]);
                XElement TaxCategoryCodeLine = new XElement("TaxCategoryCode", "S");
                XElement TaxAmountLine = new XElement("TaxAmount", Convert.ToDecimal(Total[4]) - Convert.ToDecimal(Total[5]));
                XElement TaxableAmountLine = new XElement("TaxableAmount", Total[5]);

                TaxSummaryLine.Add(TaxRateLine);
                TaxSummaryLine.Add(TaxCategoryCodeLine);
                TaxSummaryLine.Add(TaxAmountLine);
                TaxSummaryLine.Add(TaxableAmountLine);
            }
            else//КСФ
            {
                for (int i = 0; i < CntLinesInvoice; i++)
                {
                    object [,] prevItem = DispOrders.GetItemFromPrevDoc(Convert.ToString(CurrDataInvoice[14]),Convert.ToString(Item[i,0]));
                    
                    XElement Line = new XElement("Line");
                    XElement LineItem = new XElement("Line-Item");

                    InvoiceLines.Add(Line);
                    Line.Add(LineItem);

                    object[] BICode = Verifiacation.GetBuyerItemCode(Convert.ToString(PlatInfo[5]), Convert.ToString(Item[i, 1]));

                    XElement LineNumber = new XElement("LineNumber", i + 1);
                    XElement EAN = new XElement("EAN", Item[i, 0]);
                    XElement BuyerItemCode = new XElement("BuyerItemCode", BICode[0]);
                    XElement SupplierItemCode = new XElement("SupplierItemCode", Item[i, 2]);
                    XElement ItemDescription = new XElement("ItemDescription", Item[i, 3]);
                    XElement PreviousInvoiceQuantity = new XElement("PreviousInvoiceQuantity", prevItem[0, 4]);
                    XElement InvoiceQuantity = new XElement("InvoiceQuantity",Convert.ToDecimal( prevItem[0, 4])-Math.Abs(Convert.ToDecimal(Item[i, 4])));
                    XElement PreviousInvoiceUnitNetPrice = new XElement("PreviousInvoiceUnitNetPrice", prevItem[0, 5]);
                    XElement InvoiceUnitNetPrice = new XElement("InvoiceUnitNetPrice", Item[i, 5]);
                    XElement InvoiceUnitGrossPrice = new XElement("InvoiceUnitGrossPrice", Item[i, 6]);
                    XElement PreviousUnitOfMeasure = new XElement("PreviousUnitOfMeasure", prevItem[0, 7]);
                    XElement UnitOfMeasure = new XElement("UnitOfMeasure", Item[i, 7]);
                    XElement PreviousUnitOfMeasureCode = new XElement("PreviousUnitOfMeasureCode", prevItem[0, 8]);
                    XElement UnitOfMeasureCode = new XElement("UnitOfMeasureCode", Item[i, 8]);
                    XElement PreviousTaxRate = new XElement("PreviousTaxRate", Item[0, 9]);
                    XElement TaxRate = new XElement("TaxRate", Item[i, 9]);
                    XElement TaxCategoryCode = new XElement("TaxCategoryCode", Item[i, 10]);
                    XElement PreviousTaxAmount = new XElement("PreviousTaxAmount", prevItem[0, 11]);
                    XElement TaxAmount = new XElement("TaxAmount", Convert.ToDecimal(prevItem[0,11])-Math.Abs(Convert.ToDecimal(Item[i, 11])));
                    XElement DifferenceTaxAmount = new XElement("DifferenceTaxAmount", Math.Abs(Convert.ToDecimal(Item[i, 11])));
                    XElement PreviousNetAmount = new XElement("PreviousNetAmount", prevItem[0, 12]);
                    XElement NetAmount = new XElement("NetAmount",Convert.ToDecimal(prevItem[0, 12]) - Math.Abs(Convert.ToDecimal(Item[i, 12])) );
                    XElement DifferenceNetAmount = new XElement("DifferenceNetAmount", Math.Abs(Convert.ToDecimal(Item[i, 12])));
                    XElement PreviousGrossAmount = new XElement("PreviousGrossAmount", Convert.ToDecimal(prevItem[0, 4]) * Convert.ToDecimal(prevItem[0, 6]));
                    XElement DifferenceGrossAmount = new XElement("DifferenceGrossAmount", Math.Abs(Convert.ToDecimal(Item[i, 4])) * Math.Abs(Convert.ToDecimal(Item[i, 6])));
                    XElement GrossAmount = new XElement("GrossAmount", (Convert.ToDecimal(prevItem[0, 4]) * Convert.ToDecimal(prevItem[0, 6])) - (Math.Abs(Convert.ToDecimal(Item[i, 4])) * Math.Abs(Convert.ToDecimal(Item[i, 6]))));

                    KSFTotalInvoicedAmount = KSFTotalInvoicedAmount + (Convert.ToDecimal(prevItem[0, 4]) - Math.Abs(Convert.ToDecimal(Item[i, 4])));
                    KSFTotalNetAmount = KSFTotalNetAmount + (Convert.ToDecimal(prevItem[0, 12]) - Math.Abs(Convert.ToDecimal(Item[i, 12])));
                    KSFTotalTaxAmount = KSFTotalTaxAmount + (Convert.ToDecimal(prevItem[0, 11]) - Math.Abs(Convert.ToDecimal(Item[i, 11])));
                    KSFTotalGrossAmount = KSFTotalGrossAmount + ((Convert.ToDecimal(prevItem[0, 4]) * Convert.ToDecimal(prevItem[0, 6])) - (Math.Abs(Convert.ToDecimal(Item[i, 4])) * Math.Abs(Convert.ToDecimal(Item[i, 6]))));

                    KSFDecreaseTotalNetAmount = KSFDecreaseTotalNetAmount + (Convert.ToDecimal(prevItem[0, 12]) - Math.Abs(Convert.ToDecimal(Item[i, 12])));
                    KSFDecreaseTotalTaxAmount = KSFDecreaseTotalTaxAmount + Math.Abs(Convert.ToDecimal(Item[i, 11]));
                    KSFDecreaseTotalGrossAmount = KSFDecreaseTotalGrossAmount + (Math.Abs(Convert.ToDecimal(Item[i, 4])) * Math.Abs(Convert.ToDecimal(Item[i, 6])));

                    KSFNetAmount = KSFNetAmount + Convert.ToDecimal(Item[i, 12]);

                    LineItem.Add(LineNumber);
                    LineItem.Add(EAN);
                    LineItem.Add(BuyerItemCode);
                    LineItem.Add(SupplierItemCode);
                    LineItem.Add(ItemDescription);
                    LineItem.Add(PreviousInvoiceQuantity);
                    LineItem.Add(InvoiceQuantity);
                    LineItem.Add(PreviousInvoiceUnitNetPrice);
                    LineItem.Add(InvoiceUnitNetPrice);
                    LineItem.Add(InvoiceUnitGrossPrice);
                    LineItem.Add(PreviousUnitOfMeasure);
                    LineItem.Add(PreviousUnitOfMeasureCode);
                    LineItem.Add(UnitOfMeasure);
                    LineItem.Add(UnitOfMeasureCode);
                    LineItem.Add(PreviousTaxRate);
                    LineItem.Add(TaxRate);
                    LineItem.Add(TaxCategoryCode);
                    LineItem.Add(PreviousTaxAmount);
                    LineItem.Add(TaxAmount);
                    LineItem.Add(DifferenceTaxAmount);
                    LineItem.Add(PreviousNetAmount);
                    LineItem.Add(NetAmount);
                    LineItem.Add(DifferenceNetAmount);
                    LineItem.Add(PreviousGrossAmount);
                    LineItem.Add(GrossAmount);
                    LineItem.Add(DifferenceGrossAmount);

                   
                }

                //------InvoiceSummary----------------

                XElement TotalLines = new XElement("TotalLines", CntLinesInvoice);
                XElement TotalInvoicedAmount = new XElement("TotalInvoicedAmount", Convert.ToDecimal(KSFTotalInvoicedAmount));
                XElement TotalNetAmount = new XElement("TotalNetAmount", Convert.ToDecimal(KSFTotalNetAmount));
                XElement TotalTaxAmount = new XElement("TotalTaxAmount", Convert.ToDecimal(KSFTotalTaxAmount));
                XElement TotalGrossAmount = new XElement("TotalGrossAmount", Convert.ToDecimal(KSFTotalGrossAmount));
                XElement TaxSummary = new XElement("Tax-Summary");

                XElement DecreaseTotalNetAmount = new XElement("DecreaseTotalNetAmount", Convert.ToDecimal(KSFDecreaseTotalGrossAmount - KSFDecreaseTotalTaxAmount));
                XElement DecreaseTotalTaxAmount = new XElement("DecreaseTotalTaxAmount", Convert.ToDecimal(KSFDecreaseTotalTaxAmount));
                XElement DecreaseTotalGrossAmount = new XElement("DecreaseTotalGrossAmount", Convert.ToDecimal(KSFDecreaseTotalGrossAmount));

                InvoiceSummary.Add(TotalLines);
                InvoiceSummary.Add(TotalInvoicedAmount);
                InvoiceSummary.Add(TotalNetAmount);
                InvoiceSummary.Add(TotalTaxAmount);
                InvoiceSummary.Add(TotalGrossAmount);
                InvoiceSummary.Add(DecreaseTotalNetAmount);
                InvoiceSummary.Add(DecreaseTotalTaxAmount);
                InvoiceSummary.Add(DecreaseTotalGrossAmount);
                InvoiceSummary.Add(TaxSummary);

                XElement TaxSummaryLine = new XElement("Tax-Summary-Line");

                TaxSummary.Add(TaxSummaryLine);

                XElement TaxRateLine = new XElement("TaxRate", Item[0, 9]);
                XElement TaxCategoryCodeLine = new XElement("TaxCategoryCode", "S");
                XElement TaxAmountLine = new XElement("TaxAmount", Convert.ToDecimal(KSFTotalTaxAmount));
                XElement TaxableAmountLine = new XElement("TaxableAmount", Convert.ToDecimal(KSFTotalNetAmount));

                TaxSummaryLine.Add(TaxRateLine);
                TaxSummaryLine.Add(TaxCategoryCodeLine);
                TaxSummaryLine.Add(TaxAmountLine);
                TaxSummaryLine.Add(TaxableAmountLine);
            }


            //------Signer-------------------

            XElement FirstName = new XElement("FirstName",SignerInfo[0]);
            XElement LastName = new XElement("LastName", SignerInfo[1]);
            XElement PatronymicName = new XElement("PatronymicName", SignerInfo[2]);

            Signer.Add(FirstName);
            Signer.Add(LastName);
            Signer.Add(PatronymicName);


            //------сохранение документа-----------
            string dd = DateTime.Today.ToString(@"yyyyMMdd_") + DateTime.Now.ToString(@"HHmmssff_");
            string nameInv = "INVOIC_" + dd + CurrDataInvoice[5] + ".xml";
            try
            {
                xdoc.Save(InvoiceEDISOFT + nameInv);
                xdoc.Save(ArchiveEDISOFT + nameInv);
                string message = "EDISOFT.Счет-Фактура " + nameInv + " создана в " + InvoiceEDISOFT;
                Program.WriteLine(message);
                DispOrders.WriteInvoiceLog(Convert.ToString(PlatInfo[0]) + " - " + Convert.ToString(PlatInfo[1]), Convert.ToString(DelivInfo[0]) + " - " + Convert.ToString(DelivInfo[1]), nameInv, Convert.ToString(CurrDataInvoice[5]), 0 ,message, DateTime.Now);
                DispOrders.WriteProtocolEDI("Счет фактура", nameInv, PlatInfo[0] + " - " + PlatInfo[1], 0, DelivInfo[0] + " - " + DelivInfo[1], "Счет фактура сформирована", DateTime.Now, Convert.ToString(CurrDataInvoice[5]), "EDISOFT");
                ReportEDI.RecordCountEDoc("EDI-Софт", "Invoice", 1);
                //запись в лог отправки  СФ
                //int CorSf = Convert.ToInt32(CurrDataInvoice[14]);
                string Doc;
                if (KSF == false)//СФ
                {
                    Doc = "5";
                }
                else//КСФ
                {
                    Doc = "9";
                }
                DispOrders.WriteEDiSentDoc(Doc, nameInv, Convert.ToString(CurrDataInvoice[1]), Convert.ToString(CurrDataInvoice[2]), "123", Convert.ToString(CurrDataInvoice[10]), Convert.ToString(CurrDataInvoice[5]), 1);
            }
            catch(IOException e)
            {
                string message_error = "EDISOFT. Не могу создать xml файл Счет-Фактуры в " + InvoiceEDISOFT + ". Нет доступа или диск переполнен.";
                Program.WriteLine(e.Message);
                DispOrders.WriteInvoiceLog(Convert.ToString(PlatInfo[0]) + " - " + Convert.ToString(PlatInfo[1]), Convert.ToString(DelivInfo[0]) + " - " + Convert.ToString(DelivInfo[1]), nameInv, Convert.ToString(CurrDataInvoice[5]), 10, message_error, DateTime.Now);
                DispOrders.WriteProtocolEDI("Счет фактура", nameInv, PlatInfo[0] + " - " + PlatInfo[1], 10, DelivInfo[0] + " - " + DelivInfo[1], "Счет фактура не сформирована. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataInvoice[5]), "EDISOFT");
                Program.WriteLine(message_error);
                DispOrders.WriteErrorLog(e.Message);
                //запись в лог о неудаче
            }
            
        }

        public static void CreateKonturInvoiceOld(List<object> CurrDataInvoice)
        {
            //получение путей
            string ArchiveKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.АРХИВ");

            //string InvoiceKONTUR = "\\\\fileshare\\EXPIMP\\OrderIntake\\SKBKONTUR\\OUTBOX\\";//test
            //string ArchiveKONTUR = "\\\\fileshare\\EXPIMP\\OrderIntake\\SKBKONTUR\\ARCHIVE\\";

            //генерация имени файла.
            string id = Convert.ToString(Guid.NewGuid()); ;
            string nameInv = "INVOIC_" + id + ".xml";

            //получение данных
            object[] DelivInfo = Verifiacation.GetDataFromPtnCD(Convert.ToString(CurrDataInvoice[8]));
            object[] PlatInfo = Verifiacation.GetDataFromPtnCD(Convert.ToString(CurrDataInvoice[9]));

            //какой gln номер использовать
            bool UseMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(DelivInfo[8]));
            string ILN_Edi;
            string InvoiceKONTUR;
            object[] FirmInfo;
            object[] FirmAdr;
            if (UseMasterGLN == false)//используем данные текущего предприятия
            {
                ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ILN_EDI");
                InvoiceKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.СФ");
                FirmInfo = Verifiacation.GetFirmInfo();
                FirmAdr = Verifiacation.GetFirmAdr();

            }
            else//используем данные головного предприятия
            {
                ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");
                FirmInfo = Verifiacation.GetMasterFirmInfo();
                FirmAdr = Verifiacation.GetMasterFirmAdr();
                try
                {
                    InvoiceKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.ЭКСПОРТ");
                }
                catch
                {
                    InvoiceKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.СФ");
                }

            }
            Program.WriteLine("До" + InvoiceKONTUR);
            int CntLinesInvoice = Verifiacation.CountItemsInOrder(Convert.ToString(CurrDataInvoice[1]), 5);
            object[,] Item = DispOrders.GetItemsFromTrdS(Convert.ToString(CurrDataInvoice[1]), CntLinesInvoice, 5);

            object[] Total = DispOrders.GetTotal(Convert.ToString(CurrDataInvoice[1]), 17);

            object[] SignerInfo = Verifiacation.GetSigner();

            XDocument xdoc = new XDocument();

            //основные элементы (1 уровень)
            XElement eDIMessage = new XElement("eDIMessage");
            XElement interchangeHeader = new XElement("interchangeHeader");
            XElement invoice = new XElement("invoice");
            XAttribute numberInvoice = new XAttribute("number",CurrDataInvoice[2]);
            XAttribute dateInvoice = new XAttribute("date", (Convert.ToDateTime(CurrDataInvoice[3])).ToString("yyyy-MM-dd"));
            XAttribute idMessage = new XAttribute("id", id);
            XAttribute creationDateTime = new XAttribute("creationDateTime", (DateTime.Now).ToString("yyyy-MM-dd HH:mm:ss"));

            xdoc.Add(eDIMessage);
            eDIMessage.Add(interchangeHeader);
            eDIMessage.Add(invoice);
            eDIMessage.Add(idMessage);
            eDIMessage.Add(creationDateTime);
            
            invoice.Add(numberInvoice);
            invoice.Add(dateInvoice);



            //------interchangeHeader---------------
            XElement sender = new XElement("sender",ILN_Edi);
            XElement recipient = new XElement("recipient",PlatInfo[2]);
            XElement documentType = new XElement("documentType", "INVOIC");

            interchangeHeader.Add(sender);
            interchangeHeader.Add(recipient);
            interchangeHeader.Add(documentType);

            //------invoice------------------------
            XElement originOrder = new XElement("originOrder");
            XAttribute numberoriginOrder = new XAttribute("number", CurrDataInvoice[5]);
            XAttribute dateoriginOrder = new XAttribute("date",CurrDataInvoice[6]);

            XElement despatchIdentificator = new XElement("despatchIdentificator");
            XAttribute numberdespatchIdentificator = new XAttribute("number", CurrDataInvoice[12]);
            XAttribute datedespatchIdentificator = new XAttribute("date",CurrDataInvoice[3]);

            XElement seller = new XElement("seller");
            XElement buyer = new XElement("buyer");
            XElement invoicee = new XElement("invoicee");
            XElement deliveryInfo = new XElement("deliveryInfo");
            XElement lineItems = new XElement("lineItems");

            invoice.Add(originOrder);
            invoice.Add(despatchIdentificator);
            invoice.Add(seller);
            invoice.Add(buyer);
            invoice.Add(invoicee);
            invoice.Add(deliveryInfo);
            invoice.Add(lineItems);

            originOrder.Add(numberoriginOrder);
            originOrder.Add(dateoriginOrder);

            despatchIdentificator.Add(numberdespatchIdentificator);
            despatchIdentificator.Add(datedespatchIdentificator);

            //--------seller-----------------------
            XElement gln = new XElement("gln", ILN_Edi);
           // XElement organization = new XElement("organization");
           // XElement russianAddress = new XElement("russianAddress");

            seller.Add(gln);
           // seller.Add(organization);
           // seller.Add(russianAddress);

            //--------organization------------------
            //XElement name = new XElement("name", FirmInfo[0]);
            //XElement inn = new XElement("inn", FirmInfo[1]);
            //XElement kpp = new XElement("kpp", FirmInfo[2]);

            //organization.Add(name);
            //organization.Add(inn);
            //organization.Add(kpp);

            //---------russianAddress----------------
            //XElement city = new XElement("city", Convert.ToString(FirmAdr[1]));
            //XElement street = new XElement("street", Convert.ToString(FirmAdr[0]));
           // XElement regionISOCode = new XElement("regionISOCode", "RU-TYU");
            //XElement postalCode = new XElement("postalCode", Convert.ToString(FirmAdr[3]));

           // russianAddress.Add(city);
           // russianAddress.Add(street);
           // russianAddress.Add(regionISOCode);
           // russianAddress.Add(postalCode);

            //--------buyer------------------
            XElement glnbuyer = new XElement("gln", PlatInfo[2]);
            //XElement organizationbuyer = new XElement("organization");
            //XElement russianAddressbuyer = new XElement("russianAddress");

            buyer.Add(glnbuyer);
            //buyer.Add(organizationbuyer);
            //buyer.Add(russianAddressbuyer);

            //--------organization-buyer------------------
           // XElement namebuyer = new XElement("name", PlatInfo[1]);
           // XElement innbuyer = new XElement("inn", PlatInfo[3]);
           // XElement kppbuyer = new XElement("kpp", PlatInfo[4]);

           // organizationbuyer.Add(namebuyer);
           // organizationbuyer.Add(innbuyer);
           // organizationbuyer.Add(kppbuyer);

            //-------russianAddress-buyer---------------------------
         //   XElement regionISOCodebuyer = new XElement("regionISOCode", "RU-TYU");
         //   russianAddressbuyer.Add(regionISOCodebuyer);

            //--------invoicee------------------
            XElement glninvoicee = new XElement("gln", PlatInfo[2]);
            //XElement organizationinvoicee = new XElement("organization");
            //XElement russianAddressinvoicee = new XElement("russianAddress");

            invoicee.Add(glninvoicee);
           // invoicee.Add(organizationinvoicee);
           // invoicee.Add(russianAddressinvoicee);

            //--------organization-invoicee------------------
          //  XElement nameinvoicee = new XElement("name", PlatInfo[1]);
          //  XElement inninvoicee = new XElement("inn", PlatInfo[3]);
          //  XElement kppinvoicee = new XElement("kpp", PlatInfo[4]);

          //  organizationinvoicee.Add(nameinvoicee);
          //  organizationinvoicee.Add(inninvoicee);
          //  organizationinvoicee.Add(kppinvoicee);

            //-------russianAddress-invoicee---------------------------
           // XElement regionISOCodeinvoicee = new XElement("regionISOCode", "RU-TYU");
           // russianAddressinvoicee.Add(regionISOCodeinvoicee);

            //---------deliveryInfo------------------------------------
            XElement estimatedDeliveryDateTime = new XElement("estimatedDeliveryDateTime", CurrDataInvoice[15]);
            XElement shipFrom = new XElement("shipFrom");
            XElement shipTo = new XElement("shipTo");

            deliveryInfo.Add(estimatedDeliveryDateTime);
            deliveryInfo.Add(shipFrom);
            deliveryInfo.Add(shipTo);

            //---------shipFrom----------------------
            XElement glnFrom = new XElement("gln", ILN_Edi);
           // XElement organizationFrom = new XElement("organization");
           // XElement russianAddressFrom = new XElement("russianAddress");

            shipFrom.Add(glnFrom);
            //shipFrom.Add(organizationFrom);
           // shipFrom.Add(russianAddressFrom);

            //--------organization------------------
           // XElement nameFrom = new XElement("name", FirmInfo[0]);
           // XElement innFrom = new XElement("inn", FirmInfo[1]);
           // XElement kppFrom = new XElement("kpp", FirmInfo[2]);

           // organizationFrom.Add(name);
           // organizationFrom.Add(inn);
           // organizationFrom.Add(kpp);

            //---------russianAddress----------------
          //  XElement cityFrom = new XElement("city", Convert.ToString(FirmAdr[1]));
          //  XElement streetFrom = new XElement("street", Convert.ToString(FirmAdr[0]));
          //  XElement regionISOCodeFrom = new XElement("regionISOCode", "RU-TYU");
          //  XElement postalCodeFrom = new XElement("postalCode", Convert.ToString(FirmAdr[3]));

          //  russianAddressFrom.Add(cityFrom);
          //  russianAddressFrom.Add(streetFrom);
         //   russianAddressFrom.Add(regionISOCodeFrom);
          //  russianAddressFrom.Add(postalCodeFrom);
            
            //---------ShipTo-------------------------
            XElement glnTo = new XElement("gln", DelivInfo[2]);
           // XElement organizationTo = new XElement("organization");
           // XElement russianAddressTo = new XElement("russianAddress");

            shipTo.Add(glnTo);
           // shipTo.Add(organizationTo);
           // shipTo.Add(russianAddressTo);


            //--------organization------------------
           // XElement nameTo = new XElement("name", PlatInfo[1]);
           // XElement innTo = new XElement("inn", PlatInfo[3]);
           // XElement kppTo = new XElement("kpp", PlatInfo[4]);

           // organizationTo.Add(nameTo);
           // organizationTo.Add(innTo);
           // organizationTo.Add(kppTo);

            //---------russianAddress----------------
//            XElement regionISOCodeTo = new XElement("regionISOCode", "RU-TYU");

 //           russianAddressTo.Add(regionISOCodeTo);

            //-----------lineItems--------------------
            XElement currencyISOCode = new XElement("currencyISOCode", "RUB");
            
            XElement totalSumExcludingTaxes = new XElement("totalSumExcludingTaxes", Total[5]);//без ндс
            XElement totalVATAmount = new XElement("totalVATAmount",Convert.ToDecimal(Total[4])-Convert.ToDecimal(Total[5]));//ндс
            XElement totalAmount = new XElement("totalAmount",Total[4]);//c ндс

            lineItems.Add(currencyISOCode);

            //----------lineItem--------------------------
            for (int i = 0; i < CntLinesInvoice; i++)
            {
                XElement LineItem = new XElement("lineItem");

                lineItems.Add(LineItem);

                object[] BICode = Verifiacation.GetBuyerItemCode(Convert.ToString(PlatInfo[5]), Convert.ToString(Item[i, 1]));

                XElement orderLineNumber = new XElement("orderLineNumber", i + 1);
                XElement gtin = new XElement("gtin", Item[i, 0]);
                XElement internalSupplierCode = new XElement("internalSupplierCode", Item[i, 2]);
                XElement internalBuyerCode = new XElement("internalBuyerCode", BICode[0]);
                XElement description = new XElement("description", Item[i, 3]);
                XElement quantity = new XElement("quantity", Item[i, 4]);
                XAttribute unitOfMeasure = new XAttribute("unitOfMeasure", Item[i, 7]);
                XElement netPrice = new XElement("netPrice", Item[i, 5]);
                XElement netPriceWithVAT = new XElement("netPriceWithVAT", Item[i, 6]);
                XElement netAmount = new XElement("netAmount", Item[i, 12]);
                XElement vATRate = new XElement("vATRate", Convert.ToInt32(Item[i, 9]));
                XElement vATAmount = new XElement("vATAmount", Item[i, 11]);
                XElement amount = new XElement("amount", Convert.ToDecimal(Item[i, 11]) + Convert.ToDecimal(Item[i, 12]));

                XElement SupplierItemCode = new XElement("SupplierItemCode", Item[i, 2]);
                XElement InvoiceUnitNetPrice = new XElement("InvoiceUnitNetPrice", Item[i, 5]);
                XElement InvoiceUnitGrossPrice = new XElement("InvoiceUnitGrossPrice", Item[i, 6]);
               

                LineItem.Add(gtin);
                LineItem.Add(internalBuyerCode);
                LineItem.Add(internalSupplierCode);
                LineItem.Add(orderLineNumber);
                LineItem.Add(description);
                LineItem.Add(quantity);
                quantity.Add(unitOfMeasure);
                LineItem.Add(netPrice);
                LineItem.Add(netPriceWithVAT);
                LineItem.Add(netAmount);
                LineItem.Add(vATRate);
                LineItem.Add(vATAmount);
                LineItem.Add(amount);

            }
            lineItems.Add(totalSumExcludingTaxes);
            lineItems.Add(totalVATAmount);
            lineItems.Add(totalAmount);
            
            //------сохранение документа-----------
            try
            {
                xdoc.Save(InvoiceKONTUR + nameInv);
                xdoc.Save(ArchiveKONTUR + nameInv);
               // Console.WriteLine("После" + InvoiceKONTUR);
                string message = "СКБ-Контур. Счет-Фактура " + nameInv + " создана";
               // Console.WriteLine(message);
                DispOrders.WriteInvoiceLog(Convert.ToString(PlatInfo[0]) + " - " + Convert.ToString(PlatInfo[1]), Convert.ToString(DelivInfo[0]) + " - " + Convert.ToString(DelivInfo[1]), nameInv, Convert.ToString(CurrDataInvoice[5]), 0, message, DateTime.Now);
                DispOrders.WriteProtocolEDI("Счет фактура", nameInv, PlatInfo[0] + " - " + PlatInfo[1], 0, DelivInfo[0] + " - " + DelivInfo[1], "Счет фактура сформирована", DateTime.Now, Convert.ToString(CurrDataInvoice[5]), "СКБ-Контур");
                ReportEDI.RecordCountEDoc("СКБ-Контур", "Invoice", 1);
                //запись в лог отправки  СФ
                int CorSf = Convert.ToInt32(CurrDataInvoice[14]);
                string Doc;
                if (CorSf == 0)//СФ
                {
                    Doc = "5"; 
                }
                else//КСФ
                {
                    Doc = "9";
                }
                DispOrders.WriteEDiSentDoc(Doc, nameInv, Convert.ToString(CurrDataInvoice[1]), Convert.ToString(CurrDataInvoice[2]), "123", Convert.ToString(CurrDataInvoice[10]), Convert.ToString(CurrDataInvoice[5]), 1);
            }
            catch(Exception e)
            {
                Program.WriteLine(e.Message);
                string message_error = "СКБ-Контур. Не могу создать xml файл Счет-Фактуры в " + InvoiceKONTUR + ". Нет доступа или диск переполнен.";
                DispOrders.WriteInvoiceLog(Convert.ToString(PlatInfo[0]) + " - " + Convert.ToString(PlatInfo[1]), Convert.ToString(DelivInfo[0]) + " - " + Convert.ToString(DelivInfo[1]), nameInv, Convert.ToString(CurrDataInvoice[5]), 10, message_error, DateTime.Now);
                DispOrders.WriteProtocolEDI("Счет фактура", nameInv, PlatInfo[0] + " - " + PlatInfo[1], 10, DelivInfo[0] + " - " + DelivInfo[1], "Счет фактура не сформирована. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataInvoice[5]), "СКБ-Контур");
                Program.WriteLine(message_error);
                //запись в лог о неудаче
            }
            
        }

        public static void CreateKonturInvoice(List<object> CurrDataInvoice)
        {
            if (Convert.ToInt32(CurrDataInvoice[14]) == 0) //КСФ исключаем
            {
                //признак необходимости ISOCode
                bool iso = false;

                //получение путей
                string ArchiveKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.АРХИВ");

                //string InvoiceKONTUR = "\\\\fileshare\\EXPIMP\\OrderIntake\\SKBKONTUR\\OUTBOX\\";//test
                //string ArchiveKONTUR = "\\\\fileshare\\EXPIMP\\OrderIntake\\SKBKONTUR\\ARCHIVE\\";

                //генерация имени файла.
                string id; //= Convert.ToString(DateTime.Now); ;   // ну это так, ищу замену    
                string id2 = Convert.ToString(Guid.NewGuid()); ;       // эта падла иногда генерит повторно тот же ИД подряд !!!!!!!!!   и как-то по утрам, потом нормально работает ... 
                id = Convert.ToString(Guid.NewGuid());       // эта падла иногда генерит повторно тот же ИД подряд !!!!!!!!!   и как-то по утрам, потом нормально работает ... 
                if (id == id2)                                  // ну или другая фигня. не понятно. такие файлики уже есть...
                    id = id + "_";
                string nameInv = "INVOIC_" + id + ".xml";

                //получение данных
                object[] DelivInfo = Verifiacation.GetDataFromPtnCD(Convert.ToString(CurrDataInvoice[8]));   //10 = формат
                object[] PlatInfo = Verifiacation.GetDataFromPtnCD(Convert.ToString(CurrDataInvoice[9]));
                object KonturPlatGLN = Verifiacation.GetGLNGR(Convert.ToString(CurrDataInvoice[9]));                                        //  глн группы
                if (String.IsNullOrEmpty(KonturPlatGLN.ToString()) || KonturPlatGLN.ToString().Equals("0")) KonturPlatGLN = PlatInfo[2];    //  если глн группы отсутствует, то просто глн

                //какой gln номер использовать
                string check = Convert.ToString(DelivInfo[8]);
                bool UseMasterGLN = false;
                if (check.Length != 0)
                {
                    UseMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(DelivInfo[8]));
                }
                //bool UseMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(DelivInfo[8]));
                string ILN_Edi, ILN_Edi_S;
                string InvoiceKONTUR;
                object[] FirmInfo, FirmInfo_G;
                object[] FirmAdr, FirmAdr_G;
                object[] FirmInfoGrOt; //данные грузоотправителя
                object[] infoFirmAdrGrOt = Verifiacation.GetFirmAdr();  //адрес грузоотправителя

                if (UseMasterGLN == true) //используем данные головного предприятия
                {
                    ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");

                    FirmInfo = Verifiacation.GetMasterFirmInfo();
                    FirmAdr = Verifiacation.GetMasterFirmAdr();
                    try
                    {
                        InvoiceKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.ЭКСПОРТ");
                    }
                    catch
                    {
                        InvoiceKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.СФ");
                    }
                    if (Convert.ToDateTime(PlatInfo[9]) > Convert.ToDateTime(CurrDataInvoice[3]))      // 13 это дата с которой надо ставить новые данные
                    {
                        FirmInfoGrOt = Verifiacation.GetFirmInfo("20171130"); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO        // берём как до 01.12.2017
                    }
                    else
                    {
                        FirmInfoGrOt = Verifiacation.GetFirmInfo(Convert.ToDateTime(CurrDataInvoice[3]).ToString("yyyyMMdd"));
                    }
                }
                else//используем данные текущего предприятия
                {
                    ILN_Edi = DispOrders.GetValueOption("СКБ-КОНТУР.ИЛН_ПРЕДПРИЯТИЯ");
                    if (ILN_Edi == "") ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ИЛН");
                    InvoiceKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.СФ");
                    if (Convert.ToDateTime(PlatInfo[9]) > Convert.ToDateTime(CurrDataInvoice[3]))      // 13 это дата с которой надо ставить новые данные
                    {
                        FirmInfo = Verifiacation.GetFirmInfo("20171130"); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO        // берём как до 01.12.2017
                    }
                    else
                    {
                        FirmInfo = Verifiacation.GetFirmInfo(Convert.ToDateTime(CurrDataInvoice[3]).ToString("yyyyMMdd"));
                    }
                    FirmInfoGrOt = FirmInfo;
                    FirmAdr = Verifiacation.GetFirmAdr();
                }

                ILN_Edi_S = DispOrders.GetValueOption("СКБ-КОНТУР.ИЛН_ПРЕДПРИЯТИЯ");
                FirmInfo_G = Verifiacation.GetMasterFirmInfo();
                FirmAdr_G = Verifiacation.GetMasterFirmAdr();

                //костыли для азбуки вкуса
                iso = true;
                string sellerISOCode;
                sellerISOCode = "RU-CHE";

                if (File.Exists(InvoiceKONTUR + nameInv))          // уже есть такие файлы??? почему-то не может сохранить
                    nameInv = nameInv = "INVOIC_" + id + "_.xml"; ;

                int CntLinesInvoice = Verifiacation.CountItemsInInvoice(Convert.ToString(CurrDataInvoice[1]));

                bool PCE = Verifiacation.UsePCE(Convert.ToString(CurrDataInvoice[9]));//проверка на использование штук в исходящих докуметов

                object[,] Item = DispOrders.GetItemsFromInvoice(Convert.ToString(CurrDataInvoice[1]), CntLinesInvoice, PCE);

                object[] Total = DispOrders.GetTotal(Convert.ToString(CurrDataInvoice[1]), 5);

                //object[] SignerInfo = Verifiacation.GetSigner();

                XDocument xdoc = new XDocument();

                //основные элементы (1 уровень)
                XElement eDIMessage = new XElement("eDIMessage");
                XElement interchangeHeader = new XElement("interchangeHeader");
                XElement invoice = new XElement("invoice");
                XAttribute numberInvoice = new XAttribute("number", CurrDataInvoice[2]);
                XAttribute dateInvoice = new XAttribute("date", (Convert.ToDateTime(CurrDataInvoice[3])).ToString("yyyy-MM-dd"));
                XAttribute TypeInvoice = new XAttribute("type", "Original");
                XAttribute idMessage = new XAttribute("id", id);
                XAttribute creationDateTime = new XAttribute("creationDateTime", (DateTime.Now).ToString("yyyy-MM-dd HH:mm:ss"));

                xdoc.Add(eDIMessage);
                eDIMessage.Add(interchangeHeader);
                eDIMessage.Add(invoice);
                eDIMessage.Add(idMessage);
                eDIMessage.Add(creationDateTime);

                invoice.Add(numberInvoice);
                invoice.Add(dateInvoice);
                invoice.Add(TypeInvoice);  // добавлено по структуре версии 1,5,26

                //------interchangeHeader---------------
                XElement sender = new XElement("sender", ILN_Edi);
                XElement recipient = new XElement("recipient", KonturPlatGLN);            //глн группы либо просто глн, раннее PlatInfo[2]
                XElement documentType = new XElement("documentType", "INVOIC");

                interchangeHeader.Add(sender);
                interchangeHeader.Add(recipient);
                interchangeHeader.Add(documentType);

                //------invoice------------------------
                XElement originOrder = new XElement("originOrder");
                invoice.Add(originOrder);
                XAttribute numberoriginOrder = new XAttribute("number", CurrDataInvoice[5]);
                XAttribute dateoriginOrder;

                //дату заказа берем из даты документа заказа, если вывалилась ошибка, значит заказ был сделан вручную, дату заказа проставляем как в в заказе ИСПРО
                try
                {
                    Program.WriteLine("дата документа заказа " + Convert.ToDateTime(Verifiacation.GetFldFromEdiExch(Convert.ToInt64(CurrDataInvoice[20]), "Exch_OrdDocDat")).ToString("yyyy-MM-dd"));
                    //dateoriginOrder = new XAttribute("date", Convert.ToDateTime(CurrDataInvoice[6]).ToString("yyyy-MM-dd"));
                    dateoriginOrder = new XAttribute("date", Convert.ToDateTime(Verifiacation.GetFldFromEdiExch(Convert.ToInt64(CurrDataInvoice[20]), "Exch_OrdDocDat")).ToString("yyyy-MM-dd"));
                }
                catch
                {
                    dateoriginOrder = new XAttribute("date", Convert.ToDateTime(CurrDataInvoice[6]).ToString("yyyy-MM-dd"));
                }

                XElement despatchIdentificator = new XElement("despatchIdentificator");
                invoice.Add(despatchIdentificator);
                XAttribute numberdespatchIdentificator = new XAttribute("number", CurrDataInvoice[12]);
                XAttribute datedespatchIdentificator;
                datedespatchIdentificator = new XAttribute("date", Convert.ToDateTime(CurrDataInvoice[3]).ToString("yyyy-MM-dd"));

                object[] infoRecAdv = Verifiacation.GetRecAdvInfo(Convert.ToInt64(CurrDataInvoice[20]));

                if (infoRecAdv[0] != null && Convert.ToString(infoRecAdv[0]) != "")  // это теперь есть во вьюхе, CurrDataInvoice[21],22
                {
                    XElement receivingIdentificator = new XElement("receivingIdentificator");
                    XAttribute numberreceivingIdentificator = new XAttribute("number", infoRecAdv[0]);
                    XAttribute datereceivingIdentificator;
                    datereceivingIdentificator = new XAttribute("date", Convert.ToDateTime(infoRecAdv[1]).ToString("yyyy-MM-dd"));

                    invoice.Add(receivingIdentificator);
                    receivingIdentificator.Add(numberreceivingIdentificator);
                    receivingIdentificator.Add(datereceivingIdentificator);
                }

                // contractIdentificator = номер контракта и дата при наличии
                object[] ContractInfoInfo = Verifiacation.GetContractInfo(Convert.ToString(CurrDataInvoice[8])); //контракты
                if (ContractInfoInfo[0] != null && Convert.ToString(ContractInfoInfo[0]) != "нет данных")
                {
                    XElement contractIdentificator = new XElement("contractIdentificator");
                    XAttribute numbercontractIdentificator = new XAttribute("number", ContractInfoInfo[0]);
                    XAttribute DatecontractIdentificator = new XAttribute("date", Convert.ToDateTime(ContractInfoInfo[1]).ToString("yyyy-MM-dd"));
                    invoice.Add(contractIdentificator);
                    contractIdentificator.Add(numbercontractIdentificator);
                    contractIdentificator.Add(DatecontractIdentificator);
                }

                XElement seller = new XElement("seller");
                XElement buyer = new XElement("buyer");
                XElement invoicee = new XElement("invoicee");
                XElement deliveryInfo = new XElement("deliveryInfo");
                XElement lineItems = new XElement("lineItems");

                invoice.Add(seller);
                invoice.Add(buyer);
                invoice.Add(invoicee);
                invoice.Add(deliveryInfo);
                invoice.Add(lineItems);

                originOrder.Add(numberoriginOrder);
                originOrder.Add(dateoriginOrder);

                despatchIdentificator.Add(numberdespatchIdentificator);
                despatchIdentificator.Add(datedespatchIdentificator);

                //--------seller-----------------------
                XElement gln = new XElement("gln", ILN_Edi);
                seller.Add(gln);

                /*    if (iso == true)//костыли для азбуки вкуса
                    {
                        XElement organization = new XElement("organization");
                        XElement russianAddress = new XElement("russianAddress");

                        seller.Add(organization);
                        seller.Add(russianAddress);
                        //--------organization------------------
                        XElement name = new XElement("name", FirmInfo[0]);
                        XElement inn = new XElement("inn", FirmInfo[1]);
                        XElement kpp = new XElement("kpp", FirmInfo[2]);

                        organization.Add(name);
                        organization.Add(inn);
                        organization.Add(kpp);

                        //---------russianAddress----------------
                        XElement city = new XElement("city", Convert.ToString(FirmAdr[1]));
                        XElement street = new XElement("street", Convert.ToString(FirmAdr[0]));
                        XElement regionISOCode = new XElement("regionISOCode", sellerISOCode);
                        XElement postalCode = new XElement("postalCode", Convert.ToString(FirmAdr[3]));

                        russianAddress.Add(city);
                        russianAddress.Add(street);
                        russianAddress.Add(regionISOCode);
                        russianAddress.Add(postalCode);
                    }
                    else
                    {*/

                XElement organization = new XElement("organization");
                XElement russianAddress = new XElement("russianAddress");

                seller.Add(organization);
                seller.Add(russianAddress);

                if (UseMasterGLN == false)
                    {
                        
                        //--------organization------------------
                        XElement name = new XElement("name", "АО \"Группа Компаний \"Российское Молоко\" (АО \"ГК \"РОСМОЛ\")");
                        XElement inn = new XElement("inn", FirmInfo_G[1]);
                        XElement kpp = new XElement("kpp", FirmInfo[2]);

                        organization.Add(name);
                        organization.Add(inn);
                        organization.Add(kpp);

                        //---------russianAddress----------------
                        //sellerISOCode = "RU-CHE";
                        XElement city = new XElement("city", Convert.ToString(FirmAdr_G[1]));
                        XElement street = new XElement("street", Convert.ToString(FirmAdr_G[0]));
                        XElement regionISOCode = new XElement("regionISOCode", sellerISOCode);
                        XElement postalCode = new XElement("postalCode", Convert.ToString(FirmAdr_G[3]));

                        russianAddress.Add(city);
                        russianAddress.Add(street);
                        russianAddress.Add(regionISOCode);
                        russianAddress.Add(postalCode);
                    }
                else
                    {
                        //--------organization------------------
                        XElement nameFrom = new XElement("name", FirmInfo[0]);
                        XElement innFrom = new XElement("inn", FirmInfo[1]);
                        XElement kppFrom = new XElement("kpp", FirmInfo[2]);

                        organization.Add(nameFrom);
                        organization.Add(innFrom);
                        organization.Add(kppFrom);

                        //---------russianAddress----------------
                        XElement cityFrom = new XElement("city", Convert.ToString(FirmAdr[1]));
                        XElement streetFrom = new XElement("street", Convert.ToString(FirmAdr[0]));
                        XElement regionISOCodeFrom = new XElement("regionISOCode", sellerISOCode);
                        XElement postalCodeFrom = new XElement("postalCode", Convert.ToString(FirmAdr[3]));

                        russianAddress.Add(cityFrom);
                        russianAddress.Add(streetFrom);
                        russianAddress.Add(regionISOCodeFrom);
                        russianAddress.Add(postalCodeFrom);
                    }
                //}

                //--------buyer------------------
                XElement glnbuyer = new XElement("gln", KonturPlatGLN);       //глн группы либо просто глн, ранее PlatInfo[2]
                buyer.Add(glnbuyer);
                string BuyerISOCode;

                /*if (iso == true)//костыли для азбуки вкуса
                {
                    XElement organizationbuyer = new XElement("organization");
                    XElement russianAddressbuyer = new XElement("russianAddress");

                    buyer.Add(organizationbuyer);
                    buyer.Add(russianAddressbuyer);

                    //--------organization-buyer------------------
                    XElement namebuyer = new XElement("name", PlatInfo[1]);
                    XElement innbuyer = new XElement("inn", PlatInfo[3]);
                    XElement kppbuyer = new XElement("kpp", PlatInfo[4]);

                    organizationbuyer.Add(namebuyer);
                    organizationbuyer.Add(innbuyer);
                    organizationbuyer.Add(kppbuyer);

                    //-------russianAddress-buyer---------------------------
                    BuyerISOCode = DispOrders.GetISOCode(Convert.ToString(PlatInfo[0]));
                    XElement regionISOCodebuyer = new XElement("regionISOCode", BuyerISOCode);
                    russianAddressbuyer.Add(regionISOCodebuyer);
                }*/

                //--------invoicee------------------
                XElement glninvoicee = new XElement("gln", KonturPlatGLN);     //глн группы либо просто глн, ранее PlatInfo[2]
                invoicee.Add(glninvoicee);

                /*if (iso == true)//костыли для азбуки вкуса
                {
                    XElement organizationinvoicee = new XElement("organization");
                    XElement russianAddressinvoicee = new XElement("russianAddress");


                    invoicee.Add(organizationinvoicee);
                    invoicee.Add(russianAddressinvoicee);

                    //--------organization-invoicee------------------
                    XElement nameinvoicee = new XElement("name", PlatInfo[1]);
                    XElement inninvoicee = new XElement("inn", PlatInfo[3]);
                    XElement kppinvoicee = new XElement("kpp", PlatInfo[4]);

                    organizationinvoicee.Add(nameinvoicee);
                    organizationinvoicee.Add(inninvoicee);
                    organizationinvoicee.Add(kppinvoicee);

                    //-------russianAddress-invoicee---------------------------
                    BuyerISOCode = DispOrders.GetISOCode(Convert.ToString(PlatInfo[0]));
                    XElement regionISOCodeinvoicee = new XElement("regionISOCode", BuyerISOCode);
                    russianAddressinvoicee.Add(regionISOCodeinvoicee);
                }*/

                //---------deliveryInfo------------------------------------
                XElement estimatedDeliveryDateTime = new XElement("estimatedDeliveryDateTime", CurrDataInvoice[15]);
                XElement actualDeliveryDateTime = new XElement("actualDeliveryDateTime", CurrDataInvoice[15]);
                XElement shipFrom = new XElement("shipFrom");
                XElement shipTo = new XElement("shipTo");

                deliveryInfo.Add(estimatedDeliveryDateTime);
                deliveryInfo.Add(actualDeliveryDateTime);
                deliveryInfo.Add(shipFrom);
                deliveryInfo.Add(shipTo);

                //---------shipFrom----------------------

                //Грузоотправитель всегда филиал
                XElement glnFrom = new XElement("gln", ILN_Edi_S);
                shipFrom.Add(glnFrom);

                if (UseMasterGLN == true)
                {
                    XElement organizationFrom = new XElement("organization");
                    XElement russianAddressFrom = new XElement("russianAddress");

                    shipFrom.Add(organizationFrom);
                    shipFrom.Add(russianAddressFrom);

                    //--------organization------------------
                    XElement nameFrom = new XElement("name", FirmInfoGrOt[0]);
                    XElement innFrom = new XElement("inn", FirmInfoGrOt[1]);
                    XElement kppFrom = new XElement("kpp", FirmInfoGrOt[2]);

                    organizationFrom.Add(nameFrom);
                    organizationFrom.Add(innFrom);
                    organizationFrom.Add(kppFrom);

                    //---------russianAddress----------------

                    if (infoFirmAdrGrOt[1].ToString() != "")
                    {
                        XElement cityFrom = new XElement("city", "г." + infoFirmAdrGrOt[1].ToString());      //город
                        russianAddressFrom.Add(cityFrom);
                    }

                    if (infoFirmAdrGrOt[5].ToString() != "")     
                    {
                       
                        XElement streetFrom = new XElement("street", "ул." + infoFirmAdrGrOt[0].ToString());      //улица + дом
                        russianAddressFrom.Add(streetFrom);
                    }

                    XElement regionISOCodeFrom = new XElement("regionISOCode", sellerISOCode);
                    russianAddressFrom.Add(regionISOCodeFrom);                                                      //ISO-code

                    if (infoFirmAdrGrOt[3].ToString() != "")                                                                
                    {
                        XElement postalCodeFrom = new XElement("postalCode", infoFirmAdrGrOt[3].ToString());
                        russianAddressFrom.Add(postalCodeFrom);                                                         //индекс
                    }

                }

                /*if (iso == true)//костыли для азбуки вкуса
                {
                    XElement organizationFrom = new XElement("organization");
                    XElement russianAddressFrom = new XElement("russianAddress");

                    shipFrom.Add(organizationFrom);
                    shipFrom.Add(russianAddressFrom);

                    //--------organization------------------
                    XElement nameFrom = new XElement("name", FirmInfo[0]);
                    XElement innFrom = new XElement("inn", FirmInfo[1]);
                    XElement kppFrom = new XElement("kpp", FirmInfo[2]);

                    organizationFrom.Add(nameFrom);
                    organizationFrom.Add(innFrom);
                    organizationFrom.Add(kppFrom);

                    //---------russianAddress----------------
                    XElement cityFrom = new XElement("city", Convert.ToString(FirmAdr[1]));
                    XElement streetFrom = new XElement("street", Convert.ToString(FirmAdr[0]));
                    XElement regionISOCodeFrom = new XElement("regionISOCode", sellerISOCode);
                    XElement postalCodeFrom = new XElement("postalCode", Convert.ToString(FirmAdr[3]));

                    russianAddressFrom.Add(cityFrom);
                    russianAddressFrom.Add(streetFrom);
                    russianAddressFrom.Add(regionISOCodeFrom);
                    russianAddressFrom.Add(postalCodeFrom);
                }*/

                //---------ShipTo-------------------------
                XElement glnTo = new XElement("gln", DelivInfo[2]);
                shipTo.Add(glnTo);

                /*if (iso == true)//костыли для азбуки вкуса
                {
                    XElement organizationTo = new XElement("organization");
                    XElement russianAddressTo = new XElement("russianAddress");

                    shipTo.Add(organizationTo);
                    shipTo.Add(russianAddressTo);

                    //--------organization------------------
                    XElement nameTo = new XElement("name", PlatInfo[1]);
                    XElement innTo = new XElement("inn", PlatInfo[3]);
                    XElement kppTo = new XElement("kpp", PlatInfo[4]);

                    organizationTo.Add(nameTo);
                    organizationTo.Add(innTo);
                    organizationTo.Add(kppTo);

                    //---------russianAddress----------------
                    string DelivISOCode = DispOrders.GetISOCode(Convert.ToString(DelivInfo[0]));
                    XElement regionISOCodeTo = new XElement("regionISOCode", DelivISOCode);
                    russianAddressTo.Add(regionISOCodeTo);
                }*/

                //-----------lineItems--------------------
                XElement currencyISOCode = new XElement("currencyISOCode", "RUB");

                XElement totalSumExcludingTaxes = new XElement("totalSumExcludingTaxes", Total[5]);//без ндс
                XElement totalVATAmount = new XElement("totalVATAmount", Convert.ToDecimal(Total[4]) - Convert.ToDecimal(Total[5]));//ндс
                XElement totalAmount = new XElement("totalAmount", Total[4]);//c ндс

                lineItems.Add(currencyISOCode);

                //----------lineItem--------------------------
                string EAN_F = "";
                for (int i = 0; i < CntLinesInvoice; i++)
                {
                    XElement LineItem = new XElement("lineItem");

                    lineItems.Add(LineItem);

                    object[] BICode = Verifiacation.GetBuyerItemCode(Convert.ToString(PlatInfo[5]), Convert.ToString(Item[i, 1]));
                    if (Convert.ToString(DelivInfo[10]) == "MDOU") // для садиков надо мнемокод
                        BICode[0] = Verifiacation.GetMnemoCode(Convert.ToString(Item[i, 0]), Convert.ToString(PlatInfo[8])); // это для садиков, мнемокод запихан сюда. берём его, если нет другого артикула покупателя. чтобы не испортить.
                    int quantityItem = 0;

                    XElement orderLineNumber = new XElement("orderLineNumber", i + 1);
                    EAN_F = Convert.ToString(Item[i, 0]).Substring(0, 13);  //Обрезаем штрих-код до 13 символов
                    //XElement gtin = new XElement("gtin", Item[i, 0]);
                    XElement gtin = new XElement("gtin", EAN_F);
                    XElement internalSupplierCode = new XElement("internalSupplierCode", Item[i, 2]);
                    XElement internalBuyerCode = new XElement("internalBuyerCode", BICode[0]);
                    XElement description = new XElement("description", Item[i, 3]);
                    XElement quantity;
                    XAttribute unitOfMeasure;
                    XElement netPrice;
                    XElement netPriceWithVAT;

                    if (Convert.ToString(DelivInfo[10]) == "MDOU")
                    {
                        quantity = new XElement("quantity", Item[i, 14]);
                        unitOfMeasure = new XAttribute("unitOfMeasure", "KGM");  // им ВСЁ надо в КГ
                        netPrice = new XElement("netPrice", (Convert.ToDecimal(Item[i, 12]) / Convert.ToDecimal(Item[i, 14])));
                        netPriceWithVAT = new XElement("netPriceWithVAT", ((Convert.ToDecimal(Item[i, 11]) + Convert.ToDecimal(Item[i, 12])) / Convert.ToDecimal(Item[i, 14])));
                        quantityItem = Convert.ToInt32(Item[i, 14]);
                    }
                    else
                    {
                        quantity = new XElement("quantity", Item[i, 4]);
                        unitOfMeasure = new XAttribute("unitOfMeasure", Item[i, 7]);
                        netPrice = new XElement("netPrice", Item[i, 5]);
                        netPriceWithVAT = new XElement("netPriceWithVAT", Item[i, 6]);
                        quantityItem = Convert.ToInt32(Item[i, 4]);
                    }

                    XElement netAmount = new XElement("netAmount", Item[i, 12]);
                    XElement vATRate = new XElement("vATRate", Convert.ToInt32(Item[i, 9]));
                    XElement vATAmount = new XElement("vATAmount", Item[i, 11]);
                    XElement amount = new XElement("amount", Convert.ToDecimal(Item[i, 11]) + Convert.ToDecimal(Item[i, 12]));

                    XElement SupplierItemCode = new XElement("SupplierItemCode", Item[i, 2]);
                    XElement InvoiceUnitNetPrice = new XElement("InvoiceUnitNetPrice", Item[i, 5]);
                    XElement InvoiceUnitGrossPrice = new XElement("InvoiceUnitGrossPrice", Item[i, 6]);

                    LineItem.Add(gtin);
                    LineItem.Add(internalBuyerCode);
                    LineItem.Add(internalSupplierCode);
                    LineItem.Add(orderLineNumber);
                    LineItem.Add(description);
                    LineItem.Add(quantity);
                    quantity.Add(unitOfMeasure);
                    LineItem.Add(netPrice);
                    LineItem.Add(netPriceWithVAT);
                    LineItem.Add(netAmount);
                    LineItem.Add(vATRate);
                    LineItem.Add(vATAmount);
                    LineItem.Add(amount);

                    if (Convert.ToString(DelivInfo[10]) == "BaseMark")
                    {
                        XElement controlIdentificationMarks;
                        XAttribute controlIdentificationMarks_type;
                        controlIdentificationMarks = new XElement("controlIdentificationMarks", "020" + EAN_F + "37" + Convert.ToString(quantityItem));
                        controlIdentificationMarks_type = new XAttribute("type", "Group");
                        LineItem.Add(controlIdentificationMarks);
                        controlIdentificationMarks.Add(controlIdentificationMarks_type);
                    }

                    if (Convert.ToString(DelivInfo[10]) == "MDOU")
                    {
                        XElement comment = new XElement("comment", BICode[0]);
                        LineItem.Add(comment);
                    }
                }
                lineItems.Add(totalSumExcludingTaxes);
                lineItems.Add(totalVATAmount);
                lineItems.Add(totalAmount);

                

                //------сохранение документа-----------
                try
                {
                    xdoc.Save(InvoiceKONTUR + nameInv);
                    xdoc.Save(ArchiveKONTUR + nameInv);
                    string message = "СКБ-Контур. Счет-Фактура " + nameInv + " создана в " + InvoiceKONTUR;
                    Program.WriteLine(message);
                    DispOrders.WriteInvoiceLog(Convert.ToString(PlatInfo[0]) + " - " + Convert.ToString(PlatInfo[1]), Convert.ToString(DelivInfo[0]) + " - " + Convert.ToString(DelivInfo[1]), nameInv, Convert.ToString(CurrDataInvoice[5]), 0, message, DateTime.Now);
                    DispOrders.WriteProtocolEDI("Счет фактура", nameInv, PlatInfo[0] + " - " + PlatInfo[1], 0, DelivInfo[0] + " - " + DelivInfo[1], "Счет фактура сформирована", DateTime.Now, Convert.ToString(CurrDataInvoice[5]), "KONTUR");
                    ReportEDI.RecordCountEDoc("СКБ-Контур", "Invoice", 1);
                    //запись в лог отправки  СФ
                    int CorSf = Convert.ToInt32(CurrDataInvoice[14]);
                    string Doc;
                    if (CorSf == 0)//СФ
                    {
                        Doc = "5";
                    }
                    else//КСФ
                    {
                        Doc = "9";
                    }
                    DispOrders.WriteEDiSentDoc(Doc, nameInv, Convert.ToString(CurrDataInvoice[1]), Convert.ToString(CurrDataInvoice[2]), "123", Convert.ToString(CurrDataInvoice[10]), Convert.ToString(CurrDataInvoice[20]),1);
                }
                catch
                {
                    string message_error = "СКБ-Контур. Не могу создать xml файл Счет-Фактуры в " + InvoiceKONTUR + ". Нет доступа или диск переполнен.";
                    DispOrders.WriteInvoiceLog(Convert.ToString(PlatInfo[0]) + " - " + Convert.ToString(PlatInfo[1]), Convert.ToString(DelivInfo[0]) + " - " + Convert.ToString(DelivInfo[1]), nameInv, Convert.ToString(CurrDataInvoice[5]), 10, message_error, DateTime.Now);
                    DispOrders.WriteProtocolEDI("Счет фактура", nameInv, PlatInfo[0] + " - " + PlatInfo[1], 10, DelivInfo[0] + " - " + DelivInfo[1], "Счет фактура не сформирована. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataInvoice[5]), "KONTUR");
                    Program.WriteLine(message_error);
                    //запись в лог о неудаче
                }
            }
        }

        public static void CreateEdiOrderSp(string PrdZkg_rcd)
        {
            //получение путей
            string ArchiveEDISOFT = DispOrders.GetValueOption("EDI-СОФТ.АРХИВ");

            //получение данных
            object[] headerSP = Verifiacation.GetHeaderSP(PrdZkg_rcd);
            object[] DelivInfo = Verifiacation.GetDataFromPtnCD(Convert.ToString(headerSP[6]));
            object[] PlatInfo = Verifiacation.GetDataFromPtnCD(Convert.ToString(headerSP[7]));
            int CntLinesSP = Verifiacation.CountItemsInOrder(Convert.ToString(PrdZkg_rcd), 17);
            object[,] Item = DispOrders.GetItemsFromTrdS(PrdZkg_rcd, CntLinesSP, 17);
            string GlnGr = Verifiacation.GetGLNGR(Convert.ToString(headerSP[7]));

            //какой gln номер использовать
            bool UseMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(DelivInfo[8]));
            string ILN_Edi;
            string OrderSPEDISOFT;
            object[] FirmInfo;
            object[] FirmAdr;
            if (UseMasterGLN == false)//используем данные текущего предприятия
            {
                ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ИЛН");
                OrderSPEDISOFT = DispOrders.GetValueOption("EDI-СОФТ.ОТВЕТ НА ЗАКАЗ");
                FirmInfo = Verifiacation.GetFirmInfo();
                FirmAdr = Verifiacation.GetFirmAdr();
            }
            else//используем данные головного предприятия
            {
                ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");
                FirmInfo = Verifiacation.GetMasterFirmInfo();
                FirmAdr = Verifiacation.GetMasterFirmAdr();
                try
                {
                    OrderSPEDISOFT = DispOrders.GetValueOption("EDI-СОФТ.ЭКСПОРТ");
                }
                catch
                {
                    OrderSPEDISOFT = DispOrders.GetValueOption("EDI-СОФТ.ОТВЕТ НА ЗАКАЗ");
                }

            }

            XDocument xdoc = new XDocument();

            //основные элементы (1 уровень)
            XElement DocumentOrderResponse = new XElement("Document-OrderResponse");
            XElement OrderResponseHeader = new XElement("OrderResponse-Header");
            XElement DocumentParties = new XElement("Document-Parties");
            XElement OrderResponseParties = new XElement("OrderResponse-Parties");
            XElement OrderResponseLines = new XElement("OrderResponse-Lines");
            XElement OrderResponseSummary = new XElement("OrderResponse-Summary");

            xdoc.Add(DocumentOrderResponse);
            DocumentOrderResponse.Add(OrderResponseHeader);
            DocumentOrderResponse.Add(DocumentParties);
            DocumentOrderResponse.Add(OrderResponseParties);
            DocumentOrderResponse.Add(OrderResponseLines);
            DocumentOrderResponse.Add(OrderResponseSummary);

           //-------------Header------------------
            XElement OrderResponseNumber = new XElement("OrderResponseNumber",headerSP[0]);
            XElement OrderResponseDate = new XElement("OrderResponseDate", (Convert.ToDateTime(headerSP[1])).ToString("yyyy-MM-dd"));
            XElement ExpectedDeliveryDate = new XElement("ExpectedDeliveryDate", (Convert.ToDateTime(headerSP[2])).ToString("yyyy-MM-dd"));
            XElement OrderResponseCurrency = new XElement("OrderResponseCurrency", "RUR");
            XElement DocumentFunctionCode = new XElement("DocumentFunctionCode", "O");
            XElement Remarks = new XElement("Remarks", headerSP[5]);
            XElement Order = new XElement("Order");

            OrderResponseHeader.Add(OrderResponseNumber);
            OrderResponseHeader.Add(OrderResponseDate);
            OrderResponseHeader.Add(ExpectedDeliveryDate);
            OrderResponseHeader.Add(OrderResponseCurrency);
            OrderResponseHeader.Add(DocumentFunctionCode);
            OrderResponseHeader.Add(Remarks);
            OrderResponseHeader.Add(Order);

            XElement BuyerOrderNumber = new XElement("BuyerOrderNumber",headerSP[5]);
            XElement BuyerOrderDate = new XElement("BuyerOrderDate",(Convert.ToDateTime(headerSP[1])).ToString("yyyy-MM-dd"));

            Order.Add(BuyerOrderNumber);
            Order.Add(BuyerOrderDate);

            //-----------Document-Parties------------
            XElement Sender = new XElement("Sender");
            XElement Receiver = new XElement("Receiver");

            DocumentParties.Add(Sender);
            DocumentParties.Add(Receiver);

            XElement ILNSender = new XElement("ILN", ILN_Edi);
            XElement NameSender = new XElement("Name", Convert.ToString(FirmInfo[0]));
            XElement ILNReceiver = new XElement("ILN", GlnGr);
            XElement NameReceiver = new XElement("Name", DelivInfo[1]);

            Sender.Add(ILNSender);
            Sender.Add(NameSender);
            Receiver.Add(ILNReceiver);
            Receiver.Add(NameReceiver);

            //----------OrderResponse-Parties------------
            XElement Buyer = new XElement("Buyer");
            XElement Seller = new XElement("Seller");
            XElement DeliveryPoint = new XElement("DeliveryPoint");

            OrderResponseParties.Add(Buyer);
            OrderResponseParties.Add(Seller);
            OrderResponseParties.Add(DeliveryPoint);

            XElement ILNBuyer = new XElement("ILN", PlatInfo[2]);
            XElement NameBuyer = new XElement("Name", PlatInfo[1]);

            XElement ILNRSeller = new XElement("ILN",ILN_Edi);
            XElement NameSeller = new XElement("Name", Convert.ToString(FirmInfo[0]));
            
            XElement ILNDeliveryPoint = new XElement("ILN", DelivInfo[2]);
            XElement NameDeliveryPoint = new XElement("Name", DelivInfo[1]);
            XElement StreetAndNumber = new XElement("StreetAndNumber", DelivInfo[6]);
            XElement PostalCode = new XElement("PostalCode", DelivInfo[7]);

            Buyer.Add(ILNBuyer);
            Buyer.Add(NameBuyer);
            Seller.Add(ILNRSeller);
            Seller.Add(NameSeller);

            DeliveryPoint.Add(ILNDeliveryPoint);
            DeliveryPoint.Add(NameDeliveryPoint);
            DeliveryPoint.Add(StreetAndNumber);
            DeliveryPoint.Add(PostalCode);

            //------OrderResponse-Lines---------------
            

            for (int i = 0; i < CntLinesSP; i++)
            {
                XElement Line = new XElement("Line");
                XElement LineItem = new XElement("Line-Item");

                OrderResponseLines.Add(Line);
                Line.Add(LineItem);

                object[] BICode = Verifiacation.GetBuyerItemCode(Convert.ToString(PlatInfo[5]), Convert.ToString(Item[i, 1]));

                XElement LineNumber = new XElement("LineNumber", i + 1);
                XElement EAN = new XElement("EAN",Item[i,0]);
                XElement BuyerItemCode = new XElement("BuyerItemCode", BICode[0]);
                XElement SupplierItemCode = new XElement("SupplierItemCode", Item[i,2]);
                XElement ItemDescription = new XElement("ItemDescription", Item[i, 3]);
                XElement ItemStatus = new XElement("ItemStatus","5");
                XElement OrderedQuantity = new XElement("OrderedQuantity",Item[i, 4]);
                XElement AllocatedDelivered = new XElement("AllocatedDelivered", Item[i, 4]);
                XElement UnitOfMeasure = new XElement("UnitOfMeasure", Item[i, 7]);
                XElement OrderedUnitGrossPrice = new XElement("OrderedUnitGrossPrice", Item[i,6]);
                XElement TaxRate = new XElement("TaxRate",Item[i, 9]);
                XElement GrossAmount = new XElement("GrossAmount", (Convert.ToDecimal(Item[i, 6])*Convert.ToDecimal(Item[i, 4])));

                LineItem.Add(LineNumber);
                LineItem.Add(EAN);
                LineItem.Add(BuyerItemCode);
                LineItem.Add(SupplierItemCode);
                LineItem.Add(ItemDescription);
                LineItem.Add(ItemStatus);
                LineItem.Add(OrderedQuantity);
                LineItem.Add(AllocatedDelivered);
                LineItem.Add(UnitOfMeasure);
                LineItem.Add(OrderedUnitGrossPrice);
                LineItem.Add(TaxRate);
                LineItem.Add(GrossAmount);

            }


            //------OrderResponse-Summary-------------

            object[] total = DispOrders.GetTotal(PrdZkg_rcd, 17);
            XElement TotalLines = new XElement("TotalLines",CntLinesSP);
            XElement TotalAmount = new XElement("TotalAmount",total[4]);
            XElement TotalNetAmount = new XElement("TotalNetAmount",total[5]);
            XElement TotalGrossAmount = new XElement("TotalGrossAmount",total[4]);

            OrderResponseSummary.Add(TotalLines);
            OrderResponseSummary.Add(TotalAmount);
            OrderResponseSummary.Add(TotalNetAmount);
            OrderResponseSummary.Add(TotalGrossAmount);



            //------сохранение документа-----------
            string dd = DateTime.Today.ToString(@"yyyyMMdd");
            string nameInv = "ORDRSP_" + dd + headerSP[5] + ".xml";
            try
            {
                xdoc.Save(OrderSPEDISOFT + nameInv);
                xdoc.Save(ArchiveEDISOFT + nameInv);
                string message = "EDISOFT. Подтверждение(OrderSP) " + nameInv + " создан в " + OrderSPEDISOFT;
                Program.WriteLine(message);
                DispOrders.WriteProtocolEDI("Подтверждение заказа", nameInv, PlatInfo[0] + " - " + PlatInfo[1], 0, DelivInfo[0] + " - " + DelivInfo[1], "Подтверждение заказа сформировано", DateTime.Now, Convert.ToString(headerSP[5]), "EDISOFT");
                ReportEDI.RecordCountEDoc("EDI-Софт", "OrderSP", 1);
                //запись в лог о удаче
            }
            catch
            {
                string message_error = "EDISOFT. Не могу создать xml файл подтверждения(OrderSP) в " + OrderSPEDISOFT + ". Нет доступа или диск переполнен.";
                DispOrders.WriteProtocolEDI("Подтверждение заказа", nameInv, PlatInfo[0] + " - " + PlatInfo[1], 10, DelivInfo[0] + " - " + DelivInfo[1], "Подтверждение заказа не сформировано. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(headerSP[5]), "EDISOFT");
                Program.WriteLine(message_error);
                //запись в лог о неудаче
            }

        }

        public static void CreateKonturOrderSp(string PrdZkg_rcd)
        {
            //получение путей
            string ArchiveKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.АРХИВ");

            //генерация имени файла.
            string id = Convert.ToString(Guid.NewGuid()); ;
            string nameInv = "ORDRSP_" + id + ".xml";//потом раскоментировать
            //string nameInv = "ORDRSP_TEST.xml";


            //получение данных
            object[] headerSP = Verifiacation.GetHeaderSP(PrdZkg_rcd);
            object[] DelivInfo = Verifiacation.GetDataFromPtnCD(Convert.ToString(headerSP[6]));
            object[] PlatInfo = Verifiacation.GetDataFromPtnCD(Convert.ToString(headerSP[7]));
            int CntLinesSP = Verifiacation.CountItemsInOrder(Convert.ToString(PrdZkg_rcd), 17);
            object[,] Item = DispOrders.GetItemsFromTrdS(PrdZkg_rcd, CntLinesSP, 17);
            object[] total = DispOrders.GetTotal(PrdZkg_rcd, 17);
            String ILNRecvr = Verifiacation.GetILNReceiver(Convert.ToString(PlatInfo[8]));
            String ILNBur = Verifiacation.GetILNBuyer(Convert.ToString(PlatInfo[8]));

            //какой gln номер использовать
            bool UseMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(DelivInfo[8]));
            string ILN_Edi;
            string OrderSPKONTUR;
            object[] FirmInfo;
            object[] FirmAdr;
            if (UseMasterGLN == false)//используем данные текущего предприятия
            {
                ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ИЛН");
                OrderSPKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.ОТВЕТ НА ЗАКАЗ");
                FirmInfo = Verifiacation.GetFirmInfo();
                FirmAdr = Verifiacation.GetFirmAdr();

            }
            else//используем данные головного предприятия
            {
                ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");
                FirmInfo = Verifiacation.GetMasterFirmInfo();
                FirmAdr = Verifiacation.GetMasterFirmAdr();
                try
                {
                    OrderSPKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.ЭКСПОРТ");
                }
                catch
                {

                    OrderSPKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.ОТВЕТ НА ЗАКАЗ");
                }
            }


            XDocument xdoc = new XDocument();

            //основные элементы (1 уровень)
            XElement eDIMessage = new XElement("eDIMessage");
            XElement interchangeHeader = new XElement("interchangeHeader");
            XElement orderResponse = new XElement("orderResponse");

            XAttribute numberResp = new XAttribute("number", headerSP[0]);
            XAttribute dateResp = new XAttribute("date", (Convert.ToDateTime(headerSP[1])).ToString("yyyy-MM-dd"));
            XAttribute status = new XAttribute("status", "Accepted");
            XAttribute idMessage = new XAttribute("id", id);
            XAttribute creationDateTime = new XAttribute("creationDateTime", (DateTime.Now).ToString("yyyy-MM-dd HH:mm:ss"));

            xdoc.Add(eDIMessage);
            eDIMessage.Add(interchangeHeader);
            eDIMessage.Add(orderResponse);
            eDIMessage.Add(idMessage);
            eDIMessage.Add(creationDateTime);

            orderResponse.Add(numberResp);
            orderResponse.Add(dateResp);
            orderResponse.Add(status);

            //-------interchangeHeader------------
            XElement sender = new XElement("sender",ILN_Edi);
            XElement recipient = new XElement("recipient", ILNRecvr);
            XElement documentType = new XElement("documentType", "ORDRSP");

            interchangeHeader.Add(sender);
            interchangeHeader.Add(recipient);
            interchangeHeader.Add(documentType);

            //--------------orderResponse-----------
            //XElement originOrder = new XElement("originOrder",headerSP[0]);
            XElement originOrder = new XElement("originOrder");
            XAttribute numberOrder = new XAttribute("number",headerSP[5]);
            XAttribute dateOrder = new XAttribute("date", (Convert.ToDateTime(headerSP[1])).ToString("yyyy-MM-dd"));

            XElement seller = new XElement("seller");
            XElement buyer = new XElement("buyer");
            XElement deliveryInfo = new XElement("deliveryInfo");
            XElement lineItems = new XElement("lineItems");

            orderResponse.Add(originOrder);
            originOrder.Add(numberOrder);
            originOrder.Add(dateOrder);

            orderResponse.Add(seller);
            orderResponse.Add(buyer);
            orderResponse.Add(deliveryInfo);
            orderResponse.Add(lineItems);

            //---------seller----------------
            XElement gln = new XElement("gln", ILN_Edi);
            XElement organization = new XElement("organization");

            XElement name = new XElement("name", Convert.ToString(FirmInfo[0]));
            XElement inn = new XElement("inn", FirmInfo[1]);
            XElement kpp = new XElement("kpp", FirmInfo[2]);

            seller.Add(gln);
            seller.Add(organization);

            organization.Add(name);
            organization.Add(inn);
            organization.Add(kpp);

            //---------buyer----------------
            XElement glnbuyer = new XElement("gln", ILNBur);
            XElement organizationbuyer = new XElement("organization");

            XElement namebuyer = new XElement("name", PlatInfo[1]);
            XElement innbuyer = new XElement("inn", PlatInfo[3]);
            XElement kppbuyer = new XElement("kpp", PlatInfo[4]);

            buyer.Add(glnbuyer);
            buyer.Add(organizationbuyer);

            organizationbuyer.Add(namebuyer);
            organizationbuyer.Add(innbuyer);
            organizationbuyer.Add(kppbuyer);

            //---------deliveryInfo----------------
            XElement estimatedDeliveryDateTime = new XElement("estimatedDeliveryDateTime", headerSP[2]);
            XElement shipFrom = new XElement("shipFrom");
            XElement shipTo = new XElement("shipTo");

            XElement glnFrom = new XElement("gln",ILN_Edi);
            XElement glnTo = new XElement("gln",DelivInfo[2]);

            deliveryInfo.Add(estimatedDeliveryDateTime);
            deliveryInfo.Add(shipFrom);
            deliveryInfo.Add(shipTo);

            shipFrom.Add(glnFrom);
            shipTo.Add(glnTo);

            //---------lineItems----------------
            XElement currencyISOCode = new XElement("currencyISOCode", "RUB");
            
            lineItems.Add(currencyISOCode);
            
            for (int i = 0; i < CntLinesSP; i++)
            {
                object[] BICode = Verifiacation.GetBuyerItemCode(Convert.ToString(PlatInfo[5]), Convert.ToString(Item[i, 1]));

                XElement lineItem = new XElement("lineItem");
                lineItems.Add(lineItem);
                XAttribute statusItem = new XAttribute("status", "Accepted");
                lineItem.Add(status);

                XElement orderLineNumber = new XElement("orderLineNumber",i+1);
                XElement gtin = new XElement("gtin", Item[i, 0]);
                XElement internalBuyerCode = new XElement("internalBuyerCode", BICode[0]);
                XElement internalSupplierCode = new XElement("internalSupplierCode", Item[i, 2]);
                XElement description = new XElement("description", Item[i, 3]);
                XElement confirmedQuantity = new XElement("confirmedQuantity", Item[i, 4]);
                XElement netPriceWithVAT = new XElement("netPriceWithVAT", Item[i, 6]);
                XElement netAmount = new XElement("netAmount", Item[i, 12]);
                XElement amount = new XElement("amount", Item[i, 13]);
                XAttribute unitOfMeasure = new XAttribute("unitOfMeasure", Item[i, 7]);

                lineItem.Add(orderLineNumber);
                lineItem.Add(gtin);
                lineItem.Add(internalBuyerCode);
                lineItem.Add(internalSupplierCode);
                lineItem.Add(description);
                lineItem.Add(confirmedQuantity);
                confirmedQuantity.Add(unitOfMeasure);
                lineItem.Add(netPriceWithVAT);
                lineItem.Add(netAmount);
                lineItem.Add(amount);




            }
            XElement totalSumExcludingTaxes = new XElement("totalSumExcludingTaxes", total[5]);

            lineItems.Add(totalSumExcludingTaxes);



            //------сохранение документа-----------
            try
            {
                xdoc.Save(OrderSPKONTUR + nameInv);
                xdoc.Save(ArchiveKONTUR + nameInv);
                string message = "СКБ-Контур. Подтверждение(OrderSP) " + nameInv + " создан в " + OrderSPKONTUR;
                Program.WriteLine(message);
                DispOrders.WriteProtocolEDI("Подтверждение заказа", nameInv, PlatInfo[0] + " - " + PlatInfo[1], 0, DelivInfo[0] + " - " + DelivInfo[1], "Подтверждение заказа сформировано", DateTime.Now, Convert.ToString(headerSP[5]), "СКБ-Контур");
                ReportEDI.RecordCountEDoc("СКБ-Контур", "OrderSP", 1);
                //запись в лог о удаче
            }
            catch
            {
                string message_error = "СКБ-Контур. Не могу создать xml файл подтверждения(OrderSP) в " + OrderSPKONTUR + ". Нет доступа или диск переполнен.";
                DispOrders.WriteProtocolEDI("Подтверждение заказа", nameInv, PlatInfo[0] + " - " + PlatInfo[1], 10, DelivInfo[0] + " - " + DelivInfo[1], "Подтверждение заказа не сформировано. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(headerSP[5]), "СКБ-Контур");
                Program.WriteLine(message_error);
                //запись в лог о неудаче
                //запись в лог о неудаче
            }

        }

        public static void CreateEdiDesadv(List<object> CurrDataDV)
        {
            //получение путей
            string ArchiveEDI = DispOrders.GetValueOption("EDI-СОФТ.АРХИВ");

            //string DesadvEDI = "\\\\fileshare\\EXPIMP\\OrderIntake\\EDISOFT\\OUTBOX\\";
            //string ArchiveEDI = "\\\\fileshare\\EXPIMP\\OrderIntake\\EDISOFT\\ARCHIVE\\";

            //получение данных
            object[] UPDInfo = Verifiacation.GetUPDInfo(Convert.ToString(CurrDataDV[2]));
            object[] DelivInfo = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(CurrDataDV[5]), Convert.ToInt64(CurrDataDV[13]));
            object[] PlatInfo = Verifiacation.GetDataFromPtnCD(Convert.ToString(CurrDataDV[4]));
            int CntLinesDV = Verifiacation.CountItemsInOrder(Convert.ToString(CurrDataDV[2]), 1);
            object[,] Item = DispOrders.GetItemsFromTrdS(Convert.ToString(CurrDataDV[2]), CntLinesDV, 1, true);
            string ILNRecvr = Verifiacation.GetILNReceiver(Convert.ToString(PlatInfo[8]));
            string ILNBur = Verifiacation.GetILNBuyer(Convert.ToString(PlatInfo[8]));

            //признак создания ВСД для контрагента
            string vsd_send = Convert.ToString(DelivInfo[13]);

            //какой gln номер использовать
            bool UseMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(CurrDataDV[5]));
            //Program.WriteLine(UseMasterGLN);
            string ILN_Edi;
            string DesadvEDI;
            object[] FirmInfo;
            object[] FirmAdr;
            if (UseMasterGLN == false)//используем данные текущего предприятия
            {
                ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ILN_EDI");
                DesadvEDI = DispOrders.GetValueOption("EDI-СОФТ.ОТГРУЗКА");
                FirmInfo = Verifiacation.GetFirmInfo();
                FirmAdr = Verifiacation.GetFirmAdr();

            }
            else//используем данные головного предприятия
            {
                ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");
                FirmInfo = Verifiacation.GetMasterFirmInfo();
                FirmAdr = Verifiacation.GetMasterFirmAdr();
                try
                {
                    DesadvEDI = DispOrders.GetValueOption("EDI-СОФТ.ЭКСПОРТ");
                }
                catch
                {
                    DesadvEDI = DispOrders.GetValueOption("EDI-СОФТ.ОТГРУЗКА");
                }
            }


            XDocument xdoc = new XDocument();

            //основные элементы (1 уровень)
            XElement DocumentDespatchAdvice = new XElement("Document-DespatchAdvice");
            XElement DespatchAdviceHeader = new XElement("DespatchAdvice-Header");
            XElement DocumentParties = new XElement("Document-Parties");
            XElement DespatchAdviceParties = new XElement("DespatchAdvice-Parties");
            XElement DespatchAdviceConsignment= new XElement("DespatchAdvice-Consignment");
            XElement DespatchAdviceSummary = new XElement("DespatchAdvice-Summary");

            xdoc.Add(DocumentDespatchAdvice);
            DocumentDespatchAdvice.Add(DespatchAdviceHeader);
            DocumentDespatchAdvice.Add(DocumentParties);
            DocumentDespatchAdvice.Add(DespatchAdviceParties);
            DocumentDespatchAdvice.Add(DespatchAdviceConsignment);
            DocumentDespatchAdvice.Add(DespatchAdviceSummary);

            //-------DespatchAdvice-Header--------------

            XElement DespatchAdviceNumber = new XElement("DespatchAdviceNumber", CurrDataDV[1]);
            XElement DespatchAdviceDate = new XElement("DespatchAdviceDate", DateTime.Now.ToString("yyyy-MM-dd"));

            XElement EstimatedDeliveryDate = new XElement("EstimatedDeliveryDate", (Convert.ToDateTime(CurrDataDV[7])).ToString("yyyy-MM-dd"));

            //XElement EstimatedDeliveryDate = new XElement("EstimatedDeliveryDate", (Convert.ToDateTime(CurrDataDV[7])).Date);
            XElement BuyerOrderNumber = new XElement("BuyerOrderNumber",CurrDataDV[6]);
            XElement UTDnumber = new XElement("UTDnumber", UPDInfo[0]);
            XElement UTDDate = new XElement("UTDDate", (Convert.ToDateTime(UPDInfo[1]).ToString("yyyy-MM-dd")));
            XElement DocumentFunctionCode = new XElement("DocumentFunctionCode", "9");
            XElement DocumentNameCode = new XElement("DocumentNameCode", "351");
            XElement Remarks = new XElement("Remarks",CurrDataDV[6]);

            DespatchAdviceHeader.Add(DespatchAdviceNumber);
            DespatchAdviceHeader.Add(DespatchAdviceDate);
            DespatchAdviceHeader.Add(EstimatedDeliveryDate);
            DespatchAdviceHeader.Add(BuyerOrderNumber);
            if (Convert.ToInt64(CurrDataDV[13]) == 1) //если контрагенту отправляем УПД
            {
                DespatchAdviceHeader.Add(UTDnumber);
                DespatchAdviceHeader.Add(UTDDate);
            }
            
            DespatchAdviceHeader.Add(DocumentFunctionCode);
            DespatchAdviceHeader.Add(DocumentNameCode);
            DespatchAdviceHeader.Add(Remarks);

            //-----------Document-Parties------------
            XElement Sender = new XElement("Sender");
            XElement Receiver = new XElement("Receiver");

            DocumentParties.Add(Sender);
            DocumentParties.Add(Receiver);


            XElement ILNSender = new XElement("ILN",ILN_Edi);
            XElement NameSender = new XElement("Name", Convert.ToString(FirmInfo[0]));
            XElement ILNReceiver = new XElement("ILN", ILNRecvr);
            XElement NameReceiver = new XElement("Name", PlatInfo[1]);

            Sender.Add(ILNSender);
            Sender.Add(NameSender);
            Receiver.Add(ILNReceiver);
            Receiver.Add(NameReceiver);

            XElement Buyer = new XElement("Buyer");
            XElement Seller = new XElement("Seller");
            XElement DeliveryPoint = new XElement("DeliveryPoint");

            DespatchAdviceParties.Add(Buyer);
            DespatchAdviceParties.Add(Seller);
            DespatchAdviceParties.Add(DeliveryPoint);

            XElement ILNBuyer = new XElement("ILN", ILNBur);
            XElement NameBuyer = new XElement("Name", PlatInfo[1]);

            XElement ILNRSeller = new XElement("ILN", ILN_Edi);
            XElement NameSeller = new XElement("Name", Convert.ToString(FirmInfo[0]));

            XElement ILNDeliveryPoint = new XElement("ILN", DelivInfo[2]);
            XElement NameDeliveryPoint = new XElement("Name", DelivInfo[1]);
            XElement StreetAndNumber = new XElement("StreetAndNumber", DelivInfo[6]);
            XElement PostalCode = new XElement("PostalCode", DelivInfo[7]);

            Buyer.Add(ILNBuyer);
            Buyer.Add(NameBuyer);
            Seller.Add(ILNRSeller);
            Seller.Add(NameSeller);

            DeliveryPoint.Add(ILNDeliveryPoint);
            DeliveryPoint.Add(NameDeliveryPoint);
            DeliveryPoint.Add(StreetAndNumber);
            DeliveryPoint.Add(PostalCode);

            //------DespatchAdvice-Consignment----------------
            XElement PackingSequence = new XElement("Packing-Sequence");
            DespatchAdviceConsignment.Add(PackingSequence);

            for (int i = 0; i < CntLinesDV; i++)
            {
                XElement Line = new XElement("Line");
                XElement LineItem = new XElement("Line-Item");

                PackingSequence.Add(Line);
                Line.Add(LineItem);

                object[] BICode = Verifiacation.GetBuyerItemCode(Convert.ToString(PlatInfo[5]), Convert.ToString(Item[i, 1]));

                XElement LineNumber = new XElement("LineNumber", i + 1);
                XElement EAN = new XElement("EAN", Item[i, 0]);
                XElement BuyerItemCode = new XElement("BuyerItemCode", BICode[0]);
                XElement SupplierItemCode = new XElement("SupplierItemCode", Item[i, 2]);
                XElement ItemDescription = new XElement("ItemDescription", Item[i, 3]);
                XElement QuantityDespatched = new XElement("QuantityDespatched", Item[i, 4]);
                XElement UnitOfMeasure = new XElement("UnitOfMeasure", Item[i, 7]);
                XElement UnitNetPrice = new XElement("UnitNetPrice", Item[i, 6]);
                XElement TaxRate = new XElement("TaxRate", Item[i, 9]);
                XElement SuggestedPrice = new XElement("SuggestedPrice", Convert.ToDecimal(Item[i,6])*Convert.ToDecimal(Item[i,4]));
                XElement VSDNumber = new XElement("VSDNumber", Item[i, 14]);

                LineItem.Add(LineNumber);
                LineItem.Add(EAN);
                LineItem.Add(BuyerItemCode);
                LineItem.Add(SupplierItemCode);
                LineItem.Add(ItemDescription);
                LineItem.Add(QuantityDespatched);
                LineItem.Add(UnitOfMeasure);
                /*Добавляем ВСД только если у контрагента стоит признак отправки ВСД, а также всд по номенклатуре сформирован*/
                string vsd = Convert.ToString(Item[i, 14]);
                if ( vsd_send == "1" && !string.IsNullOrWhiteSpace(vsd))
                {
                    LineItem.Add(VSDNumber);
                }

                LineItem.Add(UnitNetPrice);
                LineItem.Add(TaxRate);
                LineItem.Add(SuggestedPrice);

            }

            //---------------DespatchAdvice-Summary-------------------
            object[] total = DispOrders.GetTotal(Convert.ToString(CurrDataDV[2]), 1);
            XElement TotalPSequence = new XElement("TotalPSequence", "1");
            XElement TotalLines = new XElement("TotalLines", CntLinesDV);
            XElement TotalGoodsDespatchedAmount = new XElement("TotalGoodsDespatchedAmount", total[0]);

            DespatchAdviceSummary.Add(TotalPSequence);
            DespatchAdviceSummary.Add(TotalLines);
            DespatchAdviceSummary.Add(TotalGoodsDespatchedAmount);



            //------сохранение документа-----------
            string dd = DateTime.Today.ToString(@"yyyyMMdd_") + DateTime.Now.ToString(@"HHmmssff_");
            string nameInv = "DESADV_" + dd + CurrDataDV[6] + ".xml";
            try
            {
                xdoc.Save(DesadvEDI + nameInv);
                xdoc.Save(ArchiveEDI + nameInv);
                string message = "EDISOFT. Уведомление об отгрузке(DESADV) " + nameInv + " создан в " + DesadvEDI;
                Program.WriteLine(message);
                DispOrders.WriteProtocolEDI("Уведомление об отгрузке", nameInv, PlatInfo[0] + " - " + PlatInfo[1], 0, DelivInfo[0] + " - " + DelivInfo[1], "Уведомление об отгрузке сформировано", DateTime.Now, Convert.ToString(CurrDataDV[6]), "EDISOFT");
                ReportEDI.RecordCountEDoc("EDI-Софт", "Desadv", 1);
                DispOrders.WriteEDiSentDoc("3", nameInv, Convert.ToString(CurrDataDV[2]), Convert.ToString(CurrDataDV[1]), "46", Convert.ToString(total[0]), Convert.ToString(CurrDataDV[3]),0);
                //запись в лог о удаче
            }
            catch(IOException e)
            {
                string message_error = "EDISOFT. Не могу создать xml файл Уведомление об отгрузке(DESADV) в " + DesadvEDI + ". Нет доступа или диск переполнен.";
                DispOrders.WriteProtocolEDI("Уведомление об отгрузке", nameInv, PlatInfo[0] + " - " + PlatInfo[1], 10, DelivInfo[0] + " - " + DelivInfo[1], "Уведомление об отгрузке не сформировано. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataDV[6]), "EDISOFT");
                Program.WriteLine(message_error);
                DispOrders.WriteErrorLog(e.Message);
                //запись в лог о неудаче
            }

        }

        public static void CreateKonturDesadv(List<object> CurrDataDV)
        {
            //получение путей
            string ArchiveKontur = DispOrders.GetValueOption("СКБ-КОНТУР.АРХИВ");

            //string DesadvKontur = "\\\\fileshare\\EXPIMP\\OrderIntake\\SKBKONTUR\\OUTBOX\\";
            //string ArchiveKontur = "\\\\fileshare\\EXPIMP\\OrderIntake\\SKBKONTUR\\ARCHIVE\\";

            //генерация имени файла.
            string id = Convert.ToString(Guid.NewGuid()); ;
            string nameInv = "DESADV_" + id + ".xml";//потом раскоментировать

            //получение данных
            object[] DelivInfo = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(CurrDataDV[5]), Convert.ToInt64(CurrDataDV[13]));
            object[] PlatInfo = Verifiacation.GetDataFromPtnCD(Convert.ToString(CurrDataDV[4]));
            int CntLinesDV = Verifiacation.CountItemsInOrder(Convert.ToString(CurrDataDV[2]), 1);
            object[,] Item = DispOrders.GetItemsFromTrdS(Convert.ToString(CurrDataDV[2]), CntLinesDV, 1);
            String ILNRecvr = Verifiacation.GetILNReceiver(Convert.ToString(PlatInfo[8]));
            String ILNBur = Verifiacation.GetILNBuyer(Convert.ToString(PlatInfo[8]));

            //признак создания ВСД для контрагента
            string vsd_send = Convert.ToString(DelivInfo[13]);

            //какой gln номер использовать
            bool UseMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(DelivInfo[8]));
            string ILN_Edi;
            string DesadvKontur;
            object[] FirmInfo;
            object[] FirmAdr;
            if (UseMasterGLN == false)//используем данные текущего предприятия
            {
                ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ILN_EDI");
                DesadvKontur = DispOrders.GetValueOption("СКБ-КОНТУР.ОТГРУЗКА");
                FirmInfo = Verifiacation.GetFirmInfo();
                FirmAdr = Verifiacation.GetFirmAdr();

            }
            else//используем данные головного предприятия
            {
                ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");
                FirmInfo = Verifiacation.GetMasterFirmInfo();
                FirmAdr = Verifiacation.GetMasterFirmAdr();
                try
                {
                    DesadvKontur = DispOrders.GetValueOption("СКБ-КОНТУР.ЭКСПОРТ");
                }
                catch
                {
                    DesadvKontur = DispOrders.GetValueOption("СКБ-КОНТУР.ОТГРУЗКА");
                }
            }


            XDocument xdoc = new XDocument();

            //основные элементы (1 уровень)
            XElement eDIMessage = new XElement("eDIMessage");
            XElement interchangeHeader = new XElement("interchangeHeader");
            XElement despatchAdvice = new XElement("despatchAdvice");

            XAttribute numberDV = new XAttribute("number", CurrDataDV[1]);
            XAttribute dateDV = new XAttribute("date", CurrDataDV[8]);
            XAttribute idMessage = new XAttribute("id", id);
            XAttribute creationDateTime = new XAttribute("creationDateTime", (DateTime.Now).ToString("yyyy-MM-dd HH:mm:ss"));

            xdoc.Add(eDIMessage);
            eDIMessage.Add(interchangeHeader);
            eDIMessage.Add(despatchAdvice);
            eDIMessage.Add(idMessage);
            eDIMessage.Add(creationDateTime);

            despatchAdvice.Add(numberDV);
            despatchAdvice.Add(dateDV);


            //-------interchangeHeader------------
            XElement sender = new XElement("sender", ILN_Edi);
            XElement recipient = new XElement("recipient", ILNRecvr);
            XElement documentType = new XElement("documentType", "DESADV");

            interchangeHeader.Add(sender);
            interchangeHeader.Add(recipient);
            interchangeHeader.Add(documentType);

            //--------------despatchAdvice-----------
            XElement originOrder = new XElement("originOrder");
            XAttribute numberOrder = new XAttribute("number", CurrDataDV[6]);
            XAttribute dateOrder = new XAttribute("date", CurrDataDV[9]);

            XElement seller = new XElement("seller");
            XElement buyer = new XElement("buyer");
            XElement deliveryInfo = new XElement("deliveryInfo");
            XElement lineItems = new XElement("lineItems");

            despatchAdvice.Add(originOrder);
            originOrder.Add(numberOrder);
            originOrder.Add(dateOrder);

            despatchAdvice.Add(seller);
            despatchAdvice.Add(buyer);
            despatchAdvice.Add(deliveryInfo);
            despatchAdvice.Add(lineItems);

            //---------seller----------------
            XElement gln = new XElement("gln", ILN_Edi);
            XElement organization = new XElement("organization");

            XElement name = new XElement("name", Convert.ToString(FirmInfo[0]));
            XElement inn = new XElement("inn", FirmInfo[1]);
            XElement kpp = new XElement("kpp", FirmInfo[2]);

            seller.Add(gln);
            seller.Add(organization);

            organization.Add(name);
            organization.Add(inn);
            organization.Add(kpp);

            //---------buyer----------------
            XElement glnbuyer = new XElement("gln", ILNBur);
            XElement organizationbuyer = new XElement("organization");

            XElement namebuyer = new XElement("name", PlatInfo[1]);
            XElement innbuyer = new XElement("inn", PlatInfo[3]);
            XElement kppbuyer = new XElement("kpp", PlatInfo[4]);

            buyer.Add(glnbuyer);
            buyer.Add(organizationbuyer);

            organizationbuyer.Add(namebuyer);
            organizationbuyer.Add(innbuyer);
            organizationbuyer.Add(kppbuyer);

            //---------deliveryInfo----------------
            XElement estimatedDeliveryDateTime = new XElement("estimatedDeliveryDateTime", CurrDataDV[7]);
            XElement shipFrom = new XElement("shipFrom");
            XElement shipTo = new XElement("shipTo");

            XElement glnFrom = new XElement("gln", ILN_Edi);
            XElement glnTo = new XElement("gln", DelivInfo[2]);
            XElement transportation = new XElement("transportation");

            deliveryInfo.Add(estimatedDeliveryDateTime);
            deliveryInfo.Add(shipFrom);
            deliveryInfo.Add(shipTo);
            deliveryInfo.Add(transportation);

            shipFrom.Add(glnFrom);
            shipTo.Add(glnTo);

                 //------transportation
            XElement vehicleNumber = new XElement("vehicleNumber",CurrDataDV[10]);
            string marka = Convert.ToString(CurrDataDV[11]);
            if (string.IsNullOrWhiteSpace(marka))
            {
                marka = "NONEBRAND";
            }
            //XElement vehicleBrand = new XElement("vehicleBrand",(Convert.ToString(CurrDataDV[11])).Remove(1,9));
            XElement vehicleBrand = new XElement("vehicleBrand", marka.Substring(0,marka.Length));
            XElement nameOfCarrier = new XElement("nameOfCarrier",CurrDataDV[12]);

            transportation.Add(vehicleNumber);
            transportation.Add(vehicleBrand);
            transportation.Add(nameOfCarrier);

            //---------lineItems----------------
            XElement currencyISOCode = new XElement("currencyISOCode", "RUB");

            lineItems.Add(currencyISOCode);

            decimal sum = 0;
            for (int i = 0; i < CntLinesDV; i++)
            {
                object[] BICode = Verifiacation.GetBuyerItemCode(Convert.ToString(PlatInfo[5]), Convert.ToString(Item[i, 1]));

                XElement lineItem = new XElement("lineItem");
                lineItems.Add(lineItem);

                XElement orderLineNumber = new XElement("orderLineNumber", i + 1);
                XElement gtin = new XElement("gtin", Item[i, 0]);
                XElement internalBuyerCode = new XElement("internalBuyerCode", BICode[0]);
                XElement internalSupplierCode = new XElement("internalSupplierCode", Item[i, 2]);
                XElement description = new XElement("description", Item[i, 3]);
                XElement despatchedQuantity = new XElement("despatchedQuantity", Item[i, 4]);
                XElement netAmount = new XElement("netAmount", Item[i, 12]);
                XElement amount = new XElement("amount", Convert.ToDecimal(Item[i, 4])*Convert.ToDecimal(Item[i,6]));
                XAttribute unitOfMeasure = new XAttribute("unitOfMeasure", Item[i, 7]);
                XElement VSDNumber = new XElement("veterinaryCertificateMercuryId", Item[i, 14]);

                lineItem.Add(orderLineNumber);
                lineItem.Add(gtin);
                lineItem.Add(internalBuyerCode);
                lineItem.Add(internalSupplierCode);
                lineItem.Add(description);
                lineItem.Add(despatchedQuantity);
                despatchedQuantity.Add(unitOfMeasure);
                lineItem.Add(netAmount);
                lineItem.Add(amount);
                /*Добавляем ВСД только если у контрагента стоит признак отправки ВСД, а также всд по номенклатуре сформирован*/
                string vsd = Convert.ToString(Item[i, 14]);
                if (vsd_send == "1" && !string.IsNullOrWhiteSpace(vsd))
                {
                    lineItem.Add(VSDNumber);
                }

                sum = sum + Convert.ToDecimal(Item[i, 13]);

            }

            //------сохранение документа-----------
            try
            {
                xdoc.Save(DesadvKontur + nameInv);
                xdoc.Save(ArchiveKontur + nameInv);
                string message = "СКБ-Контур. Уведомление об отгрузке (Desadv) " + nameInv + " создан в " + DesadvKontur;
                Program.WriteLine(message);
                DispOrders.WriteProtocolEDI("Уведомление об отгрузке", nameInv, PlatInfo[0] + " - " + PlatInfo[1], 0, DelivInfo[0] + " - " + DelivInfo[1], "Уведомление об отгрузке сформировано", DateTime.Now, Convert.ToString(CurrDataDV[6]), "СКБ-Контур");
                ReportEDI.RecordCountEDoc("СКБ-Контур", "Desadv", 1);
                DispOrders.WriteEDiSentDoc("3", nameInv, Convert.ToString(CurrDataDV[2]), Convert.ToString(CurrDataDV[1]), "46", Convert.ToString(sum), Convert.ToString(CurrDataDV[3]),0);
                //запись в лог о удаче
            }
            catch
            {
                string message_error = "СКБ-Контур. Не могу создать xml файл Уведомление об отгрузке (Desadv) в " + DesadvKontur + ". Нет доступа или диск переполнен.";
                DispOrders.WriteProtocolEDI("Уведомление об отгрузке", nameInv, PlatInfo[0] + " - " + PlatInfo[1], 10, DelivInfo[0] + " - " + DelivInfo[1], "Уведомление об отгрузке не сформировано. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataDV[6]), "СКБ-Контур");
                Program.WriteLine(message_error);
                
            }

        }

        /*   public static void CreateEdiUPD(List<object> CurrDataUPD)
           {
               //получение путей
               string ArchiveEDI = DispOrders.GetValueOption("EDI-СОФТ.АРХИВ");

               //получение данных
               object[] DelivInfo = Verifiacation.GetDataFromPtnRCD(Convert.ToString(CurrDataUPD[21]));
               object[] PlatInfo = Verifiacation.GetDataFromPtnCD(Convert.ToString(CurrDataUPD[20]));
               int CntLinesDV = Verifiacation.CountItemsInOrder(Convert.ToString(CurrDataUPD[19]), 1);

               bool PCE = Verifiacation.UsePCE(Convert.ToString(CurrDataUPD[4]));//проверка на использование штук в исходящих докуметов

               int CntLinesInvoice = Verifiacation.CountItemsInInvoice(Convert.ToString(CurrDataUPD[1]));

               object[,] Item = DispOrders.GetItemsFromInvoice(Convert.ToString(CurrDataUPD[1]), CntLinesDV, true);
               String ILNRecvr = Verifiacation.GetILNReceiver(Convert.ToString(PlatInfo[8]));
               String ILNBur = Verifiacation.GetILNBuyer(Convert.ToString(PlatInfo[8]));

               //какой gln номер использовать
               bool UseMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(DelivInfo[8]));
               string ILN_Edi;
               string UPDEDI;
               object[] FirmInfo;
               object[] FirmAdr;
               if (UseMasterGLN == false)//используем данные текущего предприятия
               {
                   ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ILN_EDI");
                   UPDEDI = DispOrders.GetValueOption("EDI-СОФТ.УПД");
                   FirmInfo = Verifiacation.GetFirmInfo();
                   FirmAdr = Verifiacation.GetFirmAdr();

               }
               else//используем данные головного предприятия
               {
                   ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");
                   FirmInfo = Verifiacation.GetMasterFirmInfo();
                   FirmAdr = Verifiacation.GetMasterFirmAdr();
                   try
                   {
                       UPDEDI = DispOrders.GetValueOption("EDI-СОФТ.ЭКСПОРТ");
                   }
                   catch
                   {
                       UPDEDI = DispOrders.GetValueOption("EDI-СОФТ.УПД");
                   }
               }

               //ID файла
               string dd = DateTime.Today.ToString(@"yyyyMMdd");
               string guid = Convert.ToString(Guid.NewGuid());
               string FileID = "ON_SCHFDOPPR_" + ILN_Edi + "_" + ILNRecvr + "_" + dd + "_" + guid;
               string nameUPD = FileID + ".xml";


               XDocument xdoc = new XDocument();

               //основные элементы (1 уровень)
               XElement File = new XElement("Файл");

               XElement ID = new XElement("СвУчДокОбор");
               XElement DOC = new XElement("Документ");

               XAttribute VersProg = new XAttribute("ВерсПрог", "Edisoft");
               XAttribute VersForm = new XAttribute("ВерсФорм", "5.01");
               XAttribute IdFile = new XAttribute("ИдФайл", "000000000000000000000000000000000000");

               xdoc.Add(File);
               File.Add(VersProg);
               File.Add(VersForm);
               File.Add(IdFile);

               XAttribute IdSender = new XAttribute("ИдОтпр", "0000000000");
               XAttribute Idreciever = new XAttribute("ИдПол", "0000000000");

               XElement InfOrg = new XElement("СвОЭДОтпр");
               XAttribute INNUL = new XAttribute("ИННЮЛ", "7801471082");
               XAttribute IDEDO = new XAttribute("ИдЭДО", "2IJ");
               XAttribute NaimOrg = new XAttribute("НаимОрг", "Эдисофт, ООО");

               File.Add(ID);
               File.Add(DOC);

               ID.Add(IdSender);
               ID.Add(Idreciever);

               ID.Add(InfOrg);

               InfOrg.Add(INNUL);
               InfOrg.Add(IDEDO);
               InfOrg.Add(NaimOrg);

               XAttribute TimeF = new XAttribute("ВремИнфПр", DateTime.Today.ToString(@"hh.mm.ss"));
               XAttribute DateF = new XAttribute("ДатаИнфПр", DateTime.Today.ToString(@"dd.MM.yyyy"));
               XAttribute KND = new XAttribute("КНД", "1115125");
               XAttribute NameOrg = new XAttribute("НаимЭконСубСост", "АО ГК РОСМОЛ");
               XAttribute Function = new XAttribute("Функция", "СЧФДОП");
               XAttribute PoFactXJ = new XAttribute("ПоФактХЖ", "Документ об отгрузке товаров (выполнении работ), передаче имущественных прав (документ об оказании услуг)");
               XAttribute NameDocOpr = new XAttribute("НаимДокОпр", "Счет-фактура и документ об отгрузке товаров (выполнении работ), передаче имущественных прав (документ об оказании услуг)");

               DOC.Add(TimeF);
               DOC.Add(DateF);
               DOC.Add(KND);
               DOC.Add(NameOrg);
               DOC.Add(Function);
               DOC.Add(PoFactXJ);
               DOC.Add(NameDocOpr);

               //Документ
               XElement SVSF = new XElement("СвСчФакт");
               XElement TabSF = new XElement("ТаблСчФакт");
               XElement ProdPer = new XElement("СвПродПер");
               XElement Podp = new XElement("Подписант");

               DOC.Add(SVSF);
               DOC.Add(TabSF);
               DOC.Add(ProdPer);
               DOC.Add(Podp);

               //СвСчФакт

               XAttribute DateSF = new XAttribute("ДатаСчФ", Convert.ToDateTime(CurrDataUPD[3]).ToString(@"dd.MM.yyyy"));
               XAttribute Kod = new XAttribute("КодОКВ", "643");
               XAttribute NomSF = new XAttribute("НомерСчФ", CurrDataUPD[2]);

               SVSF.Add(DateSF);
               SVSF.Add(Kod);
               SVSF.Add(NomSF);
   /*XElement IsprSF = new XElement("ИспрСчФ");
               SVSF.Add(IsprSF);
               XAttribute NmIsprSF = new XAttribute("НомИспрСчФ", "1");
               XAttribute DtIsprSF = new XAttribute("ДатаИспрСчФ", DateTime.Today.ToString(@"dd.MM.yyyy"));
               IsprSF.Add(NmIsprSF);
               IsprSF.Add(DtIsprSF);

               XElement Svprod = new XElement("СвПрод");
               XElement GruzOt = new XElement("ГрузОт");
               XElement GruzPoluch = new XElement("ГрузПолуч");
               XElement SvPokup = new XElement("СвПокуп");
               XElement InfPol = new XElement("ИнфПолФХЖ1");

               SVSF.Add(Svprod);
               SVSF.Add(GruzOt);
               SVSF.Add(GruzPoluch);
               SVSF.Add(SvPokup);
               SVSF.Add(InfPol);

               //СвПрод
               XElement IdSv = new XElement("ИдСв");
               XElement Adres = new XElement("Адрес");

               Svprod.Add(IdSv);
               Svprod.Add(Adres);

               //ИдСв
               XElement SvUluch = new XElement("СвЮЛУч");

               IdSv.Add(SvUluch);

               XAttribute INN = new XAttribute("ИННЮЛ", FirmInfo[1]);
               XAttribute Kpp = new XAttribute("КПП", FirmInfo[2]);
               XAttribute Name = new XAttribute("НаимОрг", FirmInfo[0]);

               SvUluch.Add(INN);
               SvUluch.Add(Kpp);
               SvUluch.Add(Name);

               //Адрес
               XElement AdrRF = new XElement("АдрРФ");
               XAttribute KodReg = new XAttribute("КодРегион", "00");

               Adres.Add(AdrRF);
               AdrRF.Add(KodReg);

               //ГрузОт
               XElement OnJe = new XElement("ОнЖе", "он же");
               GruzOt.Add(OnJe);

               //ГрузПолуч
               XElement IdSvGr = new XElement("ИдСв");
               XElement AdresGr = new XElement("Адрес");
               XElement AdrRFGp = new XElement("АдрРФ");
               XAttribute KodRegGp = new XAttribute("КодРегион", "00");//завести все регионы в картотеку к/а

               AdresGr.Add(AdrRFGp);
               AdrRFGp.Add(KodRegGp);
               GruzPoluch.Add(IdSvGr);
               GruzPoluch.Add(AdresGr);


               //ИдСв
               XElement SvUluc = new XElement("СвЮЛУч");
               XAttribute INNGP = new XAttribute("ИННЮЛ", DelivInfo[3]);
               XAttribute NameGP = new XAttribute("НаимОрг", "#NAME#");

               IdSvGr.Add(SvUluc);
               SvUluc.Add(INNGP);
               SvUluc.Add(NameGP);

               //Адрес  Пока пустой. Возможно нужен будет код региона.

               //СвПокуп

               XElement IdSvPok = new XElement("ИдСв");
               XElement AdresPok = new XElement("Адрес");
               XElement AdrRFPoc = new XElement("АдрРФ");
               XAttribute KodRegPoc = new XAttribute("КодРегион", "00");



               SvPokup.Add(IdSvPok);
               SvPokup.Add(AdresPok);
               AdresPok.Add(AdrRFPoc);
               AdrRFPoc.Add(KodRegPoc);

               //ИдСв
               XElement SvUlucPok = new XElement("СвЮЛУч");
               XAttribute INNPok = new XAttribute("ИННЮЛ", PlatInfo[3]);
               XAttribute KPPPok = new XAttribute("КПП", PlatInfo[4]);
               XAttribute NamePok = new XAttribute("НаимОрг", "#NAME#");

               IdSvPok.Add(SvUlucPok);
               SvUlucPok.Add(KPPPok);
               SvUlucPok.Add(INNPok);
               SvUlucPok.Add(NamePok);

               //Адрес  Пока пустой. Возможно нужен будет код региона.

               //ИнфПолФХЖ1
               XElement TextInf = new XElement("ТекстИнф");
               XAttribute Znach = new XAttribute("Значен", ILN_Edi);
               XAttribute Ident = new XAttribute("Идентиф", "отправитель");

               XElement TextInf2 = new XElement("ТекстИнф");
               XAttribute Znach2 = new XAttribute("Значен", PlatInfo[2]);
               XAttribute Ident2 = new XAttribute("Идентиф", "получатель");

               XElement TextInf3 = new XElement("ТекстИнф");
               XAttribute Znach3 = new XAttribute("Значен", DelivInfo[2]);
               XAttribute Ident3 = new XAttribute("Идентиф", "грузополучатель");


               InfPol.Add(TextInf);
               TextInf.Add(Znach);
               TextInf.Add(Ident);

               InfPol.Add(TextInf2);
               TextInf2.Add(Znach2);
               TextInf2.Add(Ident2);

               InfPol.Add(TextInf3);
               TextInf3.Add(Znach3);
               TextInf3.Add(Ident3);

               //ТаблСчФакт

               for (int i = 0; i < CntLinesInvoice; i++)
               {
                   object[] BICode = Verifiacation.GetBuyerItemCode(Convert.ToString(PlatInfo[5]), Convert.ToString(Item[i, 1]));

                   XElement SvedTov = new XElement("СведТов");
                   TabSF.Add(SvedTov);

                   XAttribute KolTov = new XAttribute("КолТов", Math.Round(Convert.ToDecimal(Item[i, 4]), 2));
                   XAttribute NaimTov = new XAttribute("НаимТов", Item[i, 3]);
                   XAttribute NalSt = new XAttribute("НалСт", Convert.ToString(Convert.ToInt32(Item[i, 10])) + "%");
                   XAttribute NomStr = new XAttribute("НомСтр", Convert.ToString(i + 1));
                   XAttribute OKEI = new XAttribute("ОКЕИ_Тов", Item[i, 8]);
                   XAttribute BezNDS = new XAttribute("СтТовБезНДС", Math.Round(Convert.ToDecimal(Item[i, 13]), 2));
                   XAttribute sNDS = new XAttribute("СтТовУчНал", Math.Round(Convert.ToDecimal(Item[i, 13]) + Convert.ToDecimal(Item[i, 12]), 2));
                   XAttribute CenaTov = new XAttribute("ЦенаТов", Item[i, 5]);

                   SvedTov.Add(KolTov);
                   SvedTov.Add(NaimTov);
                   SvedTov.Add(NalSt);
                   SvedTov.Add(NomStr);
                   SvedTov.Add(OKEI);
                   SvedTov.Add(BezNDS);
                   SvedTov.Add(sNDS);
                   SvedTov.Add(CenaTov);

                   XElement Akciz = new XElement("Акциз");
                   XElement bezAkciz = new XElement("БезАкциз", "без акциза");

                   SvedTov.Add(Akciz);
                   Akciz.Add(bezAkciz);

                   XElement SumNal = new XElement("СумНал");
                   XElement SumNal2 = new XElement("СумНал", Item[i, 12]);

                   SvedTov.Add(SumNal);
                   SumNal.Add(SumNal2);

                   XElement InfPolFHJ = new XElement("ИнфПолФХЖ2");
                   XElement InfPolFHJ2 = new XElement("ИнфПолФХЖ2");

                   SvedTov.Add(InfPolFHJ);
                   SvedTov.Add(InfPolFHJ2);

                   XAttribute Znachen = new XAttribute("Значен", CurrDataUPD[5]);
                   XAttribute Identif = new XAttribute("Идентиф", "номер_заказа");

                   XAttribute Znachen2 = new XAttribute("Значен", Convert.ToString(BICode[0]));
                   XAttribute Identif2 = new XAttribute("Идентиф", "код_материала");

                   XElement DopSvedTov = new XElement("ДопСведТов");
                   XAttribute NaimEdIzm = new XAttribute("НаимЕдИзм", Item[i, 9]);

                   SvedTov.Add(DopSvedTov);
                   DopSvedTov.Add(NaimEdIzm);

                   InfPolFHJ.Add(Znachen);
                   InfPolFHJ.Add(Identif);

                   InfPolFHJ2.Add(Znachen2);
                   InfPolFHJ2.Add(Identif2);


               }

               XElement VsegoOpl = new XElement("ВсегоОпл");
               XAttribute VsegoBezNds = new XAttribute("СтТовБезНДСВсего", Math.Round(((Convert.ToDecimal(CurrDataUPD[10]) - Convert.ToDecimal(CurrDataUPD[11]))), 2));
               XAttribute VsegoSNDS = new XAttribute("СтТовУчНалВсего", Math.Round(Convert.ToDecimal(CurrDataUPD[10]), 2));

               TabSF.Add(VsegoOpl);
               VsegoOpl.Add(VsegoBezNds);
               VsegoOpl.Add(VsegoSNDS);

               XElement SumNalVsego = new XElement("СумНалВсего");
               XElement SumNal1 = new XElement("СумНал", Math.Round(Convert.ToDecimal(CurrDataUPD[10]), 2) - (Math.Round(((Convert.ToDecimal(CurrDataUPD[10]) - Convert.ToDecimal(CurrDataUPD[11]))), 2)));

               VsegoOpl.Add(SumNalVsego);
               SumNalVsego.Add(SumNal1);

               XElement Svper = new XElement("СвПер");
               XAttribute SodOper = new XAttribute("СодОпер", "Товары переданы");

               ProdPer.Add(Svper);
               Svper.Add(SodOper);

               XElement OsnPer = new XElement("ОснПер");
               XElement SvLicPer = new XElement("СвЛицПер");
               XAttribute NaimOsn = new XAttribute("НаимОсн", "Заказ");
               XAttribute NomOsn = new XAttribute("НомОсн", CurrDataUPD[5]);
               XAttribute DataOsn = new XAttribute("ДатаОсн", Convert.ToDateTime(CurrDataUPD[6]).ToString(@"dd.MM.yyyy"));

               Svper.Add(OsnPer);
               Svper.Add(SvLicPer);
               OsnPer.Add(NaimOsn);
               OsnPer.Add(NomOsn);
               OsnPer.Add(DataOsn);

               //СвЛицПер
               XElement RabOrgProd = new XElement("РабОргПрод");
               XAttribute Doljnost = new XAttribute("Должность", "Водитель");
               SvLicPer.Add(RabOrgProd);
               RabOrgProd.Add(Doljnost);

               XElement pFIO = new XElement("ФИО");
               RabOrgProd.Add(pFIO);

               string sFIO = Convert.ToString(CurrDataUPD[22]);
               int len = sFIO.Length;
               int p1 = sFIO.IndexOf(" ");
               int p2 = sFIO.LastIndexOf(" ");
               string sF = sFIO.Remove(p1);
               string sI = sFIO.Substring(p1, (p2 - p1));
               string sO = sFIO.Substring(p2, len - p2);
               XAttribute F = new XAttribute("Фамилия", sF.Trim());
               XAttribute I = new XAttribute("Имя", sI.Trim());
               XAttribute O = new XAttribute("Отчество", sO.Trim());

               pFIO.Add(F);
               pFIO.Add(I);
               pFIO.Add(O);

               //Подписант
               XAttribute obl = new XAttribute("ОблПолн", "6");
               XAttribute osn = new XAttribute("ОснПолн", "Директор");
               XAttribute status = new XAttribute("Статус", "1");

               Podp.Add(obl);
               Podp.Add(osn);
               Podp.Add(status);

               object[] SignerInfo = Verifiacation.GetSigner();

               XElement UL = new XElement("ЮЛ");
               XElement FIO = new XElement("ФИО");

               XAttribute dolj = new XAttribute("Должн", "Директор");
               XAttribute inndir = new XAttribute("ИННЮЛ", "9999999999");
               XAttribute namedir = new XAttribute("Имя", SignerInfo[1]);
               XAttribute famdir = new XAttribute("Фамилия", SignerInfo[0]);

               Podp.Add(UL);
               UL.Add(FIO);

               UL.Add(dolj);
               UL.Add(inndir);

               FIO.Add(namedir);
               FIO.Add(famdir);

               //------сохранение документа-----------

               try
               {
                   xdoc.Save(UPDEDI + nameUPD);
                   xdoc.Save(ArchiveEDI + nameUPD);
                   string message = "EDISOFT. УПД " + nameUPD + " создан в " + UPDEDI;
                   Console.WriteLine(message);
                   DispOrders.WriteProtocolEDI("УПД", nameUPD, PlatInfo[0] + " - " + PlatInfo[1], 0, DelivInfo[0] + " - " + DelivInfo[1], "УПД сформирован", DateTime.Now, Convert.ToString(CurrDataUPD[6]), "EDISOFT");
                   //ReportEDI.RecordCountEDoc("EDI-Софт", "УПД", 1);
                   DispOrders.WriteEDiSentDoc("3", nameUPD, Convert.ToString(CurrDataUPD[1]), Convert.ToString(CurrDataUPD[2]), "46", Convert.ToString(Math.Round(Convert.ToDecimal(CurrDataUPD[10]), 2)), Convert.ToString(CurrDataUPD[5]), 1);
                   //запись в лог о удаче
               }
               catch (IOException e)
               {
                   string message_error = "EDISOFT. Не могу создать xml файл УПД в " + UPDEDI + ". Нет доступа или диск переполнен.";
                   DispOrders.WriteProtocolEDI("УПД", nameUPD, PlatInfo[0] + " - " + PlatInfo[1], 10, DelivInfo[0] + " - " + DelivInfo[1], "УПД не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUPD[6]), "EDISOFT");
                   Console.WriteLine(message_error);
                   DispOrders.WriteErrorLog(e.Message);
                   //запись в лог о неудаче
               }

           }*/

        /*   public static void CreateEdiUKD(List<object> CurrDataUKD)
           {
               //получение путей
               string ArchiveEDI = DispOrders.GetValueOption("EDI-СОФТ.АРХИВ");

               //получение данных
               object[] DelivInfo = Verifiacation.GetDataFromPtnRCD(Convert.ToString(CurrDataUKD[21]));
               object[] PlatInfo = Verifiacation.GetDataFromPtnCD(Convert.ToString(CurrDataUKD[20]));
               int CntLinesDV = Verifiacation.CountItemsInOrder(Convert.ToString(CurrDataUKD[19]), 1);

               bool PCE = Verifiacation.UsePCE(Convert.ToString(CurrDataUKD[20]));//проверка на использование штук в исходящих докуметов

               int CntLinesInvoice = Verifiacation.CountItemsInInvoice(Convert.ToString(CurrDataUKD[1]));

               object[,] Item = DispOrders.GetItemsFromInvoice(Convert.ToString(CurrDataUKD[1]), CntLinesDV, PCE); //данные по КСФ
               String ILNRecvr = Verifiacation.GetILNReceiver(Convert.ToString(PlatInfo[8]));
               String ILNBur = Verifiacation.GetILNBuyer(Convert.ToString(PlatInfo[8]));

               //какой gln номер использовать
               bool UseMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(DelivInfo[8]));
               string ILN_Edi;
               string UKDEDI;
               object[] FirmInfo;
               object[] FirmAdr;
               if (UseMasterGLN == false)//используем данные текущего предприятия
               {
                   ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ILN_EDI");
                   UKDEDI = DispOrders.GetValueOption("EDI-СОФТ.УКД");
                   FirmInfo = Verifiacation.GetFirmInfo();
                   FirmAdr = Verifiacation.GetFirmAdr();

               }
               else//используем данные головного предприятия
               {
                   ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");
                   FirmInfo = Verifiacation.GetMasterFirmInfo();
                   FirmAdr = Verifiacation.GetMasterFirmAdr();
                   try
                   {
                       UKDEDI = DispOrders.GetValueOption("EDI-СОФТ.ЭКСПОРТ");
                   }
                   catch
                   {
                       UKDEDI = DispOrders.GetValueOption("EDI-СОФТ.УКД");
                   }
               }

               //ID файла
               string dd = DateTime.Today.ToString(@"yyyyMMdd");
               string guid = Convert.ToString(Guid.NewGuid());
               string FileID = "ON_KORSCHFDOPPR_" + ILN_Edi + "_" + ILNRecvr + "_" + dd + "_" + guid;
               string nameUKD = FileID + ".xml";


               XDocument xdoc = new XDocument();

               //основные элементы (1 уровень)
               XElement File = new XElement("Файл");

               XElement ID = new XElement("СвУчДокОбор");
               XElement DOC = new XElement("Документ");

               XAttribute VersProg = new XAttribute("ВерсПрог", "Edisoft");
               XAttribute VersForm = new XAttribute("ВерсФорм", "5.01");
               XAttribute IdFile = new XAttribute("ИдФайл", "000000000000000000000000000000000000");

               xdoc.Add(File);
               File.Add(IdFile);
               File.Add(VersProg);
               File.Add(VersForm);

               XAttribute IdSender = new XAttribute("ИдОтпр", "0000000000");
               XAttribute Idreciever = new XAttribute("ИдПол", "0000000000");

               XElement InfOrg = new XElement("СвОЭДОтпр");
               XAttribute INNUL = new XAttribute("ИННЮЛ", "7801471082");
               XAttribute IDEDO = new XAttribute("ИдЭДО", "2IJ");
               XAttribute NaimOrg = new XAttribute("НаимОрг", "Эдисофт, ООО");

               File.Add(ID);
               File.Add(DOC);

               ID.Add(IdSender);
               ID.Add(Idreciever);

               ID.Add(InfOrg);

               InfOrg.Add(INNUL);
               InfOrg.Add(IDEDO);
               InfOrg.Add(NaimOrg);

               XAttribute KND = new XAttribute("КНД", "1115127");
               XAttribute Function = new XAttribute("Функция", "КСЧФДИС");
               XAttribute PoFakHJ = new XAttribute("ПоФактХЖ", "Документ об отгрузке товаров (выполнении работ), передаче имущественных прав (документ об оказании услуг)");
               XAttribute NaimDocOpr = new XAttribute("НаимДокОпр", "Счет-фактура и документ об отгрузке товаров (выполнении работ), передаче имущественных прав (документ об оказании услуг)");
               XAttribute TimeF = new XAttribute("ВремИнфПр", DateTime.Today.ToString(@"hh.mm.ss"));
               XAttribute DateF = new XAttribute("ДатаИнфПр", DateTime.Today.ToString(@"dd.MM.yyyy"));
               XAttribute NameOrg = new XAttribute("НаимЭконСубСост", "АО ГК РОСМОЛ");
               XAttribute OsnDover = new XAttribute("ОснДоверОргСост", "основание");

               DOC.Add(KND);
               DOC.Add(Function);
               DOC.Add(PoFakHJ);
               DOC.Add(NaimDocOpr);
               DOC.Add(TimeF);
               DOC.Add(DateF);
               DOC.Add(NameOrg);
               DOC.Add(OsnDover);

               //Документ
               XElement SVKSF = new XElement("СвКСчФ");
               XElement TabKSF = new XElement("ТаблКСчФ");
               XElement SodFHJZ = new XElement("СодФХЖ3");
               XElement Podp = new XElement("Подписант");

               DOC.Add(SVKSF);
               DOC.Add(TabKSF);
               DOC.Add(SodFHJZ);
               DOC.Add(Podp);

               //СвКСчФ

               XAttribute DateSF = new XAttribute("ДатаКСчФ", Convert.ToDateTime(CurrDataUKD[3]).ToString(@"dd.MM.yyyy"));
               XAttribute Kod = new XAttribute("КодОКВ", "643");
               XAttribute NomKSF = new XAttribute("НомерКСчФ", CurrDataUKD[2]);

               SVKSF.Add(NomKSF);
               SVKSF.Add(DateSF);
               SVKSF.Add(Kod);


               XElement Schf = new XElement("СчФ");
               XElement Svprod = new XElement("СвПрод");
               XElement SvPokup = new XElement("СвПокуп");
               XElement InfPol = new XElement("ИнфПолФХЖ1");

               SVKSF.Add(Schf);
               SVKSF.Add(Svprod);
               SVKSF.Add(SvPokup);
               SVKSF.Add(InfPol);

               //СчФ
               XAttribute NomSfch = new XAttribute("НомерСчФ", CurrDataUKD[18]);
               XAttribute DtSF = new XAttribute("ДатаСчФ", Convert.ToDateTime(CurrDataUKD[17]).ToString(@"dd.MM.yyyy"));

               Schf.Add(NomSfch);
               Schf.Add(DtSF);

               //ИспрКСчФ
               /*XElement IsprSF = new XElement("ИспрСчФ");
               SVKSF.Add(IsprSF);
               XAttribute NmIsprSF = new XAttribute("НомИспрКСчФ", "1");
               XAttribute DtIsprSF = new XAttribute("ДатаИспрКСчФ", DateTime.Today.ToString(@"dd.MM.yyyy"));
               IsprSF.Add(NmIsprSF);
               IsprSF.Add(DtIsprSF);

               //СвПрод
               XElement IdSv = new XElement("ИдСв");
               XElement Adres = new XElement("Адрес");

               Svprod.Add(IdSv);
               Svprod.Add(Adres);

               //ИдСв
               XElement SvUluch = new XElement("СвЮЛУч");

               IdSv.Add(SvUluch);

               XAttribute INN = new XAttribute("ИННЮЛ", FirmInfo[1]);
               XAttribute Kpp = new XAttribute("КПП", FirmInfo[2]);
               XAttribute Name = new XAttribute("НаимОрг", FirmInfo[0]);

               SvUluch.Add(INN);
               SvUluch.Add(Kpp);
               SvUluch.Add(Name);

               //Адрес
               XElement AdrRF = new XElement("АдрРФ");
               XAttribute KodReg = new XAttribute("КодРегион", "00");

               Adres.Add(AdrRF);
               AdrRF.Add(KodReg);

               //СвПокуп

               XElement IdSvPok = new XElement("ИдСв");
               XElement AdresPok = new XElement("Адрес");
               XElement AdrRFPoc = new XElement("АдрРФ");
               XAttribute KodRegPoc = new XAttribute("КодРегион", "00");


               SvPokup.Add(IdSvPok);
               SvPokup.Add(AdresPok);
               AdresPok.Add(AdrRFPoc);
               AdrRFPoc.Add(KodRegPoc);

               //ИдСв
               XElement SvUlucPok = new XElement("СвЮЛУч");
               XAttribute INNPok = new XAttribute("ИННЮЛ", PlatInfo[3]);
               XAttribute KPPPok = new XAttribute("КПП", PlatInfo[4]);
               XAttribute NamePok = new XAttribute("НаимОрг", "#NAME#");

               IdSvPok.Add(SvUlucPok);
               SvUlucPok.Add(KPPPok);
               SvUlucPok.Add(INNPok);
               SvUlucPok.Add(NamePok);

               //Адрес  Пока пустой. Возможно нужен будет код региона.

               //ИнфПолФХЖ1
               XAttribute IdFileInfPol = new XAttribute("ИдФайлИнфПол", guid);

               InfPol.Add(IdFileInfPol);

               XElement TextInfo = new XElement("ТекстИнф");
               XAttribute Identif = new XAttribute("Идентиф", "отправитель");
               XAttribute Znachen = new XAttribute("Значен", ILN_Edi);

               XElement TextInfo2 = new XElement("ТекстИнф");
               XAttribute Znach2 = new XAttribute("Значен", PlatInfo[2]);
               XAttribute Ident2 = new XAttribute("Идентиф", "получатель");

               XElement TextInfo3 = new XElement("ТекстИнф");
               XAttribute Znach3 = new XAttribute("Значен", DelivInfo[2]);
               XAttribute Ident3 = new XAttribute("Идентиф", "грузополучатель ");

               InfPol.Add(TextInfo);
               TextInfo.Add(Identif);
               TextInfo.Add(Znachen);

               InfPol.Add(TextInfo2);
               TextInfo2.Add(Ident2);
               TextInfo2.Add(Znach2);

               InfPol.Add(TextInfo3);
               TextInfo3.Add(Ident3);
               TextInfo3.Add(Znach3);

               //ТаблСчФакт

               decimal UvTotalNoNds = 0;
               decimal UvToTalWithNds = 0;
               decimal UmTotalNoNds = 0;
               decimal UmToTalWithNds = 0;
               for (int i = 0; i < CntLinesInvoice; i++)
               {
                   object[] BICode = Verifiacation.GetBuyerItemCode(Convert.ToString(PlatInfo[5]), Convert.ToString(Item[i, 1]));
                   object[,] prevItem = DispOrders.GetItemFromPrevDoc(Convert.ToString(CurrDataUKD[14]), Convert.ToString(Item[i, 0])); //исходная СФ

                   XElement SvedTov = new XElement("СведТов");

                   TabKSF.Add(SvedTov);

                   XAttribute NomStr = new XAttribute("НомСтр", Convert.ToString(i + 1));
                   XAttribute NaimTov = new XAttribute("НаимТов", Item[i, 3]);
                   XAttribute OKEIDo = new XAttribute("ОКЕИ_ТовДо", prevItem[0, 8]);
                   XAttribute OKEIPosle = new XAttribute("ОКЕИ_ТовПосле", Item[i, 8]);
                   XAttribute KolTovDo = new XAttribute("КолТовДо", Math.Round((Convert.ToDecimal(prevItem[0, 4])), 2));
                   XAttribute KolTovPosle = new XAttribute("КолТовПосле", Math.Round((Convert.ToDecimal(prevItem[0, 4]) + Convert.ToDecimal(Item[i, 4]))));
                   XAttribute CenaTovDo = new XAttribute("ЦенаТовДо", Math.Round(Convert.ToDecimal(prevItem[0, 5]), 2));
                   XAttribute CenaTovPosle = new XAttribute("ЦенаТовПосле", Math.Round(Convert.ToDecimal(Item[i, 5]), 2));
                   XAttribute NalStDo = new XAttribute("НалСтДо", Convert.ToString(Convert.ToInt32(prevItem[0, 9])) + "%");
                   XAttribute NalStPosle = new XAttribute("НалСтПосле", Convert.ToString(Convert.ToInt32(Item[i, 10])) + "%");

                   SvedTov.Add(NomStr);
                   SvedTov.Add(NaimTov);
                   SvedTov.Add(OKEIDo);
                   SvedTov.Add(OKEIPosle);
                   SvedTov.Add(KolTovDo);
                   SvedTov.Add(KolTovPosle);
                   SvedTov.Add(CenaTovDo);
                   SvedTov.Add(CenaTovPosle);
                   SvedTov.Add(NalStDo);
                   SvedTov.Add(NalStPosle);

                   XElement StTovBezNds = new XElement("СтТовБезНДС");
                   XElement AkcizDo = new XElement("АкцизДо");
                   XElement AkzicPosle = new XElement("АкцизПосле");
                   XElement AkcizRazn = new XElement("АкцизРазн");
                   XElement SumNalDo = new XElement("СумНалДо");
                   XElement SumNalPosle = new XElement("СумНалПосле");
                   XElement SumNalRazn = new XElement("СумНалРазн");
                   XElement StTovUchNal = new XElement("СтТовУчНал");
                   XElement InfPol1 = new XElement("ИнфПолФХЖ2");
                   XElement InfPol2 = new XElement("ИнфПолФХЖ2");

                   SvedTov.Add(StTovBezNds);
                   SvedTov.Add(AkcizDo);
                   SvedTov.Add(AkzicPosle);
                   SvedTov.Add(AkcizRazn);
                   SvedTov.Add(SumNalDo);
                   SvedTov.Add(SumNalPosle);
                   SvedTov.Add(SumNalRazn);
                   SvedTov.Add(StTovUchNal);

                   //СтТовБезНДС
                   decimal a = Math.Round(Convert.ToDecimal(prevItem[0, 12]), 2); //сумма до изменения без налога
                   decimal b = Math.Round(Convert.ToDecimal(prevItem[0, 12]), 2) + Math.Round(Convert.ToDecimal(Item[i, 13]),2); //сумма после изменения без налога
                   XAttribute StDoBezNDS = new XAttribute("СтоимДоИзм", a);
                   XAttribute StPosleBezNDS = new XAttribute("СтоимПослеИзм", b);
                   string StoimUvelOrUm = "";
                   string SumUvelOrUm = "";
                   if ((b - a) > 0) //если стоимость после изменения больше стоимости до изменения
                   {
                       StoimUvelOrUm = "СтоимУвел";
                       SumUvelOrUm = "СумУвел";
                       if (Convert.ToDecimal(Item[i, 4]) != 0)
                       {
                           UvTotalNoNds = UvTotalNoNds + Math.Abs(Convert.ToDecimal(Item[i, 13])); ;
                           UvToTalWithNds = UvToTalWithNds + Math.Abs(Convert.ToDecimal(Item[i, 14]));
                       }
                       else
                       {
                           UvTotalNoNds = UvTotalNoNds + 0;
                           UvToTalWithNds = UvToTalWithNds + 0;
                       }
                   }
                   else //уменьшилась сумма
                   {
                       StoimUvelOrUm = "СтоимУм";
                       SumUvelOrUm = "СумУм";

                       if (Convert.ToDecimal(Item[i, 4]) != 0)
                       {
                           UmTotalNoNds = UmTotalNoNds + Math.Abs(Convert.ToDecimal(Item[i, 13]));
                           UmToTalWithNds = UmToTalWithNds + Math.Abs(Convert.ToDecimal(Item[i, 14]));
                       }
                       else
                       {
                           UmTotalNoNds = UmTotalNoNds + 0;
                           UmToTalWithNds = UmToTalWithNds + 0;
                       }
                   }   
                   XAttribute StUvelUmBezNDS = new XAttribute(StoimUvelOrUm, Math.Abs(b - a));

                   StTovBezNds.Add(StDoBezNDS);
                   StTovBezNds.Add(StPosleBezNDS);
                   StTovBezNds.Add(StUvelUmBezNDS);

                   XElement bezAkciz = new XElement("БезАкциз", "без акциза");
                   XElement SumUvel = new XElement("СумУвел", "0.00");
                   AkcizDo.Add(bezAkciz);
                   AkzicPosle.Add(bezAkciz);
                   AkcizRazn.Add(SumUvel);

                   decimal smNdsDo = Math.Round(Convert.ToDecimal(prevItem[0, 11]), 2);// сумма НДС по позиции до изменения
                   decimal smNdsPosle = Math.Abs(Math.Round(Convert.ToDecimal(prevItem[0, 11]), 2) + Math.Round(Convert.ToDecimal(Item[i, 12]),2));// сумма НДС по позиции после изменения
                   XElement SumNDS1 = new XElement("СумНДС", smNdsDo);
                   XElement SumNDS2 = new XElement("СумНДС", smNdsPosle); 
                   XElement SumUvelUmNDS = new XElement(SumUvelOrUm, Math.Abs(smNdsPosle-smNdsDo)); //разница

                   SumNalDo.Add(SumNDS1);
                   SumNalPosle.Add(SumNDS2);
                   SumNalRazn.Add(SumUvelUmNDS);

                   decimal smDo = Math.Round(Convert.ToDecimal(prevItem[0, 13]), 2); //стоимость до изменения
                   decimal smPosle = Math.Abs(Math.Round(Convert.ToDecimal(prevItem[0, 13]), 2) + Math.Round(Convert.ToDecimal(Item[i, 14]), 2)); //стоимость после изменения
                   XAttribute StDo = new XAttribute("СтоимДоИзм", smDo);
                   XAttribute StPosle = new XAttribute("СтоимПослеИзм", smPosle);
                   XAttribute StUvelUm = new XAttribute(StoimUvelOrUm, Math.Abs(smPosle-smDo));

                   StTovUchNal.Add(StDo);
                   StTovUchNal.Add(StPosle);
                   StTovUchNal.Add(StUvelUm);

                   XElement InfPolFHJ = new XElement("ИнфПолФХЖ2");
                   XElement InfPolFHJ2 = new XElement("ИнфПолФХЖ2");

                   SvedTov.Add(InfPolFHJ);
                   SvedTov.Add(InfPolFHJ2);

                   XAttribute Znachen1 = new XAttribute("Значен", CurrDataUKD[5]);
                   XAttribute Identif1 = new XAttribute("Идентиф", "номер_заказа");

                   XAttribute Znachen2 = new XAttribute("Значен", Convert.ToString(BICode[0]));
                   XAttribute Identif2 = new XAttribute("Идентиф", "код_материала");

                   InfPolFHJ.Add(Znachen1);
                   InfPolFHJ.Add(Identif1);

                   InfPolFHJ2.Add(Znachen2);
                   InfPolFHJ2.Add(Identif2);
               }

               if((UvTotalNoNds != 0) || (UvToTalWithNds != 0))
               {
                   XElement VsegoOpl = new XElement("ВсегоУвел");
                   XAttribute noNds = new XAttribute("СтТовБезНДСВсего", Math.Round(UvTotalNoNds, 2));
                   XAttribute wNds = new XAttribute("СтТовУчНалВсего", Math.Round(UvToTalWithNds, 2));

                   TabKSF.Add(VsegoOpl);
                   VsegoOpl.Add(noNds);
                   VsegoOpl.Add(wNds);

                   XElement SumNal = new XElement("СумНал");
                   XElement SumNDS = new XElement("СумНДС", Math.Round(UvToTalWithNds, 2) - Math.Round(UvTotalNoNds, 2));

                   VsegoOpl.Add(SumNal);
                   SumNal.Add(SumNDS);
               }

               if ((UmTotalNoNds != 0) || (UmToTalWithNds != 0))
               {
                   XElement VsegoOpl = new XElement("ВсегоУм");
                   XAttribute noNds = new XAttribute("СтТовБезНДСВсего", Math.Round(UmTotalNoNds, 2));
                   XAttribute wNds = new XAttribute("СтТовУчНалВсего", Math.Round(UmToTalWithNds, 2));

                   TabKSF.Add(VsegoOpl);
                   VsegoOpl.Add(noNds);
                   VsegoOpl.Add(wNds);

                   XElement SumNal = new XElement("СумНал");
                   XElement SumNDS = new XElement("СумНДС", Math.Round(UmToTalWithNds, 2) - Math.Round(UmTotalNoNds, 2));

                   VsegoOpl.Add(SumNal);
                   SumNal.Add(SumNDS);
               }

               //СодФХЖ3
               XAttribute dtNapr = new XAttribute("ДатаНапр", DateTime.Today.ToString(@"dd.MM.yyyy"));
               XAttribute InSved = new XAttribute("ИныеСвИзмСтоим", "Изменения");
               XAttribute PeredatDoc = new XAttribute("ПередатДокум", Convert.ToString(CurrDataUKD[12]));
               XAttribute SodOper = new XAttribute("СодОпер", "Иные");

               SodFHJZ.Add(dtNapr);
               SodFHJZ.Add(InSved);
               SodFHJZ.Add(PeredatDoc);
               SodFHJZ.Add(SodOper);

               XElement OsnKor = new XElement("ОснКор");

               SodFHJZ.Add(OsnKor);

               XAttribute NaimOsn = new XAttribute("НаимОсн", "отсутствует");

               OsnKor.Add(NaimOsn);

               //Подписант
               XAttribute obl = new XAttribute("ОблПолн", "6");
               XAttribute osn = new XAttribute("ОснПолн", "Директор");
               XAttribute status = new XAttribute("Статус", "1");

               Podp.Add(obl);
               Podp.Add(osn);
               Podp.Add(status);

               object[] SignerInfo = Verifiacation.GetSigner();

               XElement UL = new XElement("ЮЛ");
               XElement FIO = new XElement("ФИО");

               XAttribute dolj = new XAttribute("Должн", "Директор");
               XAttribute inndir = new XAttribute("ИННЮЛ", "9999999999");
               XAttribute namedir = new XAttribute("Имя", SignerInfo[1]);
               XAttribute famdir = new XAttribute("Фамилия", SignerInfo[0]);


               Podp.Add(UL);
               UL.Add(FIO);

               UL.Add(dolj);
               UL.Add(inndir);

               FIO.Add(namedir);
               FIO.Add(famdir);

               //------сохранение документа-----------

               try
               {
                   xdoc.Save(UKDEDI + nameUKD);
                   xdoc.Save(ArchiveEDI + nameUKD);
                   string message = "EDISOFT. УПД " + nameUKD + " создан в " + UKDEDI;
                   Console.WriteLine(message);
                   DispOrders.WriteProtocolEDI("УКД", nameUKD, PlatInfo[0] + " - " + PlatInfo[1], 0, DelivInfo[0] + " - " + DelivInfo[1], "УКД сформирован", DateTime.Now, Convert.ToString(CurrDataUKD[6]), "EDISOFT");
                   //ReportEDI.RecordCountEDoc("EDI-Софт", "УКД", 1);
                   DispOrders.WriteEDiSentDoc("3", nameUKD, Convert.ToString(CurrDataUKD[1]), Convert.ToString(CurrDataUKD[2]), "46", Convert.ToString(Math.Round(Convert.ToDecimal(CurrDataUKD[10]), 2)), Convert.ToString(CurrDataUKD[5]), 1);
                   //запись в лог о удаче
               }
               catch (IOException e)
               {
                   string message_error = "EDISOFT. Не могу создать xml файл УКД в " + UKDEDI + ". Нет доступа или диск переполнен.";
                   DispOrders.WriteProtocolEDI("УКД", nameUKD, PlatInfo[0] + " - " + PlatInfo[1], 10, DelivInfo[0] + " - " + DelivInfo[1], "УRД не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUKD[6]), "EDISOFT");
                   Console.WriteLine(message_error);
                   DispOrders.WriteErrorLog(e.Message);
                   //запись в лог о неудаче
               }

           }*/

        public static void CreateKonturCOInvoice(List<object> CurrDataCOInvoice) //0 ProviderOpt, 1 ProviderZkg, 2 NastDoc_Fmt, 3 SklSf_Rcd, 4 SklSf_TpOtg, 5 SklSfA_RcdCor, 6 PrdZkg_NmrExt, 7 PrdZkg_Rcd, 8 PrdZkg_Dt, 9 SklNk_TDrvNm
        {
            //признак необходимости ISOCode
            bool iso = false;

            //получение путей
            string ArchiveKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.АРХИВ");

            //string InvoiceKONTUR = "\\\\fileshare\\EXPIMP\\OrderIntake\\SKBKONTUR\\OUTBOX\\";//test
            //string ArchiveKONTUR = "\\\\fileshare\\EXPIMP\\OrderIntake\\SKBKONTUR\\ARCHIVE\\";

            //генерация имени файла.
            string id; //= Convert.ToString(DateTime.Now); ;   // ну это так, ищу замену    
            string id2 = Convert.ToString(Guid.NewGuid());        // эта строка иногда генерит повторно тот же ИД подряд !!!!!!!!!   и как-то по утрам, потом нормально работает ... 
            id = Convert.ToString(Guid.NewGuid());        // эта строка иногда генерит повторно тот же ИД подряд !!!!!!!!!   и как-то по утрам, потом нормально работает ... 
            if (id == id2)                                  // ну или другая фигня. не понятно. такие файлики уже есть...
                id = id + "_";
            string nameInv = "COINVOIC_" + id + ".xml";

            //Запрос данных Корректировочной СФ
            object[] infoSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataCOInvoice[3])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO

            //Запрос данных Корректируемой (отгрузочной) СФ
            object[] infoCorSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataCOInvoice[5])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO

            Program.WriteLine("СФ " + Convert.ToString(infoSf[0]) + " Корректируемая СФ " + Convert.ToString(infoCorSf[0]));

            //Возвращает заголовок накладной по рсд заказа   
            object[] infoNk = Verifiacation.GetNkDataFromZkg(Convert.ToInt64(CurrDataCOInvoice[7])); //0 SklNk_Nmr, 1 SklNk_Dat

            //получение данных
            object[] DelivInfo = Verifiacation.GetDataFromPtn_Rcd(Convert.ToString(infoSf[4]));   //10 = формат
            object[] PlatInfo = Verifiacation.GetDataFromPtn_Rcd(Convert.ToString(infoSf[2]));
            object KonturPlatGLN = Verifiacation.GetGLNGR(Convert.ToString(infoSf[2]));
            if (String.IsNullOrEmpty(KonturPlatGLN.ToString())) KonturPlatGLN = PlatInfo[2];

            Program.WriteLine("DelivInfo " + Convert.ToString(infoSf[4]) + " PlatInfo " + Convert.ToString(infoSf[2]));
            Program.WriteLine("DelivInfo GLN " + Convert.ToString(DelivInfo[2]) + " PlatInfo GLN " + Convert.ToString(PlatInfo[2]));

            //какой gln номер использовать
            string check = Convert.ToString(DelivInfo[8]);
            bool UseMasterGLN = false;
            if (check.Length != 0)
            {
                UseMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(DelivInfo[8]));
            }
            //bool UseMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(DelivInfo[8]));
            string ILN_Edi, ILN_Edi_S;
            string InvoiceKONTUR;
            object[] FirmInfo, FirmInfo_G;
            object[] FirmAdr, FirmAdr_G;
            if (UseMasterGLN == true) //используем данные головного предприятия, для Агроторгов и Метро в любом случае
            {
                ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");

                FirmInfo = Verifiacation.GetMasterFirmInfo();
                FirmAdr = Verifiacation.GetMasterFirmAdr();
                try
                {
                    InvoiceKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.ЭКСПОРТ");
                }
                catch
                {
                    InvoiceKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.СФ");
                }
            }

            else//используем данные текущего предприятия
            {

                ILN_Edi = DispOrders.GetValueOption("СКБ-КОНТУР.ИЛН_ПРЕДПРИЯТИЯ");
                if (ILN_Edi == "") ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ИЛН");
                InvoiceKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.СФ");
                if (Convert.ToDateTime(PlatInfo[9]) > Convert.ToDateTime(infoSf[1]))      // 13 это дата с которой надо ставить новые данные
                {
                    FirmInfo = Verifiacation.GetFirmInfo("20171130"); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO        // берём как до 01.12.2017
                }
                else
                {
                    FirmInfo = Verifiacation.GetFirmInfo(Convert.ToDateTime(infoSf[1]).ToString("yyyyMMdd"));
                }
                FirmAdr = Verifiacation.GetFirmAdr();
            }

            ILN_Edi_S = DispOrders.GetValueOption("СКБ-КОНТУР.ИЛН_ПРЕДПРИЯТИЯ");
            FirmInfo_G = Verifiacation.GetMasterFirmInfo();
            FirmAdr_G = Verifiacation.GetMasterFirmAdr();

            string sellerISOCode;//костыли для азбуки вкуса
            sellerISOCode = "RU-CHE";

            if (File.Exists(InvoiceKONTUR + nameInv))          // уже есть такие файлы??? почему-то не может сохранить
                nameInv = nameInv = "COINVOIC_" + id + "_.xml"; ;

            int CntLinesInvoice = Verifiacation.CountItemsInInvoice(Convert.ToString(CurrDataCOInvoice[3]));

            bool PCE = Verifiacation.UsePCE(Convert.ToString(infoSf[4]));//проверка на использование штук в исходящих докуметов

            object[,] Item = DispOrders.GetItemsFromInvoice(Convert.ToString(CurrDataCOInvoice[3]), CntLinesInvoice, PCE);

            object[] Total = DispOrders.GetTotal(Convert.ToString(CurrDataCOInvoice[3]), 5);

            XDocument xdoc = new XDocument();

            //основные элементы (1 уровень)
            XElement eDIMessage = new XElement("eDIMessage");
            XElement interchangeHeader = new XElement("interchangeHeader");
            XElement correctiveInvoice = new XElement("correctiveInvoice");

            XAttribute idMessage = new XAttribute("id", id);
            XAttribute creationDateTime = new XAttribute("creationDateTime", (DateTime.Now).ToString("yyyy-MM-dd HH:mm:ss"));

            xdoc.Add(eDIMessage);
            eDIMessage.Add(interchangeHeader);
            eDIMessage.Add(correctiveInvoice);
            eDIMessage.Add(idMessage);

            //------interchangeHeader---------------
            XElement sender = new XElement("sender", ILN_Edi);
            XElement recipient = new XElement("recipient", KonturPlatGLN);       //глн группы либо протсо глн (Convert.ToString(PlatInfo[2]))
            XElement documentType = new XElement("documentType", "COINVOIC");

            interchangeHeader.Add(sender);
            interchangeHeader.Add(recipient);
            interchangeHeader.Add(documentType);
            interchangeHeader.Add(creationDateTime);

            //------invoice------------------------
            XAttribute numberInvoice = new XAttribute("number", infoSf[0]);
            XAttribute dateInvoice = new XAttribute("date", (Convert.ToDateTime(infoSf[1])).ToString("yyyy-MM-dd"));
            XAttribute TypeInvoice = new XAttribute("type", "Original");
            XElement originInvoic = new XElement("originInvoic");
            XElement originOrder = new XElement("originOrder");

            correctiveInvoice.Add(numberInvoice);
            correctiveInvoice.Add(dateInvoice);
            correctiveInvoice.Add(TypeInvoice);
            correctiveInvoice.Add(originInvoic);
            correctiveInvoice.Add(originOrder);

            XAttribute numberoriginInvoic = new XAttribute("number", infoCorSf[0]);
            XAttribute dateoriginInvoic;
            dateoriginInvoic = new XAttribute("date", Convert.ToDateTime(infoCorSf[1]).ToString("yyyy-MM-dd"));

            originInvoic.Add(numberoriginInvoic);
            originInvoic.Add(dateoriginInvoic);

            XAttribute numberoriginOrder = new XAttribute("number", CurrDataCOInvoice[6]);
            XAttribute dateoriginOrder;
            //дату заказа берем из даты документа заказа, если вывалилась ошибка, значит заказ был сделан вручную, дату заказа проставляем как в в заказе ИСПРО
            try
            {
                Program.WriteLine("дата документа заказа " + Convert.ToDateTime(Verifiacation.GetFldFromEdiExch(Convert.ToInt64(CurrDataCOInvoice[7]), "Exch_OrdDocDat")).ToString("yyyy-MM-dd"));

                dateoriginOrder = new XAttribute("date", Convert.ToDateTime(Verifiacation.GetFldFromEdiExch(Convert.ToInt64(CurrDataCOInvoice[7]), "Exch_OrdDocDat")).ToString("yyyy-MM-dd"));
            }
            catch
            {

                dateoriginOrder = new XAttribute("date", Convert.ToDateTime(CurrDataCOInvoice[8]).ToString("yyyy-MM-dd"));
            }

            originOrder.Add(numberoriginOrder);
            originOrder.Add(dateoriginOrder);

            XElement despatchIdentificator = new XElement("despatchIdentificator");
            correctiveInvoice.Add(despatchIdentificator);
            XAttribute numberdespatchIdentificator = new XAttribute("number", infoNk[0]);

            XAttribute datedespatchIdentificator;

            Program.WriteLine("numberoriginOrder " + CurrDataCOInvoice[6] + " numberdespatchIdentificator " + infoNk[0]);

            datedespatchIdentificator = new XAttribute("date", Convert.ToDateTime(infoNk[1]).ToString("yyyy-MM-dd"));

            despatchIdentificator.Add(numberdespatchIdentificator);
            despatchIdentificator.Add(datedespatchIdentificator);

            object[] infoRecAdv = Verifiacation.GetRecAdvInfo(Convert.ToInt64(CurrDataCOInvoice[7]));

            if (infoRecAdv[0] != null)  // это теперь есть во вьюхе, CurrDataInvoice[21],22
            {
                XElement receivingIdentificator = new XElement("receivingIdentificator");
                XAttribute numberreceivingIdentificator = new XAttribute("number", infoRecAdv[0]);
                XAttribute datereceivingIdentificator;

                datereceivingIdentificator = new XAttribute("date", Convert.ToDateTime(infoRecAdv[1]).ToString("yyyy-MM-dd"));

                correctiveInvoice.Add(receivingIdentificator);
                receivingIdentificator.Add(numberreceivingIdentificator);
                receivingIdentificator.Add(datereceivingIdentificator);
            }

            // contractIdentificator = номер контракта и дата при наличии
            object[] ContractInfoInfo = Verifiacation.GetContractInfo(Convert.ToString(infoSf[2])); //контракты
            if (ContractInfoInfo[0] != null)
            {
                XElement contractIdentificator = new XElement("contractIdentificator");
                XAttribute numbercontractIdentificator = new XAttribute("number", ContractInfoInfo[0]);
                XAttribute DatecontractIdentificator = new XAttribute("date", Convert.ToDateTime(ContractInfoInfo[1]).ToString("yyyy-MM-dd"));
                correctiveInvoice.Add(contractIdentificator);
                contractIdentificator.Add(numbercontractIdentificator);
                contractIdentificator.Add(DatecontractIdentificator);
            }

            XElement seller = new XElement("seller");
            XElement buyer = new XElement("buyer");
            XElement invoicee = new XElement("invoicee");
            XElement deliveryInfo = new XElement("deliveryInfo");
            XElement lineItems = new XElement("lineItems");

            correctiveInvoice.Add(seller);
            correctiveInvoice.Add(buyer);
            correctiveInvoice.Add(invoicee);
            correctiveInvoice.Add(deliveryInfo);
            correctiveInvoice.Add(lineItems);

            //--------seller-----------------------
            XElement gln = new XElement("gln", ILN_Edi);
            seller.Add(gln);

            XElement organization = new XElement("organization");
            XElement russianAddress = new XElement("russianAddress");
            seller.Add(organization);
            seller.Add(russianAddress);

            if (UseMasterGLN == false)
            {
                //--------organization------------------
                XElement name = new XElement("name", "АО \"Группа Компаний \"Российское Молоко\" (АО \"ГК \"РОСМОЛ\")");
                XElement inn = new XElement("inn", FirmInfo_G[1]);
                XElement kpp = new XElement("kpp", FirmInfo[2]);
                
                organization.Add(name);
                organization.Add(inn);
                organization.Add(kpp);
                
                //---------russianAddress----------------
                XElement city = new XElement("city", Convert.ToString(FirmAdr_G[1]));
                XElement street = new XElement("street", Convert.ToString(FirmAdr_G[0]));
                XElement regionISOCode = new XElement("regionISOCode", sellerISOCode);
                XElement postalCode = new XElement("postalCode", Convert.ToString(FirmAdr_G[3]));
                
                russianAddress.Add(city);
                russianAddress.Add(street);
                russianAddress.Add(regionISOCode);
                russianAddress.Add(postalCode);
            }
            else
            {
                XElement name = new XElement("name", FirmInfo[0]);
                XElement inn = new XElement("inn", FirmInfo[1]);
                XElement kpp = new XElement("kpp", FirmInfo[2]);

                organization.Add(name);
                organization.Add(inn);
                organization.Add(kpp);

                //---------russianAddress----------------
                XElement city = new XElement("city", Convert.ToString(FirmAdr[1]));
                XElement street = new XElement("street", Convert.ToString(FirmAdr[0]));
                XElement regionISOCode = new XElement("regionISOCode", sellerISOCode);
                XElement postalCode = new XElement("postalCode", Convert.ToString(FirmAdr[3]));

                russianAddress.Add(city);
                russianAddress.Add(street);
                russianAddress.Add(regionISOCode);
                russianAddress.Add(postalCode);
            }


            //--------buyer------------------
            XElement glnbuyer = new XElement("gln", KonturPlatGLN);           //глн группы либо протсо глн (Convert.ToString(PlatInfo[2]))
            buyer.Add(glnbuyer);
            string BuyerISOCode;

            XElement organizationbuyer = new XElement("organization");
            XElement russianAddressbuyer = new XElement("russianAddress");

            buyer.Add(organizationbuyer);
            buyer.Add(russianAddressbuyer);

            //--------organization-buyer------------------
            XElement namebuyer = new XElement("name", Convert.ToString(PlatInfo[1]));
            XElement innbuyer = new XElement("inn", Convert.ToString(PlatInfo[3]));
            XElement kppbuyer = new XElement("kpp", Convert.ToString(PlatInfo[4]));

            organizationbuyer.Add(namebuyer);
            organizationbuyer.Add(innbuyer);
            organizationbuyer.Add(kppbuyer);

            //-------russianAddress-buyer---------------------------
            BuyerISOCode = DispOrders.GetISOCode(Convert.ToString(PlatInfo[0]));

            string S_streetbuyer = Convert.ToString(PlatInfo[6]);
            int indexOfChar = S_streetbuyer.IndexOf(',');
            S_streetbuyer = S_streetbuyer.Substring(indexOfChar + 1);
            indexOfChar = S_streetbuyer.IndexOf(',');
            S_streetbuyer = S_streetbuyer.Substring(indexOfChar + 1);

            //---------russianAddress----------------
            //XElement citybuyer = new XElement("city", Convert.ToString(PlatInfo[1]));
            XElement streetbuyer = new XElement("street", S_streetbuyer);
            XElement regionISOCodebuyer = new XElement("regionISOCode", BuyerISOCode);
            XElement postalCodebuyer = new XElement("postalCode", Convert.ToString(PlatInfo[7]));

            //russianAddressbuyer.Add(citybuyer);
            russianAddressbuyer.Add(streetbuyer);
            russianAddressbuyer.Add(regionISOCodebuyer);
            russianAddressbuyer.Add(postalCodebuyer);

            //--------invoicee------------------
            /*
            XElement glninvoicee = new XElement("gln", Convert.ToString(PlatInfo[2]));
            invoicee.Add(glninvoicee);

            
            XElement organizationinvoicee = new XElement("organization");
            XElement russianAddressinvoicee = new XElement("russianAddress");

            invoicee.Add(organizationinvoicee);
            invoicee.Add(russianAddressinvoicee);

            //--------organization-invoicee------------------
            XElement nameinvoicee = new XElement("name", Convert.ToString(PlatInfo[1]));
            XElement inninvoicee = new XElement("inn", Convert.ToString(PlatInfo[3]));
            XElement kppinvoicee = new XElement("kpp", Convert.ToString(PlatInfo[4]));

            organizationinvoicee.Add(nameinvoicee);
            organizationinvoicee.Add(inninvoicee);
            organizationinvoicee.Add(kppinvoicee);

            //-------russianAddress-invoicee---------------------------
            BuyerISOCode = DispOrders.GetISOCode(Convert.ToString(PlatInfo[0]));
            XElement regionISOCodeinvoicee = new XElement("regionISOCode", BuyerISOCode);
            russianAddressinvoicee.Add(regionISOCodeinvoicee);
            */

            //---------deliveryInfo------------------------------------
            XElement shipFrom = new XElement("shipFrom");
            XElement shipTo = new XElement("shipTo");

            deliveryInfo.Add(shipFrom);
            deliveryInfo.Add(shipTo);

            //---------shipFrom----------------------
            if (UseMasterGLN == true)
            {
                XElement glnFrom = new XElement("gln", ILN_Edi);
                shipFrom.Add(glnFrom);
            }
            else
            {
                XElement glnFrom = new XElement("gln", ILN_Edi_S);
                shipFrom.Add(glnFrom);
            }

            XElement organizationFrom = new XElement("organization");
            XElement russianAddressFrom = new XElement("russianAddress");

            shipFrom.Add(organizationFrom);
            shipFrom.Add(russianAddressFrom);

            //--------organization------------------
            XElement nameFrom = new XElement("name", FirmInfo[0]);
            XElement innFrom = new XElement("inn", FirmInfo[1]);
            XElement kppFrom = new XElement("kpp", FirmInfo[2]);

            organizationFrom.Add(nameFrom);
            organizationFrom.Add(innFrom);
            organizationFrom.Add(kppFrom);

            //---------russianAddress----------------
            XElement cityFrom = new XElement("city", Convert.ToString(FirmAdr[1]));
            XElement streetFrom = new XElement("street", Convert.ToString(FirmAdr[0]));
            XElement regionISOCodeFrom = new XElement("regionISOCode", sellerISOCode);
            XElement postalCodeFrom = new XElement("postalCode", Convert.ToString(FirmAdr[3]));

            russianAddressFrom.Add(cityFrom);
            russianAddressFrom.Add(streetFrom);
            russianAddressFrom.Add(regionISOCodeFrom);
            russianAddressFrom.Add(postalCodeFrom);

            //---------ShipTo-------------------------
            XElement glnTo = new XElement("gln", Convert.ToString(DelivInfo[2]));
            shipTo.Add(glnTo);

            if (iso == true)//костыли для азбуки вкуса
            {
                XElement organizationTo = new XElement("organization");
                XElement russianAddressTo = new XElement("russianAddress");

                shipTo.Add(organizationTo);
                shipTo.Add(russianAddressTo);

                //--------organization------------------
                XElement nameTo = new XElement("name", Convert.ToString(PlatInfo[1]));
                XElement innTo = new XElement("inn", Convert.ToString(PlatInfo[3]));
                XElement kppTo = new XElement("kpp", Convert.ToString(PlatInfo[4]));

                organizationTo.Add(nameTo);
                organizationTo.Add(innTo);
                organizationTo.Add(kppTo);

                //---------russianAddress----------------
                string DelivISOCode = DispOrders.GetISOCode(Convert.ToString(DelivInfo[0]));
                XElement regionISOCodeTo = new XElement("regionISOCode", DelivISOCode);
                russianAddressTo.Add(regionISOCodeTo);
            }

            Program.WriteLine("запрос данных спецификации " + Convert.ToString(CurrDataCOInvoice[3]) + "   " + Convert.ToString(CurrDataCOInvoice[5]));

            //-----------lineItems--------------------
            XElement currencyISOCode = new XElement("currencyISOCode", "RUB");

            //без ндс
            String S_totalSumExcludingTaxesDecrease = "";
            String S_totalSumExcludingTaxesIncrease = "";
            //ндс
            String S_totalVATAmountDecrease = "";
            String S_totalVATAmountIncrease = "";
            //c ндс
            String S_totalAmountDecrease = "";
            String S_totalAmountIncrease = "";

            if (Convert.ToDecimal(Total[5]) >= 0)
            {
                S_totalSumExcludingTaxesDecrease = "0"; //уменьшение
                S_totalSumExcludingTaxesIncrease = Convert.ToString(Convert.ToDecimal(Total[5])); //увеличение
                S_totalVATAmountDecrease = "0"; //уменьшение
                S_totalVATAmountIncrease = Convert.ToString(Convert.ToDecimal(Total[4]) - Convert.ToDecimal(Total[5]));//увеличение
                S_totalAmountDecrease = "0"; //уменьшение
                S_totalAmountIncrease = Convert.ToString(Convert.ToDecimal(Total[4]));//увеличение
            }
            else
            {
                S_totalSumExcludingTaxesDecrease = Convert.ToString(Convert.ToDecimal(Total[5])).Substring(1);//убираем минус
                S_totalSumExcludingTaxesDecrease = S_totalSumExcludingTaxesDecrease.Replace(",", ".");
                S_totalSumExcludingTaxesIncrease = "0";
                S_totalVATAmountDecrease = Convert.ToString((-1) * (Convert.ToDecimal(Total[4]) - Convert.ToDecimal(Total[5])));//убираем минус
                S_totalVATAmountDecrease = S_totalVATAmountDecrease.Replace(",", ".");
                S_totalVATAmountIncrease = "0";
                S_totalAmountDecrease = Convert.ToString(Convert.ToDecimal(Total[4])).Substring(1);//убираем минус
                S_totalAmountDecrease = S_totalAmountDecrease.Replace(",", ".");
                S_totalAmountIncrease = "0";
            }

            XElement totalSumExcludingTaxesDecrease = new XElement("totalSumExcludingTaxesDecrease", S_totalSumExcludingTaxesDecrease); //уменьшение
            XElement totalSumExcludingTaxesIncrease = new XElement("totalSumExcludingTaxesIncrease", S_totalSumExcludingTaxesIncrease); //увеличение
            XElement totalVATAmountDecrease = new XElement("totalVATAmountDecrease", S_totalVATAmountDecrease); //уменьшение
            XElement totalVATAmountIncrease = new XElement("totalVATAmountIncrease", S_totalVATAmountIncrease);//увеличение
            XElement totalAmountDecrease = new XElement("totalAmountDecrease", S_totalAmountDecrease); //уменьшение
            XElement totalAmountIncrease = new XElement("totalAmountIncrease", S_totalAmountIncrease);//увеличение

            Program.WriteLine(S_totalSumExcludingTaxesDecrease + " " + S_totalSumExcludingTaxesIncrease + " " + S_totalVATAmountDecrease + " " + S_totalVATAmountIncrease);

            String S_quantityIncrease = "";
            String S_quantityDecrease = "";
            String S_netAmountIncrease = "";
            String S_netAmountDecrease = "";
            String S_vatAmountIncrease = "";
            String S_vatAmountDecrease = "";
            String S_amountIncrease = "";
            String S_amountDecrease = "";

            lineItems.Add(currencyISOCode);

            Program.WriteLine("Количество позиций " + CntLinesInvoice);

            //----------lineItem--------------------------
            string EAN_F = "";
            for (int i = 0; i < CntLinesInvoice; i++)
            {
                Program.WriteLine("Блок1");
                XElement LineItem = new XElement("lineItem");

                lineItems.Add(LineItem);
                Program.WriteLine(Convert.ToString(CurrDataCOInvoice[5]) + " " + Convert.ToString(Item[i, 2]) + " " + Convert.ToString(PCE));
                Program.WriteLine(Convert.ToString(PlatInfo[5]) + " " + Convert.ToString(Item[i, 1]));
                try
                {

                    object[,] prevItem = DispOrders.GetItemFromInvoice(Convert.ToString(CurrDataCOInvoice[5]), Convert.ToString(Item[i, 2]), PCE); //Позиция до изменения 

                    Program.WriteLine(Convert.ToString(prevItem[0, 4]));

                    object[] BICode = Verifiacation.GetBuyerItemCode(Convert.ToString(PlatInfo[5]), Convert.ToString(Item[i, 1]));

                    Program.WriteLine(Convert.ToString(BICode[0]));

                    if (Convert.ToString(DelivInfo[10]) == "MDOU") // для садиков надо мнемокод
                    {
                        BICode[0] = Verifiacation.GetMnemoCode(Convert.ToString(Item[i, 0]), Convert.ToString(PlatInfo[8])); // это для садиков, мнемокод запихан сюда. берём его, если нет другого артикула покупателя. чтобы не испортить.
                    }

                    Program.WriteLine("Блок2");
                    XElement orderLineNumber = new XElement("orderLineNumber", i + 1);
                    EAN_F = Convert.ToString(Item[i, 0]).Substring(0, 13);  //Обрезаем штрих-код до 13 символов
                    //XElement gtin = new XElement("gtin", Item[i, 0]);
                    XElement gtin = new XElement("gtin", EAN_F);
                    XElement internalSupplierCode = new XElement("internalSupplierCode", Item[i, 2]);
                    XElement internalBuyerCode = new XElement("internalBuyerCode", BICode[0]);
                    XElement description = new XElement("description", Item[i, 3]);
                    XElement quantityBefore = new XElement("quantityBefore", prevItem[0, 4]);
                    Program.WriteLine("internalBuyerCode" + " " + BICode[0] + " quantityAfter " + (Convert.ToDecimal(prevItem[0, 4]) + Convert.ToDecimal(Item[i, 4])));
                    XElement quantityAfter = new XElement("quantityAfter", (Convert.ToDecimal(prevItem[0, 4]) + Convert.ToDecimal(Item[i, 4])));

                    XElement quantityIncrease;
                    XElement quantityDecrease;
                    XAttribute unitOfMeasure;
                    XElement netPriceBefore;
                    XElement netPriceAfter;
                    XElement netPriceIncrease;
                    XElement netPriceDecrease;
                    XElement netPriceWithVAT;

                    if (Convert.ToString(DelivInfo[10]) == "MDOU")
                    {
                        if (Convert.ToDecimal(Item[i, 14]) >= 0)
                        {
                            S_quantityDecrease = "0"; //уменьшение
                            S_quantityIncrease = Convert.ToString(Item[i, 14]); //увеличение                        
                        }
                        else
                        {
                            S_quantityDecrease = Convert.ToString(Item[i, 14]).Substring(1);//убираем минус
                            S_quantityIncrease = "0";
                        }
                        quantityIncrease = new XElement("quantityIncrease", S_quantityIncrease);
                        quantityDecrease = new XElement("quantityDecrease", S_quantityDecrease);
                        unitOfMeasure = new XAttribute("unitOfMeasure", "KGM");  // им ВСЁ надо в КГ                    
                        netPriceBefore = new XElement("netPriceBefore", (Convert.ToDecimal(Item[i, 12]) / Convert.ToDecimal(Item[i, 14])));
                        netPriceAfter = new XElement("netPriceAfter", (Convert.ToDecimal(Item[i, 12]) / Convert.ToDecimal(Item[i, 14])));
                        netPriceWithVAT = new XElement("netPriceWithVAT", ((Convert.ToDecimal(Item[i, 11]) + Convert.ToDecimal(Item[i, 12])) / Convert.ToDecimal(Item[i, 14])));
                        Program.WriteLine("unitOfMeasure" + " " + "KGM");
                    }
                    else
                    {
                        Program.WriteLine("Блок3");
                        if (Convert.ToDecimal(Item[i, 4]) >= 0)
                        {
                            S_quantityDecrease = "0"; //уменьшение
                            S_quantityIncrease = Convert.ToString(Item[i, 4]); //увеличение 
                        }
                        else
                        {
                            S_quantityDecrease = Convert.ToString(Item[i, 4]).Substring(1);//убираем минус
                            S_quantityIncrease = "0";
                        }
                        quantityIncrease = new XElement("quantityIncrease", S_quantityIncrease);
                        quantityDecrease = new XElement("quantityDecrease", S_quantityDecrease);
                        unitOfMeasure = new XAttribute("unitOfMeasure", Item[i, 7]);
                        netPriceBefore = new XElement("netPriceBefore", Item[i, 5]);
                        netPriceAfter = new XElement("netPriceAfter", Item[i, 5]);
                        netPriceWithVAT = new XElement("netPriceWithVAT", Item[i, 6]);
                        Program.WriteLine("unitOfMeasure" + " " + Item[i, 7]);
                    }
                    Program.WriteLine("Блок4");
                    XElement netAmountBefore = new XElement("netAmountBefore", prevItem[0, 12]);
                    XElement netAmountAfter = new XElement("netAmountAfter", (Convert.ToDecimal(prevItem[0, 12]) + Convert.ToDecimal(Item[i, 12])));

                    if (Convert.ToDecimal(Item[i, 12]) >= 0)
                    {
                        S_netAmountDecrease = "0"; //уменьшение
                        S_netAmountIncrease = Convert.ToString(Item[i, 12]); //увеличение 
                        S_netAmountIncrease = S_netAmountIncrease.Replace(",", ".");
                        S_vatAmountDecrease = "0"; //уменьшение
                        S_vatAmountIncrease = Convert.ToString(Item[i, 11]); //увеличение
                        S_vatAmountIncrease = S_vatAmountIncrease.Replace(",", ".");
                        S_amountDecrease = "0"; //уменьшение
                        S_amountIncrease = Convert.ToString(Convert.ToDecimal(Item[i, 11]) + Convert.ToDecimal(Item[i, 12])); //увеличение
                        S_amountIncrease = S_amountIncrease.Replace(",", ".");
                    }
                    else
                    {
                        S_netAmountDecrease = Convert.ToString(Item[i, 12]).Substring(1);//убираем минус
                        S_netAmountDecrease = S_netAmountDecrease.Replace(",", ".");
                        S_netAmountIncrease = "0";
                        S_vatAmountDecrease = Convert.ToString(Item[i, 11]).Substring(1);//убираем минус
                        S_vatAmountDecrease = S_vatAmountDecrease.Replace(",", ".");
                        S_vatAmountIncrease = "0";
                        S_amountDecrease = Convert.ToString(Convert.ToDecimal(Item[i, 11]) + Convert.ToDecimal(Item[i, 12])).Substring(1);//убираем минус
                        S_amountDecrease = S_amountDecrease.Replace(",", ".");
                        S_amountIncrease = "0";
                    }
                    Program.WriteLine("Блок5");
                    XElement netAmountIncrease = new XElement("netAmountIncrease", S_netAmountIncrease);
                    XElement netAmountDecrease = new XElement("netAmountDecrease", S_netAmountDecrease);

                    XElement vatRateBefore = new XElement("vatRateBefore", Convert.ToInt32(Item[i, 9]));
                    XElement vatRateAfter = new XElement("vatRateAfter", Convert.ToInt32(Item[i, 9]));

                    XElement vatAmountBefore = new XElement("vatAmountBefore", prevItem[0, 11]);
                    XElement vatAmountAfter = new XElement("vatAmountAfter", (Convert.ToDecimal(prevItem[0, 11]) + Convert.ToDecimal(Item[i, 11])));

                    XElement vatAmountIncrease = new XElement("vatAmountIncrease", S_vatAmountIncrease);
                    XElement vatAmountDecrease = new XElement("vatAmountDecrease", S_vatAmountDecrease);

                    XElement amountBefore = new XElement("amountBefore", (Convert.ToDecimal(prevItem[0, 11]) + Convert.ToDecimal(prevItem[0, 12])));
                    XElement amountAfter = new XElement("amountAfter", (Convert.ToDecimal(prevItem[0, 11]) + Convert.ToDecimal(prevItem[0, 12]) + Convert.ToDecimal(Item[i, 11]) + Convert.ToDecimal(Item[i, 12])));

                    XElement amountIncrease = new XElement("amountIncrease", S_amountIncrease);
                    XElement amountDecrease = new XElement("amountDecrease", S_amountDecrease);

                    Program.WriteLine(S_netAmountIncrease + " " + S_netAmountDecrease);

                    Program.WriteLine("Блок6");
                    LineItem.Add(gtin);
                    LineItem.Add(internalBuyerCode);
                    LineItem.Add(internalSupplierCode);
                    LineItem.Add(orderLineNumber);
                    LineItem.Add(description);
                    LineItem.Add(quantityBefore);
                    LineItem.Add(quantityAfter);
                    LineItem.Add(quantityIncrease);
                    LineItem.Add(quantityDecrease);

                    quantityBefore.Add(unitOfMeasure);
                    quantityAfter.Add(unitOfMeasure);
                    quantityIncrease.Add(unitOfMeasure);
                    quantityDecrease.Add(unitOfMeasure);

                    LineItem.Add(netPriceBefore);
                    LineItem.Add(netPriceAfter);
                    LineItem.Add(netPriceWithVAT);
                    LineItem.Add(netAmountBefore);  //Стоимость без НДС до
                    LineItem.Add(netAmountAfter);   //Стоимость без НДС после 
                    LineItem.Add(netAmountIncrease);
                    LineItem.Add(netAmountDecrease);

                    LineItem.Add(vatRateBefore);
                    LineItem.Add(vatRateAfter);
                    LineItem.Add(vatAmountBefore);  //Сумма НДС до
                    LineItem.Add(vatAmountAfter);   //Сумма НДС после
                    LineItem.Add(vatAmountIncrease);
                    LineItem.Add(vatAmountDecrease);
                    LineItem.Add(amountBefore);     //Стоимость с НДС до
                    LineItem.Add(amountAfter);      //Стоимость с НДС после
                    LineItem.Add(amountIncrease);
                    LineItem.Add(amountDecrease);

                    if (Convert.ToString(DelivInfo[10]) == "BaseMark")
                    {
                        int quantityItemBefore = Convert.ToInt32(prevItem[0, 4]);
                        int quantityItemAfter = Convert.ToInt32(prevItem[0, 4]) + Convert.ToInt32(Item[i, 4]);
                        XElement controlIdentificationMarksBefore;
                        XElement controlIdentificationMarksAfter;
                        XAttribute controlIdentificationMarksBefore_type;
                        XAttribute controlIdentificationMarksAfter_type;
                        controlIdentificationMarksBefore = new XElement("controlIdentificationMarksBefore", "020" + EAN_F + "37" + Convert.ToString(quantityItemBefore));
                        controlIdentificationMarksBefore_type = new XAttribute("type", "Group");
                        controlIdentificationMarksAfter = new XElement("controlIdentificationMarksAfter", "020" + EAN_F + "37" + Convert.ToString(quantityItemAfter));
                        controlIdentificationMarksAfter_type = new XAttribute("type", "Group");
                        LineItem.Add(controlIdentificationMarksBefore);
                        controlIdentificationMarksBefore.Add(controlIdentificationMarksBefore_type);
                        LineItem.Add(controlIdentificationMarksAfter);
                        controlIdentificationMarksAfter.Add(controlIdentificationMarksAfter_type);
                    }

                    if (Convert.ToString(DelivInfo[10]) == "MDOU")
                    {
                        XElement comment = new XElement("comment", BICode[0]);
                        LineItem.Add(comment);
                    }

                }
                catch (Exception e)
                {
                    string error = "Ошибка в процедуре ";
                    Program.WriteLine(error);
                }
            }
            Program.WriteLine("Блок7");
            lineItems.Add(totalSumExcludingTaxesDecrease);
            lineItems.Add(totalSumExcludingTaxesIncrease);
            lineItems.Add(totalVATAmountDecrease);
            lineItems.Add(totalVATAmountIncrease);
            lineItems.Add(totalAmountDecrease);
            lineItems.Add(totalAmountIncrease);

            Program.WriteLine("Сохранение кор. счет-фактуры");

            //------сохранение документа-----------
            try
            {
                xdoc.Save(InvoiceKONTUR + nameInv);
                xdoc.Save(ArchiveKONTUR + nameInv);
                string message = "СКБ-Контур. Кор. счет-Фактура " + nameInv + " создана в " + InvoiceKONTUR;
                Program.WriteLine(message);
                DispOrders.WriteInvoiceLog(Convert.ToString(PlatInfo[0]) + " - " + Convert.ToString(PlatInfo[1]), Convert.ToString(DelivInfo[0]) + " - " + Convert.ToString(DelivInfo[1]), nameInv, Convert.ToString(CurrDataCOInvoice[6]), 0, message, DateTime.Now);
                DispOrders.WriteProtocolEDI("Кор. счет фактура", nameInv, Convert.ToString(PlatInfo[0]) + " - " + Convert.ToString(PlatInfo[1]), 0, DelivInfo[0] + " - " + DelivInfo[1], "Кор. счет фактура сформирована", DateTime.Now, Convert.ToString(CurrDataCOInvoice[6]), "KONTUR");

                //запись в лог отправки  СФ
                int CorSf = Convert.ToInt32(CurrDataCOInvoice[4]);
                string Doc;
                if (CorSf == 0)//СФ
                {
                    Doc = "5";
                }
                else//КСФ
                {
                    Doc = "9";
                }
                Program.WriteLine("Записываем в журнал отправленных " + Doc + ", " + nameInv + ", " + Convert.ToString(CurrDataCOInvoice[3]) + ", " + Convert.ToString(infoSf[0]) + ", " + Convert.ToString(Total[4]) + ", " + Convert.ToString(CurrDataCOInvoice[6]));
                DispOrders.WriteEDiSentDoc(Doc, nameInv, Convert.ToString(CurrDataCOInvoice[3]), Convert.ToString(infoSf[0]), "123", Convert.ToString(Total[4]), Convert.ToString(CurrDataCOInvoice[7]),1);
                Program.WriteLine("Блок13");
                ReportEDI.RecordCountEDoc("СКБ-Контур", "Invoice", 1);
            }
            catch
            {
                string message_error = "СКБ-Контур. Не могу создать xml файл Кор. счет-Фактуры в " + InvoiceKONTUR + ". Нет доступа или диск переполнен.";
                DispOrders.WriteInvoiceLog(Convert.ToString(PlatInfo[0]) + " - " + Convert.ToString(PlatInfo[1]), Convert.ToString(DelivInfo[0]) + " - " + Convert.ToString(DelivInfo[1]), nameInv, Convert.ToString(CurrDataCOInvoice[6]), 10, message_error, DateTime.Now);
                DispOrders.WriteProtocolEDI("Кор. счет фактура", nameInv, Convert.ToString(PlatInfo[0]) + " - " + Convert.ToString(PlatInfo[1]), 10, DelivInfo[0] + " - " + DelivInfo[1], "Кор. счет фактура не сформирована. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataCOInvoice[6]), "KONTUR");
                Program.WriteLine(message_error);
                //запись в лог о неудаче
            }
        }

        public static void CreateKonturInvoiceDOP(List<object> CurrDataInvoice)
        {
            if (Convert.ToInt32(CurrDataInvoice[5]) == 0) //КСФ исключаем
            {
                //признак необходимости ISOCode
                bool iso = false;

                //получение путей
                string ArchiveKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.АРХИВ");

                //генерация имени файла.
                string id; //= Convert.ToString(DateTime.Now); ;   // ну это так, ищу замену    
                string id2 = Convert.ToString(Guid.NewGuid()); ;       // иногда генерит повторно тот же ИД подряд !!!!!!!!!   и как-то по утрам, потом нормально работает ... 
                id = Convert.ToString(Guid.NewGuid()); ;       // иногда генерит повторно тот же ИД подряд !!!!!!!!!   и как-то по утрам, потом нормально работает ... 
                if (id == id2)                                  // ну или другая фигня. не понятно. такие файлики уже есть...
                    id = id + "_";
                string nameInv = "INVOIC_" + id + ".xml";

                //Запрос данных Заказа
                object[] infoPrdZkg = Verifiacation.GetPrdZkg(Convert.ToString(CurrDataInvoice[7])); //0 PrdZkg_nmr, 1 PrdZkg_Dt, 2 PrdZkg_DtOtg, 3 PrdZkg_NmrExt
                //Запрос данных СФ
                object[] infoSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataInvoice[3])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO
                //Запрос данных покупателя
                object[] infoKag = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(infoSf[2]), Convert.ToInt64(infoSf[3])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование
                //Запрос данных грузополучателя
                //object[] infoGpl = Verifiacation.GetDataFromPtnRCD(Convert.ToInt64(infoSf[4]), Convert.ToInt64(infoSf[5])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование

                bool UseMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(infoSf[4]));

                //получение данных
                object[] DelivInfo = Verifiacation.GetDataFromPtn_Rcd(Convert.ToString(infoSf[4]));   //10 = формат
                object[] PlatInfo = Verifiacation.GetDataFromPtn_Rcd(Convert.ToString(infoSf[2]));
                /*
                //какой gln номер использовать
                string check = Convert.ToString(DelivInfo[8]);
                bool UseMasterGLN = false;
                if (check.Length != 0)
                {
                    UseMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(DelivInfo[8]));
                }*/
                //bool UseMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(DelivInfo[8]));
                string ILN_Edi, ILN_Edi_S;
                string InvoiceKONTUR;
                object[] FirmInfo, FirmInfo_G;
                object[] FirmAdr, FirmAdr_G;
                if (UseMasterGLN == true) //используем данные головного предприятия, для Агроторгов и Метро в любом случае
                {
                    ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");

                    FirmInfo = Verifiacation.GetMasterFirmInfo();
                    FirmAdr = Verifiacation.GetMasterFirmAdr();
                    try
                    {
                        InvoiceKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.ЭКСПОРТ");
                    }
                    catch
                    {
                        InvoiceKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.СФ");
                    }
                }

                else//используем данные текущего предприятия
                {
                    //ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ИЛН");
                    ILN_Edi = DispOrders.GetValueOption("СКБ-КОНТУР.ИЛН_ПРЕДПРИЯТИЯ");
                    if (ILN_Edi == "") ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ИЛН");
                    InvoiceKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.СФ");
                    if (Convert.ToDateTime(infoKag[13]) > Convert.ToDateTime(infoSf[1]))      // 13 это дата с которой надо ставить новые данные     
                    {
                        FirmInfo = Verifiacation.GetFirmInfo("20171130"); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO        // берём как до 01.12.2017
                    }
                    else
                    {
                        FirmInfo = Verifiacation.GetFirmInfo(Convert.ToDateTime(infoSf[1]).ToString("yyyyMMdd"));
                    }
                    FirmAdr = Verifiacation.GetFirmAdr();
                }

                ILN_Edi_S = DispOrders.GetValueOption("СКБ-КОНТУР.ИЛН_ПРЕДПРИЯТИЯ");
                FirmInfo_G = Verifiacation.GetMasterFirmInfo();
                FirmAdr_G = Verifiacation.GetMasterFirmAdr();

                //костыли для азбуки вкуса
                if (Convert.ToString(DelivInfo[0]).Substring(0, 2) == "41")
                {
                    iso = true;
                }
                else
                {
                    iso = false;
                }

                string sellerISOCode;//костыли для азбуки вкуса
                if (ILN_Edi == "4607008059991")
                {
                    sellerISOCode = "RU-CHE";
                }
                else
                {
                    sellerISOCode = "RU-SVE";
                }

                if (File.Exists(InvoiceKONTUR + nameInv))          // уже есть такие файлы??? почему-то не может сохранить
                    nameInv = nameInv = "INVOIC_" + id + "_.xml"; ;

                int CntLinesInvoice = Verifiacation.CountItemsInInvoice(Convert.ToString(CurrDataInvoice[3]));

                bool PCE = Verifiacation.UsePCE(Convert.ToString(PlatInfo[0]));//проверка на использование штук в исходящих докуметов  

                object[,] Item = DispOrders.GetItemsFromInvoice(Convert.ToString(CurrDataInvoice[3]), CntLinesInvoice, PCE);

                object[] Total = DispOrders.GetTotal(Convert.ToString(CurrDataInvoice[3]), 5);

                //object[] SignerInfo = Verifiacation.GetSigner();

                XDocument xdoc = new XDocument();

                //основные элементы (1 уровень)
                XElement eDIMessage = new XElement("eDIMessage");
                XElement interchangeHeader = new XElement("interchangeHeader");
                XElement invoice = new XElement("invoice");
                XAttribute idMessage = new XAttribute("id", id);
                XAttribute creationDateTime = new XAttribute("creationDateTime", (DateTime.Now).ToString("yyyy-MM-dd HH:mm:ss"));
                xdoc.Add(eDIMessage);
                eDIMessage.Add(interchangeHeader);
                eDIMessage.Add(invoice);
                eDIMessage.Add(idMessage);
                eDIMessage.Add(creationDateTime);

                //Определяем это оригинальный документ или испрвление
                DateTime dateToday = new DateTime();
                dateToday = DateTime.Today;
                if (Convert.ToDateTime(infoSf[1]) < dateToday)
                {
                    Program.WriteLine("Исправительная УПД");
                    XAttribute numberInvoice = new XAttribute("number", infoSf[0].ToString());
                    XAttribute dateInvoice = new XAttribute("date", (Convert.ToDateTime(infoSf[1])).ToString("yyyy-MM-dd"));
                    XAttribute revisionNumberInvoice = new XAttribute("revisionNumber", "1");
                    XAttribute revisionDateInvoice = new XAttribute("revisionDate", dateToday.ToString("yyyy-MM-dd"));
                    XAttribute TypeInvoice = new XAttribute("type", "Replace");
                    invoice.Add(numberInvoice);
                    invoice.Add(dateInvoice);
                    invoice.Add(revisionNumberInvoice);
                    invoice.Add(revisionDateInvoice);
                    invoice.Add(TypeInvoice);
                }
                else
                {
                    Program.WriteLine("Оригинальная УПД");
                    XAttribute numberInvoice = new XAttribute("number", infoSf[0].ToString());
                    XAttribute dateInvoice = new XAttribute("date", (Convert.ToDateTime(infoSf[1])).ToString("yyyy-MM-dd"));
                    XAttribute TypeInvoice = new XAttribute("type", "Original");
                    invoice.Add(numberInvoice);
                    invoice.Add(dateInvoice);
                    invoice.Add(TypeInvoice);
                }

                //------interchangeHeader---------------
                XElement sender = new XElement("sender", ILN_Edi);
                XElement recipient = new XElement("recipient", PlatInfo[2]);
                XElement documentType = new XElement("documentType", "INVOIC");

                interchangeHeader.Add(sender);
                interchangeHeader.Add(recipient);
                interchangeHeader.Add(documentType);

                //------invoice------------------------
                XElement originOrder = new XElement("originOrder");
                invoice.Add(originOrder);
                XAttribute numberoriginOrder = new XAttribute("number", CurrDataInvoice[6]);
                XAttribute dateoriginOrder;

                //дату заказа берем из даты документа заказа, если вывалилась ошибка, значит заказ был сделан вручную, дату заказа проставляем как в в заказе ИСПРО
                try
                {
                    Program.WriteLine("дата документа заказа " + Convert.ToDateTime(Verifiacation.GetFldFromEdiExch(Convert.ToInt64(CurrDataInvoice[7]), "Exch_OrdDocDat")).ToString("yyyy-MM-dd"));
                    //dateoriginOrder = new XAttribute("date", Convert.ToDateTime(CurrDataInvoice[6]).ToString("yyyy-MM-dd"));
                    dateoriginOrder = new XAttribute("date", Convert.ToDateTime(Verifiacation.GetFldFromEdiExch(Convert.ToInt64(CurrDataInvoice[7]), "Exch_OrdDocDat")).ToString("yyyy-MM-dd"));
                }
                catch
                {
                    dateoriginOrder = new XAttribute("date", Convert.ToDateTime(CurrDataInvoice[8]).ToString("yyyy-MM-dd"));
                }

                XElement despatchIdentificator = new XElement("despatchIdentificator");
                invoice.Add(despatchIdentificator);
                XAttribute numberdespatchIdentificator = new XAttribute("number", Verifiacation.GetSklnkNumber(Convert.ToInt32(CurrDataInvoice[7])));
                XAttribute datedespatchIdentificator;
                datedespatchIdentificator = new XAttribute("date", Convert.ToDateTime(infoSf[1]).ToString("yyyy-MM-dd"));

                object[] infoRecAdv = Verifiacation.GetRecAdvInfo(Convert.ToInt64(CurrDataInvoice[7]));

                if (infoRecAdv[0] != null && infoRecAdv[0] != "")  // это теперь есть во вьюхе, CurrDataInvoice[21],22
                {
                    XElement receivingIdentificator = new XElement("receivingIdentificator");
                    XAttribute numberreceivingIdentificator = new XAttribute("number", infoRecAdv[0]);
                    XAttribute datereceivingIdentificator;
                    datereceivingIdentificator = new XAttribute("date", Convert.ToDateTime(infoRecAdv[1]).ToString("yyyy-MM-dd"));

                    invoice.Add(receivingIdentificator);
                    receivingIdentificator.Add(numberreceivingIdentificator);
                    receivingIdentificator.Add(datereceivingIdentificator);
                }

                // contractIdentificator = номер контракта и дата при наличии
                object[] ContractInfoInfo = Verifiacation.GetContractInfo(Convert.ToString(DelivInfo[0])); //контракты
                if (ContractInfoInfo[0] != null && Convert.ToString(ContractInfoInfo[0]) != "нет данных")
                {
                    XElement contractIdentificator = new XElement("contractIdentificator");
                    XAttribute numbercontractIdentificator = new XAttribute("number", ContractInfoInfo[0]);
                    XAttribute DatecontractIdentificator = new XAttribute("date", Convert.ToDateTime(ContractInfoInfo[1]).ToString("yyyy-MM-dd"));
                    invoice.Add(contractIdentificator);
                    contractIdentificator.Add(numbercontractIdentificator);
                    contractIdentificator.Add(DatecontractIdentificator);
                }

                XElement seller = new XElement("seller");
                XElement buyer = new XElement("buyer");
                XElement invoicee = new XElement("invoicee");
                XElement deliveryInfo = new XElement("deliveryInfo");
                XElement lineItems = new XElement("lineItems");

                invoice.Add(seller);
                invoice.Add(buyer);
                invoice.Add(invoicee);
                invoice.Add(deliveryInfo);
                invoice.Add(lineItems);

                originOrder.Add(numberoriginOrder);
                originOrder.Add(dateoriginOrder);

                despatchIdentificator.Add(numberdespatchIdentificator);
                despatchIdentificator.Add(datedespatchIdentificator);

                //--------seller-----------------------
                XElement gln = new XElement("gln", ILN_Edi);
                seller.Add(gln);

                if (iso == true)//костыли для азбуки вкуса
                {
                    XElement organization = new XElement("organization");
                    XElement russianAddress = new XElement("russianAddress");

                    seller.Add(organization);
                    seller.Add(russianAddress);
                    //--------organization------------------
                    XElement name = new XElement("name", FirmInfo[0]);
                    XElement inn = new XElement("inn", FirmInfo[1]);
                    XElement kpp = new XElement("kpp", FirmInfo[2]);

                    organization.Add(name);
                    organization.Add(inn);
                    organization.Add(kpp);

                    //---------russianAddress----------------
                    XElement city = new XElement("city", Convert.ToString(FirmAdr[1]));
                    XElement street = new XElement("street", Convert.ToString(FirmAdr[0]));
                    XElement regionISOCode = new XElement("regionISOCode", sellerISOCode);
                    XElement postalCode = new XElement("postalCode", Convert.ToString(FirmAdr[3]));

                    russianAddress.Add(city);
                    russianAddress.Add(street);
                    russianAddress.Add(regionISOCode);
                    russianAddress.Add(postalCode);
                }
                else
                {
                    if (UseMasterGLN == false)
                    {
                        XElement organization = new XElement("organization");
                        XElement russianAddress = new XElement("russianAddress");

                        seller.Add(organization);
                        seller.Add(russianAddress);
                        //--------organization------------------
                        XElement name = new XElement("name", "АО \"Группа Компаний \"Российское Молоко\" (АО \"ГК \"РОСМОЛ\")");
                        XElement inn = new XElement("inn", FirmInfo_G[1]);
                        XElement kpp = new XElement("kpp", FirmInfo[2]);

                        organization.Add(name);
                        organization.Add(inn);
                        organization.Add(kpp);

                        //---------russianAddress----------------
                        sellerISOCode = "RU-CHE";
                        XElement city = new XElement("city", Convert.ToString(FirmAdr_G[1]));
                        XElement street = new XElement("street", Convert.ToString(FirmAdr_G[0]));
                        XElement regionISOCode = new XElement("regionISOCode", sellerISOCode);
                        XElement postalCode = new XElement("postalCode", Convert.ToString(FirmAdr_G[3]));

                        russianAddress.Add(city);
                        russianAddress.Add(street);
                        russianAddress.Add(regionISOCode);
                        russianAddress.Add(postalCode);
                    }
                }

                //--------buyer------------------
                XElement glnbuyer = new XElement("gln", PlatInfo[2]);
                buyer.Add(glnbuyer);
                string BuyerISOCode;

                if (iso == true)//костыли для азбуки вкуса
                {
                    XElement organizationbuyer = new XElement("organization");
                    XElement russianAddressbuyer = new XElement("russianAddress");

                    buyer.Add(organizationbuyer);
                    buyer.Add(russianAddressbuyer);

                    //--------organization-buyer------------------
                    XElement namebuyer = new XElement("name", PlatInfo[1]);
                    XElement innbuyer = new XElement("inn", PlatInfo[3]);
                    XElement kppbuyer = new XElement("kpp", PlatInfo[4]);

                    organizationbuyer.Add(namebuyer);
                    organizationbuyer.Add(innbuyer);
                    organizationbuyer.Add(kppbuyer);

                    //-------russianAddress-buyer---------------------------
                    BuyerISOCode = DispOrders.GetISOCode(Convert.ToString(PlatInfo[0]));
                    XElement regionISOCodebuyer = new XElement("regionISOCode", BuyerISOCode);
                    russianAddressbuyer.Add(regionISOCodebuyer);
                }

                //--------invoicee------------------
                XElement glninvoicee = new XElement("gln", PlatInfo[2]);
                invoicee.Add(glninvoicee);

                if (iso == true)//костыли для азбуки вкуса
                {
                    XElement organizationinvoicee = new XElement("organization");
                    XElement russianAddressinvoicee = new XElement("russianAddress");


                    invoicee.Add(organizationinvoicee);
                    invoicee.Add(russianAddressinvoicee);

                    //--------organization-invoicee------------------
                    XElement nameinvoicee = new XElement("name", PlatInfo[1]);
                    XElement inninvoicee = new XElement("inn", PlatInfo[3]);
                    XElement kppinvoicee = new XElement("kpp", PlatInfo[4]);

                    organizationinvoicee.Add(nameinvoicee);
                    organizationinvoicee.Add(inninvoicee);
                    organizationinvoicee.Add(kppinvoicee);

                    //-------russianAddress-invoicee---------------------------
                    BuyerISOCode = DispOrders.GetISOCode(Convert.ToString(PlatInfo[0]));
                    XElement regionISOCodeinvoicee = new XElement("regionISOCode", BuyerISOCode);
                    russianAddressinvoicee.Add(regionISOCodeinvoicee);
                }

                //---------deliveryInfo------------------------------------
                XElement estimatedDeliveryDateTime = new XElement("estimatedDeliveryDateTime", infoPrdZkg[2]);
                XElement actualDeliveryDateTime = new XElement("actualDeliveryDateTime", infoPrdZkg[2]);
                XElement shipFrom = new XElement("shipFrom");
                XElement shipTo = new XElement("shipTo");

                deliveryInfo.Add(estimatedDeliveryDateTime);
                deliveryInfo.Add(actualDeliveryDateTime);
                deliveryInfo.Add(shipFrom);
                deliveryInfo.Add(shipTo);

                //---------shipFrom----------------------
                if (UseMasterGLN == true)
                {
                    XElement glnFrom = new XElement("gln", ILN_Edi);
                    shipFrom.Add(glnFrom);
                }
                else
                {
                    XElement glnFrom = new XElement("gln", ILN_Edi_S);
                    shipFrom.Add(glnFrom);
                }

                if (iso == true)//костыли для азбуки вкуса
                {
                    XElement organizationFrom = new XElement("organization");
                    XElement russianAddressFrom = new XElement("russianAddress");

                    shipFrom.Add(organizationFrom);
                    shipFrom.Add(russianAddressFrom);

                    //--------organization------------------
                    XElement nameFrom = new XElement("name", FirmInfo[0]);
                    XElement innFrom = new XElement("inn", FirmInfo[1]);
                    XElement kppFrom = new XElement("kpp", FirmInfo[2]);

                    organizationFrom.Add(nameFrom);
                    organizationFrom.Add(innFrom);
                    organizationFrom.Add(kppFrom);

                    //---------russianAddress----------------
                    XElement cityFrom = new XElement("city", Convert.ToString(FirmAdr[1]));
                    XElement streetFrom = new XElement("street", Convert.ToString(FirmAdr[0]));
                    XElement regionISOCodeFrom = new XElement("regionISOCode", sellerISOCode);
                    XElement postalCodeFrom = new XElement("postalCode", Convert.ToString(FirmAdr[3]));

                    russianAddressFrom.Add(cityFrom);
                    russianAddressFrom.Add(streetFrom);
                    russianAddressFrom.Add(regionISOCodeFrom);
                    russianAddressFrom.Add(postalCodeFrom);
                }

                //---------ShipTo-------------------------
                XElement glnTo = new XElement("gln", DelivInfo[2]);
                shipTo.Add(glnTo);

                if (iso == true)//костыли для азбуки вкуса
                {
                    XElement organizationTo = new XElement("organization");
                    XElement russianAddressTo = new XElement("russianAddress");

                    shipTo.Add(organizationTo);
                    shipTo.Add(russianAddressTo);

                    //--------organization------------------
                    XElement nameTo = new XElement("name", PlatInfo[1]);
                    XElement innTo = new XElement("inn", PlatInfo[3]);
                    XElement kppTo = new XElement("kpp", PlatInfo[4]);

                    organizationTo.Add(nameTo);
                    organizationTo.Add(innTo);
                    organizationTo.Add(kppTo);

                    //---------russianAddress----------------
                    string DelivISOCode = DispOrders.GetISOCode(Convert.ToString(DelivInfo[0]));
                    XElement regionISOCodeTo = new XElement("regionISOCode", DelivISOCode);
                    russianAddressTo.Add(regionISOCodeTo);
                }

                //-----------lineItems--------------------
                XElement currencyISOCode = new XElement("currencyISOCode", "RUB");

                XElement totalSumExcludingTaxes = new XElement("totalSumExcludingTaxes", Total[5]);//без ндс
                XElement totalVATAmount = new XElement("totalVATAmount", Convert.ToDecimal(Total[4]) - Convert.ToDecimal(Total[5]));//ндс
                XElement totalAmount = new XElement("totalAmount", Total[4]);//c ндс

                lineItems.Add(currencyISOCode);

                //----------lineItem--------------------------
                string EAN_F = "";
                for (int i = 0; i < CntLinesInvoice; i++)
                {
                    XElement LineItem = new XElement("lineItem");

                    lineItems.Add(LineItem);

                    object[] BICode = Verifiacation.GetBuyerItemCode(Convert.ToString(PlatInfo[5]), Convert.ToString(Item[i, 1]));

                    if (Convert.ToString(DelivInfo[10]) == "MDOU") // для садиков надо мнемокод
                        BICode[0] = Verifiacation.GetMnemoCode(Convert.ToString(Item[i, 0]), Convert.ToString(PlatInfo[8])); // это для садиков, мнемокод запихан сюда. берём его, если нет другого артикула покупателя. чтобы не испортить.

                    XElement orderLineNumber = new XElement("orderLineNumber", i + 1);
                    EAN_F = Convert.ToString(Item[i, 0]).Substring(0, 13);  //Обрезаем штрих-код до 13 символов
                    //XElement gtin = new XElement("gtin", Item[i, 0]);
                    XElement gtin = new XElement("gtin", EAN_F);
                    XElement internalSupplierCode = new XElement("internalSupplierCode", Item[i, 2]);
                    XElement internalBuyerCode = new XElement("internalBuyerCode", BICode[0]);
                    XElement description = new XElement("description", Item[i, 3]);
                    XElement quantity;
                    XAttribute unitOfMeasure;
                    XElement netPrice;
                    XElement netPriceWithVAT;

                    if (Convert.ToString(DelivInfo[10]) == "MDOU")
                    {
                        quantity = new XElement("quantity", Item[i, 14]);
                        unitOfMeasure = new XAttribute("unitOfMeasure", "KGM");  // им ВСЁ надо в КГ
                        netPrice = new XElement("netPrice", (Convert.ToDecimal(Item[i, 12]) / Convert.ToDecimal(Item[i, 14])));
                        netPriceWithVAT = new XElement("netPriceWithVAT", ((Convert.ToDecimal(Item[i, 11]) + Convert.ToDecimal(Item[i, 12])) / Convert.ToDecimal(Item[i, 14])));
                    }
                    else
                    {
                        quantity = new XElement("quantity", Item[i, 4]);
                        unitOfMeasure = new XAttribute("unitOfMeasure", Item[i, 7]);
                        netPrice = new XElement("netPrice", Item[i, 5]);
                        netPriceWithVAT = new XElement("netPriceWithVAT", Item[i, 6]);
                    }

                    XElement netAmount = new XElement("netAmount", Item[i, 12]);
                    XElement vATRate = new XElement("vATRate", Convert.ToInt32(Item[i, 9]));
                    XElement vATAmount = new XElement("vATAmount", Item[i, 11]);
                    XElement amount = new XElement("amount", Convert.ToDecimal(Item[i, 11]) + Convert.ToDecimal(Item[i, 12]));

                    XElement SupplierItemCode = new XElement("SupplierItemCode", Item[i, 2]);
                    XElement InvoiceUnitNetPrice = new XElement("InvoiceUnitNetPrice", Item[i, 5]);
                    XElement InvoiceUnitGrossPrice = new XElement("InvoiceUnitGrossPrice", Item[i, 6]);

                    LineItem.Add(gtin);
                    LineItem.Add(internalBuyerCode);
                    LineItem.Add(internalSupplierCode);
                    LineItem.Add(orderLineNumber);
                    LineItem.Add(description);
                    LineItem.Add(quantity);
                    quantity.Add(unitOfMeasure);
                    LineItem.Add(netPrice);
                    LineItem.Add(netPriceWithVAT);
                    LineItem.Add(netAmount);
                    LineItem.Add(vATRate);
                    LineItem.Add(vATAmount);
                    LineItem.Add(amount);
                    if (Convert.ToString(DelivInfo[10]) == "MDOU")
                    {
                        XElement comment = new XElement("comment", BICode[0]);
                        LineItem.Add(comment);
                    }
                }
                lineItems.Add(totalSumExcludingTaxes);
                lineItems.Add(totalVATAmount);
                lineItems.Add(totalAmount);

                //------сохранение документа-----------
                try
                {
                    xdoc.Save(InvoiceKONTUR + nameInv);
                    xdoc.Save(ArchiveKONTUR + nameInv);
                    string message = "СКБ-Контур. Счет-Фактура " + nameInv + " создана в " + InvoiceKONTUR;
                    Program.WriteLine(message);
                    DispOrders.WriteInvoiceLog(Convert.ToString(PlatInfo[0]) + " - " + Convert.ToString(PlatInfo[1]), Convert.ToString(DelivInfo[0]) + " - " + Convert.ToString(DelivInfo[1]), nameInv, Convert.ToString(CurrDataInvoice[6]), 0, message, DateTime.Now);
                    DispOrders.WriteProtocolEDI("Счет фактура", nameInv, PlatInfo[0] + " - " + PlatInfo[1], 0, DelivInfo[0] + " - " + DelivInfo[1], "Счет фактура сформирована", DateTime.Now, Convert.ToString(CurrDataInvoice[6]), "KONTUR");
                    ReportEDI.RecordCountEDoc("СКБ-Контур", "Invoice", 1);
                    //запись в лог отправки  СФ
                    int CorSf = Convert.ToInt32(CurrDataInvoice[5]);
                    string Doc;
                    if (CorSf == 0)//СФ
                    {
                        Doc = "5";
                    }
                    else//КСФ
                    {
                        Doc = "9";
                    }
                    DispOrders.WriteEDiSentDoc(Doc, nameInv, Convert.ToString(CurrDataInvoice[3]), Convert.ToString(infoSf[0]), "123", Convert.ToString(Total[4]), Convert.ToString(CurrDataInvoice[6]),1);
                }
                catch
                {
                    string message_error = "СКБ-Контур. Не могу создать xml файл Счет-Фактуры в " + InvoiceKONTUR + ". Нет доступа или диск переполнен.";
                    DispOrders.WriteInvoiceLog(Convert.ToString(PlatInfo[0]) + " - " + Convert.ToString(PlatInfo[1]), Convert.ToString(DelivInfo[0]) + " - " + Convert.ToString(DelivInfo[1]), nameInv, Convert.ToString(CurrDataInvoice[6]), 10, message_error, DateTime.Now);
                    DispOrders.WriteProtocolEDI("Счет фактура", nameInv, PlatInfo[0] + " - " + PlatInfo[1], 10, DelivInfo[0] + " - " + DelivInfo[1], "Счет фактура не сформирована. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataInvoice[6]), "KONTUR");
                    Program.WriteLine(message_error);
                    //запись в лог о неудаче
                }
            }
        }

        public static void CreateKonturCOInvoiceDOP(List<object> CurrDataCOInvoice) //0 ProviderOpt, 1 ProviderZkg, 2 NastDoc_Fmt, 3 SklSf_Rcd, 4 SklSf_TpOtg, 5 SklSfA_RcdCor, 6 PrdZkg_NmrExt, 7 PrdZkg_Rcd, 8 PrdZkg_Dt, 9 SklNk_TDrvNm
        {
            //признак необходимости ISOCode
            bool iso = false;

            //получение путей
            string ArchiveKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.АРХИВ");

            //string InvoiceKONTUR = "\\\\fileshare\\EXPIMP\\OrderIntake\\SKBKONTUR\\OUTBOX\\";//test
            //string ArchiveKONTUR = "\\\\fileshare\\EXPIMP\\OrderIntake\\SKBKONTUR\\ARCHIVE\\";

            //генерация имени файла.
            string id; //= Convert.ToString(DateTime.Now); ;   // ну это так, ищу замену    
            string id2 = Convert.ToString(Guid.NewGuid());        // эта строка иногда генерит повторно тот же ИД подряд !!!!!!!!!   и как-то по утрам, потом нормально работает ... 
            id = Convert.ToString(Guid.NewGuid());        // эта строка иногда генерит повторно тот же ИД подряд !!!!!!!!!   и как-то по утрам, потом нормально работает ... 
            if (id == id2)                                  // ну или другая фигня. не понятно. такие файлики уже есть...
                id = id + "_";
            string nameInv = "COINVOIC_" + id + ".xml";

            //Запрос данных Корректировочной СФ
            object[] infoSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataCOInvoice[3])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO

            //Запрос данных Корректируемой (отгрузочной) СФ
            object[] infoCorSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataCOInvoice[5])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO

            Program.WriteLine("СФ " + Convert.ToString(infoSf[0]) + " Корректируемая СФ " + Convert.ToString(infoCorSf[0]));

            //Возвращает заголовок накладной по рсд заказа   
            object[] infoNk = Verifiacation.GetNkDataFromZkg(Convert.ToInt64(CurrDataCOInvoice[7])); //0 SklNk_Nmr, 1 SklNk_Dat

            //получение данных
            object[] DelivInfo = Verifiacation.GetDataFromPtn_Rcd(Convert.ToString(infoSf[4]));   //10 = формат
            object[] PlatInfo = Verifiacation.GetDataFromPtn_Rcd(Convert.ToString(infoSf[2]));

            Program.WriteLine("DelivInfo " + Convert.ToString(infoSf[4]) + " PlatInfo " + Convert.ToString(infoSf[2]));
            Program.WriteLine("DelivInfo GLN " + Convert.ToString(DelivInfo[2]) + " PlatInfo GLN " + Convert.ToString(PlatInfo[2]));

            //какой gln номер использовать
            string check = Convert.ToString(DelivInfo[8]);
            bool UseMasterGLN = false;
            if (check.Length != 0)
            {
                UseMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(DelivInfo[8]));
            }
            //bool UseMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(DelivInfo[8]));
            string ILN_Edi, ILN_Edi_S;
            string InvoiceKONTUR;
            object[] FirmInfo, FirmInfo_G;
            object[] FirmAdr, FirmAdr_G;
            if (UseMasterGLN == true) //используем данные головного предприятия, для Агроторгов и Метро в любом случае
            {
                ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");

                FirmInfo = Verifiacation.GetMasterFirmInfo();
                FirmAdr = Verifiacation.GetMasterFirmAdr();
                try
                {
                    InvoiceKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.ЭКСПОРТ");
                }
                catch
                {
                    InvoiceKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.СФ");
                }
            }

            else//используем данные текущего предприятия
            {

                ILN_Edi = DispOrders.GetValueOption("СКБ-КОНТУР.ИЛН_ПРЕДПРИЯТИЯ");
                if (ILN_Edi == "") ILN_Edi = DispOrders.GetValueOption("ОБЩИЕ.ИЛН");
                InvoiceKONTUR = DispOrders.GetValueOption("СКБ-КОНТУР.СФ");
                if (Convert.ToDateTime(PlatInfo[9]) > Convert.ToDateTime(infoSf[1]))      // 13 это дата с которой надо ставить новые данные
                {
                    FirmInfo = Verifiacation.GetFirmInfo("20171130"); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO        // берём как до 01.12.2017
                }
                else
                {
                    FirmInfo = Verifiacation.GetFirmInfo(Convert.ToDateTime(infoSf[1]).ToString("yyyyMMdd"));
                }
                FirmAdr = Verifiacation.GetFirmAdr();
            }

            ILN_Edi_S = DispOrders.GetValueOption("СКБ-КОНТУР.ИЛН_ПРЕДПРИЯТИЯ");
            FirmInfo_G = Verifiacation.GetMasterFirmInfo();
            FirmAdr_G = Verifiacation.GetMasterFirmAdr();

            //костыли для азбуки вкуса
            if (Convert.ToString(infoSf[2]).Substring(0, 2) == "41")
            {
                iso = true;
            }
            else
            {
                iso = false;
            }

            string sellerISOCode;//костыли для азбуки вкуса
            if (ILN_Edi == "4607008059991")
            {
                sellerISOCode = "RU-CHE";
            }
            else
            {
                sellerISOCode = "RU-SVE";
            }

            if (File.Exists(InvoiceKONTUR + nameInv))          // уже есть такие файлы??? почему-то не может сохранить
                nameInv = nameInv = "COINVOIC_" + id + "_.xml"; ;

            int CntLinesInvoice = Verifiacation.CountItemsInInvoice(Convert.ToString(CurrDataCOInvoice[3]));

            bool PCE = Verifiacation.UsePCE(Convert.ToString(infoSf[4]));//проверка на использование штук в исходящих докуметов

            object[,] Item = DispOrders.GetItemsFromInvoice(Convert.ToString(CurrDataCOInvoice[3]), CntLinesInvoice, PCE);

            object[] Total = DispOrders.GetTotal(Convert.ToString(CurrDataCOInvoice[3]), 5);

            XDocument xdoc = new XDocument();

            //основные элементы (1 уровень)
            XElement eDIMessage = new XElement("eDIMessage");
            XElement interchangeHeader = new XElement("interchangeHeader");
            XElement correctiveInvoice = new XElement("correctiveInvoice");

            XAttribute idMessage = new XAttribute("id", id);
            XAttribute creationDateTime = new XAttribute("creationDateTime", (DateTime.Now).ToString("yyyy-MM-dd HH:mm:ss"));

            xdoc.Add(eDIMessage);
            eDIMessage.Add(interchangeHeader);
            eDIMessage.Add(correctiveInvoice);
            eDIMessage.Add(idMessage);

            //------interchangeHeader---------------
            XElement sender = new XElement("sender", ILN_Edi);
            XElement recipient = new XElement("recipient", Convert.ToString(PlatInfo[2]));
            XElement documentType = new XElement("documentType", "COINVOIC");

            interchangeHeader.Add(sender);
            interchangeHeader.Add(recipient);
            interchangeHeader.Add(documentType);
            interchangeHeader.Add(creationDateTime);

            //------invoice------------------------
            XAttribute numberInvoice = new XAttribute("number", infoSf[0]);
            XAttribute dateInvoice = new XAttribute("date", (Convert.ToDateTime(infoSf[1])).ToString("yyyy-MM-dd"));
            XAttribute TypeInvoice = new XAttribute("type", "Original");
            XElement originInvoic = new XElement("originInvoic");
            XElement originOrder = new XElement("originOrder");

            correctiveInvoice.Add(numberInvoice);
            correctiveInvoice.Add(dateInvoice);
            correctiveInvoice.Add(TypeInvoice);
            correctiveInvoice.Add(originInvoic);
            correctiveInvoice.Add(originOrder);

            XAttribute numberoriginInvoic = new XAttribute("number", infoCorSf[0]);
            XAttribute dateoriginInvoic;
            dateoriginInvoic = new XAttribute("date", Convert.ToDateTime(infoCorSf[1]).ToString("yyyy-MM-dd"));

            originInvoic.Add(numberoriginInvoic);
            originInvoic.Add(dateoriginInvoic);

            XAttribute numberoriginOrder = new XAttribute("number", CurrDataCOInvoice[6]);
            XAttribute dateoriginOrder;
            //дату заказа берем из даты документа заказа, если вывалилась ошибка, значит заказ был сделан вручную, дату заказа проставляем как в в заказе ИСПРО
            try
            {
                Program.WriteLine("дата документа заказа " + Convert.ToDateTime(Verifiacation.GetFldFromEdiExch(Convert.ToInt64(CurrDataCOInvoice[7]), "Exch_OrdDocDat")).ToString("yyyy-MM-dd"));

                dateoriginOrder = new XAttribute("date", Convert.ToDateTime(Verifiacation.GetFldFromEdiExch(Convert.ToInt64(CurrDataCOInvoice[7]), "Exch_OrdDocDat")).ToString("yyyy-MM-dd"));
            }
            catch
            {

                dateoriginOrder = new XAttribute("date", Convert.ToDateTime(CurrDataCOInvoice[8]).ToString("yyyy-MM-dd"));
            }

            originOrder.Add(numberoriginOrder);
            originOrder.Add(dateoriginOrder);

            XElement despatchIdentificator = new XElement("despatchIdentificator");
            correctiveInvoice.Add(despatchIdentificator);
            XAttribute numberdespatchIdentificator = new XAttribute("number", infoNk[0]);

            XAttribute datedespatchIdentificator;

            Program.WriteLine("numberoriginOrder " + CurrDataCOInvoice[6] + " numberdespatchIdentificator " + infoNk[0]);

            datedespatchIdentificator = new XAttribute("date", Convert.ToDateTime(infoNk[1]).ToString("yyyy-MM-dd"));

            despatchIdentificator.Add(numberdespatchIdentificator);
            despatchIdentificator.Add(datedespatchIdentificator);

            object[] infoRecAdv = Verifiacation.GetRecAdvInfo(Convert.ToInt64(CurrDataCOInvoice[7]));

            if (infoRecAdv[0] != null && infoRecAdv[0] != "")  // это теперь есть во вьюхе, CurrDataInvoice[21],22
            {
                XElement receivingIdentificator = new XElement("receivingIdentificator");
                XAttribute numberreceivingIdentificator = new XAttribute("number", infoRecAdv[0]);
                XAttribute datereceivingIdentificator;

                datereceivingIdentificator = new XAttribute("date", Convert.ToDateTime(infoRecAdv[1]).ToString("yyyy-MM-dd"));

                correctiveInvoice.Add(receivingIdentificator);
                receivingIdentificator.Add(numberreceivingIdentificator);
                receivingIdentificator.Add(datereceivingIdentificator);
            }

            // contractIdentificator = номер контракта и дата при наличии
            object[] ContractInfoInfo = Verifiacation.GetContractInfo(Convert.ToString(infoSf[2])); //контракты
            if (ContractInfoInfo[0] != null)
            {
                XElement contractIdentificator = new XElement("contractIdentificator");
                XAttribute numbercontractIdentificator = new XAttribute("number", ContractInfoInfo[0]);
                XAttribute DatecontractIdentificator = new XAttribute("date", Convert.ToDateTime(ContractInfoInfo[1]).ToString("yyyy-MM-dd"));
                correctiveInvoice.Add(contractIdentificator);
                contractIdentificator.Add(numbercontractIdentificator);
                contractIdentificator.Add(DatecontractIdentificator);
            }

            XElement seller = new XElement("seller");
            XElement buyer = new XElement("buyer");
            XElement invoicee = new XElement("invoicee");
            XElement deliveryInfo = new XElement("deliveryInfo");
            XElement lineItems = new XElement("lineItems");

            correctiveInvoice.Add(seller);
            correctiveInvoice.Add(buyer);
            correctiveInvoice.Add(invoicee);
            correctiveInvoice.Add(deliveryInfo);
            correctiveInvoice.Add(lineItems);

            //--------seller-----------------------
            XElement gln = new XElement("gln", ILN_Edi);
            seller.Add(gln);

            if (iso == true)//костыли для азбуки вкуса
            {
                XElement organization = new XElement("organization");
                XElement russianAddress = new XElement("russianAddress");

                seller.Add(organization);
                seller.Add(russianAddress);
                //--------organization------------------
                XElement name = new XElement("name", FirmInfo[0]);
                XElement inn = new XElement("inn", FirmInfo[1]);
                XElement kpp = new XElement("kpp", FirmInfo[2]);

                organization.Add(name);
                organization.Add(inn);
                organization.Add(kpp);

                //---------russianAddress----------------
                XElement city = new XElement("city", Convert.ToString(FirmAdr[1]));
                XElement street = new XElement("street", Convert.ToString(FirmAdr[0]));
                XElement regionISOCode = new XElement("regionISOCode", sellerISOCode);
                XElement postalCode = new XElement("postalCode", Convert.ToString(FirmAdr[3]));

                russianAddress.Add(city);
                russianAddress.Add(street);
                russianAddress.Add(regionISOCode);
                russianAddress.Add(postalCode);
            }
            else
            {
                if (UseMasterGLN == false)
                {
                    XElement organization = new XElement("organization");
                    XElement russianAddress = new XElement("russianAddress");

                    seller.Add(organization);
                    seller.Add(russianAddress);
                    //--------organization------------------
                    XElement name = new XElement("name", "АО \"Группа Компаний \"Российское Молоко\" (АО \"ГК \"РОСМОЛ\")");
                    XElement inn = new XElement("inn", FirmInfo_G[1]);
                    XElement kpp = new XElement("kpp", FirmInfo[2]);

                    organization.Add(name);
                    organization.Add(inn);
                    organization.Add(kpp);

                    //---------russianAddress----------------
                    sellerISOCode = "RU-CHE";
                    XElement city = new XElement("city", Convert.ToString(FirmAdr_G[1]));
                    XElement street = new XElement("street", Convert.ToString(FirmAdr_G[0]));
                    XElement regionISOCode = new XElement("regionISOCode", sellerISOCode);
                    XElement postalCode = new XElement("postalCode", Convert.ToString(FirmAdr_G[3]));

                    russianAddress.Add(city);
                    russianAddress.Add(street);
                    russianAddress.Add(regionISOCode);
                    russianAddress.Add(postalCode);
                }
            }

            //--------buyer------------------
            XElement glnbuyer = new XElement("gln", Convert.ToString(PlatInfo[2]));
            buyer.Add(glnbuyer);
            string BuyerISOCode;

            XElement organizationbuyer = new XElement("organization");
            XElement russianAddressbuyer = new XElement("russianAddress");

            buyer.Add(organizationbuyer);
            buyer.Add(russianAddressbuyer);

            //--------organization-buyer------------------
            XElement namebuyer = new XElement("name", Convert.ToString(PlatInfo[1]));
            XElement innbuyer = new XElement("inn", Convert.ToString(PlatInfo[3]));
            XElement kppbuyer = new XElement("kpp", Convert.ToString(PlatInfo[4]));

            organizationbuyer.Add(namebuyer);
            organizationbuyer.Add(innbuyer);
            organizationbuyer.Add(kppbuyer);

            //-------russianAddress-buyer---------------------------
            BuyerISOCode = DispOrders.GetISOCode(Convert.ToString(PlatInfo[0]));

            string S_streetbuyer = Convert.ToString(PlatInfo[6]);
            int indexOfChar = S_streetbuyer.IndexOf(',');
            S_streetbuyer = S_streetbuyer.Substring(indexOfChar + 1);
            indexOfChar = S_streetbuyer.IndexOf(',');
            S_streetbuyer = S_streetbuyer.Substring(indexOfChar + 1);

            //---------russianAddress----------------
            //XElement citybuyer = new XElement("city", Convert.ToString(PlatInfo[1]));
            XElement streetbuyer = new XElement("street", S_streetbuyer);
            XElement regionISOCodebuyer = new XElement("regionISOCode", BuyerISOCode);
            XElement postalCodebuyer = new XElement("postalCode", Convert.ToString(PlatInfo[7]));

            //russianAddressbuyer.Add(citybuyer);
            russianAddressbuyer.Add(streetbuyer);
            russianAddressbuyer.Add(regionISOCodebuyer);
            russianAddressbuyer.Add(postalCodebuyer);

            //--------invoicee------------------
            /*
            XElement glninvoicee = new XElement("gln", Convert.ToString(PlatInfo[2]));
            invoicee.Add(glninvoicee);

            
            XElement organizationinvoicee = new XElement("organization");
            XElement russianAddressinvoicee = new XElement("russianAddress");

            invoicee.Add(organizationinvoicee);
            invoicee.Add(russianAddressinvoicee);

            //--------organization-invoicee------------------
            XElement nameinvoicee = new XElement("name", Convert.ToString(PlatInfo[1]));
            XElement inninvoicee = new XElement("inn", Convert.ToString(PlatInfo[3]));
            XElement kppinvoicee = new XElement("kpp", Convert.ToString(PlatInfo[4]));

            organizationinvoicee.Add(nameinvoicee);
            organizationinvoicee.Add(inninvoicee);
            organizationinvoicee.Add(kppinvoicee);

            //-------russianAddress-invoicee---------------------------
            BuyerISOCode = DispOrders.GetISOCode(Convert.ToString(PlatInfo[0]));
            XElement regionISOCodeinvoicee = new XElement("regionISOCode", BuyerISOCode);
            russianAddressinvoicee.Add(regionISOCodeinvoicee);
            */

            //---------deliveryInfo------------------------------------
            XElement shipFrom = new XElement("shipFrom");
            XElement shipTo = new XElement("shipTo");

            deliveryInfo.Add(shipFrom);
            deliveryInfo.Add(shipTo);

            //---------shipFrom----------------------
            if (UseMasterGLN == true)
            {
                XElement glnFrom = new XElement("gln", ILN_Edi);
                shipFrom.Add(glnFrom);
            }
            else
            {
                XElement glnFrom = new XElement("gln", ILN_Edi_S);
                shipFrom.Add(glnFrom);
            }

            if (iso == true)//костыли для азбуки вкуса
            {
                XElement organizationFrom = new XElement("organization");
                XElement russianAddressFrom = new XElement("russianAddress");

                shipFrom.Add(organizationFrom);
                shipFrom.Add(russianAddressFrom);

                //--------organization------------------
                XElement nameFrom = new XElement("name", FirmInfo[0]);
                XElement innFrom = new XElement("inn", FirmInfo[1]);
                XElement kppFrom = new XElement("kpp", FirmInfo[2]);

                organizationFrom.Add(nameFrom);
                organizationFrom.Add(innFrom);
                organizationFrom.Add(kppFrom);

                //---------russianAddress----------------
                XElement cityFrom = new XElement("city", Convert.ToString(FirmAdr[1]));
                XElement streetFrom = new XElement("street", Convert.ToString(FirmAdr[0]));
                XElement regionISOCodeFrom = new XElement("regionISOCode", sellerISOCode);
                XElement postalCodeFrom = new XElement("postalCode", Convert.ToString(FirmAdr[3]));

                russianAddressFrom.Add(cityFrom);
                russianAddressFrom.Add(streetFrom);
                russianAddressFrom.Add(regionISOCodeFrom);
                russianAddressFrom.Add(postalCodeFrom);
            }

            //---------ShipTo-------------------------
            XElement glnTo = new XElement("gln", Convert.ToString(DelivInfo[2]));
            shipTo.Add(glnTo);

            if (iso == true)//костыли для азбуки вкуса
            {
                XElement organizationTo = new XElement("organization");
                XElement russianAddressTo = new XElement("russianAddress");

                shipTo.Add(organizationTo);
                shipTo.Add(russianAddressTo);

                //--------organization------------------
                XElement nameTo = new XElement("name", Convert.ToString(PlatInfo[1]));
                XElement innTo = new XElement("inn", Convert.ToString(PlatInfo[3]));
                XElement kppTo = new XElement("kpp", Convert.ToString(PlatInfo[4]));

                organizationTo.Add(nameTo);
                organizationTo.Add(innTo);
                organizationTo.Add(kppTo);

                //---------russianAddress----------------
                string DelivISOCode = DispOrders.GetISOCode(Convert.ToString(DelivInfo[0]));
                XElement regionISOCodeTo = new XElement("regionISOCode", DelivISOCode);
                russianAddressTo.Add(regionISOCodeTo);
            }

            Program.WriteLine("запрос данных спецификации " + Convert.ToString(CurrDataCOInvoice[3]) + "   " + Convert.ToString(CurrDataCOInvoice[5]));

            //-----------lineItems--------------------
            XElement currencyISOCode = new XElement("currencyISOCode", "RUB");

            //без ндс
            String S_totalSumExcludingTaxesDecrease = "";
            String S_totalSumExcludingTaxesIncrease = "";
            //ндс
            String S_totalVATAmountDecrease = "";
            String S_totalVATAmountIncrease = "";
            //c ндс
            String S_totalAmountDecrease = "";
            String S_totalAmountIncrease = "";

            if (Convert.ToDecimal(Total[5]) >= 0)
            {
                S_totalSumExcludingTaxesDecrease = "0"; //уменьшение
                S_totalSumExcludingTaxesIncrease = Convert.ToString(Convert.ToDecimal(Total[5])); //увеличение
                S_totalVATAmountDecrease = "0"; //уменьшение
                S_totalVATAmountIncrease = Convert.ToString(Convert.ToDecimal(Total[4]) - Convert.ToDecimal(Total[5]));//увеличение
                S_totalAmountDecrease = "0"; //уменьшение
                S_totalAmountIncrease = Convert.ToString(Convert.ToDecimal(Total[4]));//увеличение
            }
            else
            {
                S_totalSumExcludingTaxesDecrease = Convert.ToString(Convert.ToDecimal(Total[5])).Substring(1);//убираем минус
                S_totalSumExcludingTaxesDecrease = S_totalSumExcludingTaxesDecrease.Replace(",", ".");
                S_totalSumExcludingTaxesIncrease = "0";
                S_totalVATAmountDecrease = Convert.ToString((-1) * (Convert.ToDecimal(Total[4]) - Convert.ToDecimal(Total[5])));//убираем минус
                S_totalVATAmountDecrease = S_totalVATAmountDecrease.Replace(",", ".");
                S_totalVATAmountIncrease = "0";
                S_totalAmountDecrease = Convert.ToString(Convert.ToDecimal(Total[4])).Substring(1);//убираем минус
                S_totalAmountDecrease = S_totalAmountDecrease.Replace(",", ".");
                S_totalAmountIncrease = "0";
            }

            XElement totalSumExcludingTaxesDecrease = new XElement("totalSumExcludingTaxesDecrease", S_totalSumExcludingTaxesDecrease); //уменьшение
            XElement totalSumExcludingTaxesIncrease = new XElement("totalSumExcludingTaxesIncrease", S_totalSumExcludingTaxesIncrease); //увеличение
            XElement totalVATAmountDecrease = new XElement("totalVATAmountDecrease", S_totalVATAmountDecrease); //уменьшение
            XElement totalVATAmountIncrease = new XElement("totalVATAmountIncrease", S_totalVATAmountIncrease);//увеличение
            XElement totalAmountDecrease = new XElement("totalAmountDecrease", S_totalAmountDecrease); //уменьшение
            XElement totalAmountIncrease = new XElement("totalAmountIncrease", S_totalAmountIncrease);//увеличение

            Program.WriteLine(S_totalSumExcludingTaxesDecrease + " " + S_totalSumExcludingTaxesIncrease + " " + S_totalVATAmountDecrease + " " + S_totalVATAmountIncrease);

            String S_quantityIncrease = "";
            String S_quantityDecrease = "";
            String S_netAmountIncrease = "";
            String S_netAmountDecrease = "";
            String S_vatAmountIncrease = "";
            String S_vatAmountDecrease = "";
            String S_amountIncrease = "";
            String S_amountDecrease = "";

            lineItems.Add(currencyISOCode);

            Program.WriteLine("Количество позиций " + CntLinesInvoice);

            //----------lineItem--------------------------
            string EAN_F = "";
            for (int i = 0; i < CntLinesInvoice; i++)
            {
                Program.WriteLine("Блок1");
                XElement LineItem = new XElement("lineItem");

                lineItems.Add(LineItem);
                Program.WriteLine(Convert.ToString(CurrDataCOInvoice[5]) + " " + Convert.ToString(Item[i, 2]) + " " + Convert.ToString(PCE));
                Program.WriteLine(Convert.ToString(PlatInfo[5]) + " " + Convert.ToString(Item[i, 1]));
                try
                {

                    object[,] prevItem = DispOrders.GetItemFromInvoice(Convert.ToString(CurrDataCOInvoice[5]), Convert.ToString(Item[i, 2]), PCE); //Позиция до изменения 

                    Program.WriteLine(Convert.ToString(prevItem[0, 4]));

                    object[] BICode = Verifiacation.GetBuyerItemCode(Convert.ToString(PlatInfo[5]), Convert.ToString(Item[i, 1]));

                    Program.WriteLine(Convert.ToString(BICode[0]));

                    if (Convert.ToString(DelivInfo[10]) == "MDOU") // для садиков надо мнемокод
                    {
                        BICode[0] = Verifiacation.GetMnemoCode(Convert.ToString(Item[i, 0]), Convert.ToString(PlatInfo[8])); // это для садиков, мнемокод запихан сюда. берём его, если нет другого артикула покупателя. чтобы не испортить.
                    }

                    Program.WriteLine("Блок2");
                    XElement orderLineNumber = new XElement("orderLineNumber", i + 1);
                    EAN_F = Convert.ToString(Item[i, 0]).Substring(0, 13);  //Обрезаем штрих-код до 13 символов
                    //XElement gtin = new XElement("gtin", Item[i, 0]);
                    XElement gtin = new XElement("gtin", EAN_F);
                    XElement internalSupplierCode = new XElement("internalSupplierCode", Item[i, 2]);
                    XElement internalBuyerCode = new XElement("internalBuyerCode", BICode[0]);
                    XElement description = new XElement("description", Item[i, 3]);
                    XElement quantityBefore = new XElement("quantityBefore", prevItem[0, 4]);
                    Program.WriteLine("internalBuyerCode" + " " + BICode[0] + " quantityAfter " + (Convert.ToDecimal(prevItem[0, 4]) + Convert.ToDecimal(Item[i, 4])));
                    XElement quantityAfter = new XElement("quantityAfter", (Convert.ToDecimal(prevItem[0, 4]) + Convert.ToDecimal(Item[i, 4])));

                    XElement quantityIncrease;
                    XElement quantityDecrease;
                    XAttribute unitOfMeasure;
                    XElement netPriceBefore;
                    XElement netPriceAfter;
                    XElement netPriceIncrease;
                    XElement netPriceDecrease;
                    XElement netPriceWithVAT;

                    if (Convert.ToString(DelivInfo[10]) == "MDOU")
                    {
                        if (Convert.ToDecimal(Item[i, 14]) >= 0)
                        {
                            S_quantityDecrease = "0"; //уменьшение
                            S_quantityIncrease = Convert.ToString(Item[i, 14]); //увеличение                        
                        }
                        else
                        {
                            S_quantityDecrease = Convert.ToString(Item[i, 14]).Substring(1);//убираем минус
                            S_quantityIncrease = "0";
                        }
                        quantityIncrease = new XElement("quantityIncrease", S_quantityIncrease);
                        quantityDecrease = new XElement("quantityDecrease", S_quantityDecrease);
                        unitOfMeasure = new XAttribute("unitOfMeasure", "KGM");  // им ВСЁ надо в КГ                    
                        netPriceBefore = new XElement("netPriceBefore", (Convert.ToDecimal(Item[i, 12]) / Convert.ToDecimal(Item[i, 14])));
                        netPriceAfter = new XElement("netPriceAfter", (Convert.ToDecimal(Item[i, 12]) / Convert.ToDecimal(Item[i, 14])));
                        netPriceWithVAT = new XElement("netPriceWithVAT", ((Convert.ToDecimal(Item[i, 11]) + Convert.ToDecimal(Item[i, 12])) / Convert.ToDecimal(Item[i, 14])));
                        Program.WriteLine("unitOfMeasure" + " " + "KGM");
                    }
                    else
                    {
                        Program.WriteLine("Блок3");
                        if (Convert.ToDecimal(Item[i, 4]) >= 0)
                        {
                            S_quantityDecrease = "0"; //уменьшение
                            S_quantityIncrease = Convert.ToString(Item[i, 4]); //увеличение 
                        }
                        else
                        {
                            S_quantityDecrease = Convert.ToString(Item[i, 4]).Substring(1);//убираем минус
                            S_quantityIncrease = "0";
                        }
                        quantityIncrease = new XElement("quantityIncrease", S_quantityIncrease);
                        quantityDecrease = new XElement("quantityDecrease", S_quantityDecrease);
                        unitOfMeasure = new XAttribute("unitOfMeasure", Item[i, 7]);
                        netPriceBefore = new XElement("netPriceBefore", Item[i, 5]);
                        netPriceAfter = new XElement("netPriceAfter", Item[i, 5]);
                        netPriceWithVAT = new XElement("netPriceWithVAT", Item[i, 6]);
                        Program.WriteLine("unitOfMeasure" + " " + Item[i, 7]);
                    }
                    Program.WriteLine("Блок4");
                    XElement netAmountBefore = new XElement("netAmountBefore", prevItem[0, 12]);
                    XElement netAmountAfter = new XElement("netAmountAfter", (Convert.ToDecimal(prevItem[0, 12]) + Convert.ToDecimal(Item[i, 12])));

                    if (Convert.ToDecimal(Item[i, 12]) >= 0)
                    {
                        S_netAmountDecrease = "0"; //уменьшение
                        S_netAmountIncrease = Convert.ToString(Item[i, 12]); //увеличение 
                        S_netAmountIncrease = S_netAmountIncrease.Replace(",", ".");
                        S_vatAmountDecrease = "0"; //уменьшение
                        S_vatAmountIncrease = Convert.ToString(Item[i, 11]); //увеличение
                        S_vatAmountIncrease = S_vatAmountIncrease.Replace(",", ".");
                        S_amountDecrease = "0"; //уменьшение
                        S_amountIncrease = Convert.ToString(Convert.ToDecimal(Item[i, 11]) + Convert.ToDecimal(Item[i, 12])); //увеличение
                        S_amountIncrease = S_amountIncrease.Replace(",", ".");
                    }
                    else
                    {
                        S_netAmountDecrease = Convert.ToString(Item[i, 12]).Substring(1);//убираем минус
                        S_netAmountDecrease = S_netAmountDecrease.Replace(",", ".");
                        S_netAmountIncrease = "0";
                        S_vatAmountDecrease = Convert.ToString(Item[i, 11]).Substring(1);//убираем минус
                        S_vatAmountDecrease = S_vatAmountDecrease.Replace(",", ".");
                        S_vatAmountIncrease = "0";
                        S_amountDecrease = Convert.ToString(Convert.ToDecimal(Item[i, 11]) + Convert.ToDecimal(Item[i, 12])).Substring(1);//убираем минус
                        S_amountDecrease = S_amountDecrease.Replace(",", ".");
                        S_amountIncrease = "0";
                    }
                    Program.WriteLine("Блок5");
                    XElement netAmountIncrease = new XElement("netAmountIncrease", S_netAmountIncrease);
                    XElement netAmountDecrease = new XElement("netAmountDecrease", S_netAmountDecrease);

                    XElement vatRateBefore = new XElement("vatRateBefore", Convert.ToInt32(Item[i, 9]));
                    XElement vatRateAfter = new XElement("vatRateAfter", Convert.ToInt32(Item[i, 9]));

                    XElement vatAmountBefore = new XElement("vatAmountBefore", prevItem[0, 11]);
                    XElement vatAmountAfter = new XElement("vatAmountAfter", (Convert.ToDecimal(prevItem[0, 11]) + Convert.ToDecimal(Item[i, 11])));

                    XElement vatAmountIncrease = new XElement("vatAmountIncrease", S_vatAmountIncrease);
                    XElement vatAmountDecrease = new XElement("vatAmountDecrease", S_vatAmountDecrease);

                    XElement amountBefore = new XElement("amountBefore", (Convert.ToDecimal(prevItem[0, 11]) + Convert.ToDecimal(prevItem[0, 12])));
                    XElement amountAfter = new XElement("amountAfter", (Convert.ToDecimal(prevItem[0, 11]) + Convert.ToDecimal(prevItem[0, 12]) + Convert.ToDecimal(Item[i, 11]) + Convert.ToDecimal(Item[i, 12])));

                    XElement amountIncrease = new XElement("amountIncrease", S_amountIncrease);
                    XElement amountDecrease = new XElement("amountDecrease", S_amountDecrease);

                    Program.WriteLine(S_netAmountIncrease + " " + S_netAmountDecrease);

                    Program.WriteLine("Блок6");
                    LineItem.Add(gtin);
                    LineItem.Add(internalBuyerCode);
                    LineItem.Add(internalSupplierCode);
                    LineItem.Add(orderLineNumber);
                    LineItem.Add(description);
                    LineItem.Add(quantityBefore);
                    LineItem.Add(quantityAfter);
                    LineItem.Add(quantityIncrease);
                    LineItem.Add(quantityDecrease);

                    quantityBefore.Add(unitOfMeasure);
                    quantityAfter.Add(unitOfMeasure);
                    quantityIncrease.Add(unitOfMeasure);
                    quantityDecrease.Add(unitOfMeasure);

                    LineItem.Add(netPriceBefore);
                    LineItem.Add(netPriceAfter);
                    LineItem.Add(netPriceWithVAT);
                    LineItem.Add(netAmountBefore);  //Стоимость без НДС до
                    LineItem.Add(netAmountAfter);   //Стоимость без НДС после 
                    LineItem.Add(netAmountIncrease);
                    LineItem.Add(netAmountDecrease);

                    LineItem.Add(vatRateBefore);
                    LineItem.Add(vatRateAfter);
                    LineItem.Add(vatAmountBefore);  //Сумма НДС до
                    LineItem.Add(vatAmountAfter);   //Сумма НДС после
                    LineItem.Add(vatAmountIncrease);
                    LineItem.Add(vatAmountDecrease);
                    LineItem.Add(amountBefore);     //Стоимость с НДС до
                    LineItem.Add(amountAfter);      //Стоимость с НДС после
                    LineItem.Add(amountIncrease);
                    LineItem.Add(amountDecrease);

                    if (Convert.ToString(DelivInfo[10]) == "MDOU")
                    {
                        XElement comment = new XElement("comment", BICode[0]);
                        LineItem.Add(comment);
                    }
                }
                catch (Exception e)
                {
                    string error = "Ошибка в процедуре ";
                    Program.WriteLine(error);
                }
            }
            Program.WriteLine("Блок7");
            lineItems.Add(totalSumExcludingTaxesDecrease);
            lineItems.Add(totalSumExcludingTaxesIncrease);
            lineItems.Add(totalVATAmountDecrease);
            lineItems.Add(totalVATAmountIncrease);
            lineItems.Add(totalAmountDecrease);
            lineItems.Add(totalAmountIncrease);

            Program.WriteLine("Сохранение кор. счет-фактуры");

            //------сохранение документа-----------
            try
            {
                xdoc.Save(InvoiceKONTUR + nameInv);
                xdoc.Save(ArchiveKONTUR + nameInv);
                string message = "СКБ-Контур. Кор. счет-Фактура " + nameInv + " создана в " + InvoiceKONTUR;
                Program.WriteLine(message);
                DispOrders.WriteInvoiceLog(Convert.ToString(PlatInfo[0]) + " - " + Convert.ToString(PlatInfo[1]), Convert.ToString(DelivInfo[0]) + " - " + Convert.ToString(DelivInfo[1]), nameInv, Convert.ToString(CurrDataCOInvoice[6]), 0, message, DateTime.Now);
                DispOrders.WriteProtocolEDI("Кор. счет фактура", nameInv, Convert.ToString(PlatInfo[0]) + " - " + Convert.ToString(PlatInfo[1]), 0, DelivInfo[0] + " - " + DelivInfo[1], "Кор. счет фактура сформирована", DateTime.Now, Convert.ToString(CurrDataCOInvoice[6]), "KONTUR");

                //запись в лог отправки  СФ
                int CorSf = Convert.ToInt32(CurrDataCOInvoice[4]);
                string Doc;
                if (CorSf == 0)//СФ
                {
                    Doc = "5";
                }
                else//КСФ
                {
                    Doc = "9";
                }
                Program.WriteLine("Записываем в журнал отправленных " + Doc + ", " + nameInv + ", " + Convert.ToString(CurrDataCOInvoice[3]) + ", " + Convert.ToString(infoSf[0]) + ", " + Convert.ToString(Total[4]) + ", " + Convert.ToString(CurrDataCOInvoice[6]));
                DispOrders.WriteEDiSentDoc(Doc, nameInv, Convert.ToString(CurrDataCOInvoice[3]), Convert.ToString(infoSf[0]), "123", Convert.ToString(Total[4]), Convert.ToString(CurrDataCOInvoice[6]),1);
                Program.WriteLine("Блок13");
                ReportEDI.RecordCountEDoc("СКБ-Контур", "Invoice", 1);
            }
            catch
            {
                string message_error = "СКБ-Контур. Не могу создать xml файл Кор. счет-Фактуры в " + InvoiceKONTUR + ". Нет доступа или диск переполнен.";
                DispOrders.WriteInvoiceLog(Convert.ToString(PlatInfo[0]) + " - " + Convert.ToString(PlatInfo[1]), Convert.ToString(DelivInfo[0]) + " - " + Convert.ToString(DelivInfo[1]), nameInv, Convert.ToString(CurrDataCOInvoice[6]), 10, message_error, DateTime.Now);
                DispOrders.WriteProtocolEDI("Кор. счет фактура", nameInv, Convert.ToString(PlatInfo[0]) + " - " + Convert.ToString(PlatInfo[1]), 10, DelivInfo[0] + " - " + DelivInfo[1], "Кор. счет фактура не сформирована. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataCOInvoice[6]), "KONTUR");
                Program.WriteLine(message_error);
                //запись в лог о неудаче
            }
        }

    }

    class ReportEDI
    {
        public static void RecordCountEDoc(string Provider, string TypeDoc, int VolumeDoc)//запись количества обработанных документов
        {
            string connString = Settings.Default.ConnStringISPRO;
            string transaction = "begin tran "
                                 + " if exists (select * from U_CHCountEDoc where Provider = '" + Provider + "' and Convert(Date,DD) = Convert(date,GETDATE()) ) "
                                 + " begin      "
                                 + " declare @volume int "
                                 + " set @volume  = (select " + TypeDoc + " from  U_CHCountEDoc where Provider = 'EDI-SOFT' and Convert(Date,DD) = Convert(date,GETDATE())) "
                                 + " update U_CHCountEDoc   set " + TypeDoc + " = ISNULL(@volume,0)+" + VolumeDoc.ToString() + " where  Provider = '" + Provider + "' and Convert(Date,DD) = Convert(date,GETDATE())"
                                 + " end    "
                                 + " else  "
                                 + " begin   "
                                 + " insert into U_CHCountEDoc (Provider, DD, " + TypeDoc + ") values ('" + Provider + "',GETDATE()," + VolumeDoc.ToString() + ") "
                                 + " end commit tran ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(transaction, conn);
            SqlDataReader dr = command.ExecuteReader();
            conn.Close(); ;
        }
    }

    class SentDoc
    {
        public static void SentInvoiceOld()
        {
            string UsedProvider;
            
            int countSF = DispOrders.CountSF();
            object[,] ListSF = DispOrders.GetListSF();//список СФ, LISTSF[i,0] - это используемый провайдер EDI
            for (int i = 0; i < countSF; i++)
            {
                UsedProvider = Convert.ToString(ListSF[i, 0]).Trim(); //получаем имя провайдера
                if (UsedProvider == "EDISOFT")
                {
                    List<object> CurrDataInvoice = new List<object>();
                    for (int j = 0; j < 19; j++)
                    {
                        CurrDataInvoice.Add(ListSF[i, j]);
                    }
                    EDIXMLCreation.CreateEdiInvoice(CurrDataInvoice);
                }
                if (UsedProvider == "KONTUR")
                {
                    List<object> CurrDataInvoice = new List<object>();
                    for (int j = 0; j < 19; j++)
                    {
                        CurrDataInvoice.Add(ListSF[i, j]);
                    }
                    EDIXMLCreation.CreateKonturInvoice(CurrDataInvoice);
                }
            }

        }

        public static void SentInvoice()
        {
            string UsedProvider;

            try
            {
                object[,] ListSF = DispOrders.GetListSF();//список СФ, LISTSF[i,0] - это используемый провайдер EDI
                for (int i = 0; i < ListSF.GetLength(0); i++)
                {
                    UsedProvider = Convert.ToString(ListSF[i, 0]).Trim(); //получаем имя провайдера
                    if (UsedProvider == "EDISOFT")
                    {
                        List<object> CurrDataInvoice = new List<object>();
                        for (int j = 0; j < ListSF.GetLength(1); j++)
                        {
                            CurrDataInvoice.Add(ListSF[i, j]);
                        }
                        try
                        {
                            EDIXMLCreation.CreateEdiInvoice(CurrDataInvoice);
                            CheckSentInv(Convert.ToString(CurrDataInvoice[1]));
                        }
                        catch (Exception err)
                        {
                            DispOrders.WriteErrorLog(err.Message);
                            continue;
                        }
                    }
                    if (UsedProvider == "KONTUR")
                    {
                        List<object> CurrDataInvoice = new List<object>();
                        for (int j = 0; j < ListSF.GetLength(1); j++)
                        {
                            CurrDataInvoice.Add(ListSF[i, j]);
                        }
                        try
                        {
                            EDIXMLCreation.CreateKonturInvoice(CurrDataInvoice);
                            CheckSentInv(Convert.ToString(CurrDataInvoice[1]));
                        }
                        catch (Exception err)
                        {
                            DispOrders.WriteErrorLog(err.Message);
                            continue;
                        }
                    }
                }
            }
            catch (Exception err)
            {
                Program.WriteLine("Ошибка выгрузки INVOICE " + Convert.ToString(err));
            }
            //выгрузка COINVOICE
            Program.WriteLine("Выгрузка COINVOICE");

            object[,] ListCoSF = DispOrders.GetListCOInvoice();//список COINVOICE, 0 ProviderOpt, 1 ProviderZkg, 2 NastDoc_Fmt, 3 SklSf_Rcd, 4 SklSf_TpOtg, 5 SklSfA_RcdCor, 6 PrdZkg_NmrExt, 7 PrdZkg_Rcd, 8 PrdZkg_Dt, 9 SklNk_TDrvNm
            try
            {
                for (int i = 0; i < ListCoSF.GetLength(0); i++)
                {
                    if (ListCoSF[i, 1].ToString() == "KONTUR")
                    {
                        Program.WriteLine("Подготовка к отправке SklSf_Rcd " + Convert.ToString(ListCoSF[i, 3]) + " провайдер заказа " + Convert.ToString(ListCoSF[i, 1]));
                        List<object> CurrDataCoSF = new List<object>();
                        for (int j = 0; j < ListCoSF.GetLength(1); j++) CurrDataCoSF.Add(ListCoSF[i, j]);

                        if (ListCoSF[i, 4].ToString() == "3") //Коректировка
                        {
                            try
                            {
                                EDIXMLCreation.CreateKonturCOInvoice(CurrDataCoSF);
                                CheckSentInv(Convert.ToString(CurrDataCoSF[3]));
                            }
                            catch (Exception err)
                            {
                                DispOrders.WriteErrorLog(err.Message);
                                continue;
                            }
                        }
                    }
                }
            }
            catch (Exception err)
            {
                Program.WriteLine("Ошибка выгрузки COINVOICE " + Convert.ToString(err));
            }
        }

        public static void CheckSentInv(string IsProDoc)
        {
            string connString = Settings.Default.ConnStringISPRO;
            string Insert = "IF ((select COUNT(*) From UFPRV where UF_TblId = 212 and UF_RkRcd = ( SELECT UFR_RkRcd FROM UFRKV WHERE UFR_DbRcd = 212 AND UFR_Id = 'U_OTPR') and UF_TblRcd = " + IsProDoc + ") = 0) " +
                            "INSERT INTO UFPRV (UF_TblId, UF_TblRcd, UF_RkRcd, UF_RkValN, UF_RkValS) values (212, " + IsProDoc + ", ( SELECT UFR_RkRcd FROM UFRKV WHERE UFR_DbRcd = 212 AND UFR_Id = 'U_OTPR'), 1, 1) ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(Insert, conn);
            SqlDataReader dr = command.ExecuteReader();
            conn.Close();
        }

        public static void SentDesadv()
        {
            string UsedProvider;
            //int countDV = DispOrders.CountDesadv();
            object[,] ListDV = DispOrders.GetListDesadv(/*countDV*/);//список desadv, LISTSF[i,0] - это используемый провайдер EDI, номер накладной, рсд накладной, номер заказа(может быть ноль), плательщик, грузополучатель, номер заказа edi 

            for (int i = 0; i < ListDV.GetLength(0); i++)
            /*for (int i = 0; i < countDV; i++)*/
            {
                UsedProvider = Convert.ToString(ListDV[i, 0]).Trim(); //получаем имя провайдера
                if (UsedProvider == "EDISOFT")
                {
                    List<object> CurrDataDV = new List<object>();
                    for (int j = 0; j < 14; j++)
                    {
                        CurrDataDV.Add(ListDV[i, j]);
                    }
                    EDIXMLCreation.CreateEdiDesadv(CurrDataDV);
                }
                if (UsedProvider == "KONTUR")
                {
                    List<object> CurrDataDV = new List<object>();
                    for (int j = 0; j < 14; j++)
                    {
                        CurrDataDV.Add(ListDV[i, j]);
                    }
                    EDIXMLCreation.CreateKonturDesadv(CurrDataDV);
                }
            }
        }

        /*
         * typeFunc = "" - Это УПД с функциями СЧФДОП и СЧФ
         * typeFunc = "ДОП" - Это УПД с функцией ДОП
         * */
        public static void SentUPD(string typeFunc = "")
        {

            //Новый формат
            object[,] ListSF = DispOrders.GetListUPDN(typeFunc);//список УПД, 0 ProviderOpt, 1 ProviderZkg, 2 NastDoc_Fmt, 3 SklSf_Rcd, 4 SklSf_TpOtg, 5 SklSfA_RcdCor, 6 PrdZkg_NmrExt, 7 PrdZkg_Rcd, 8 PrdZkg_Dt, 9 SklNk_TDrvNm

            try
            {

                for (int i = 0; i < ListSF.GetLength(0); i++)
                {
                    Program.WriteLine("Подготовка к отправке SklSf_Rcd " + Convert.ToString(ListSF[i, 3]) + " провайдер заказа " + Convert.ToString(ListSF[i, 1]));
                    if (ListSF[i, 1].ToString() == "EDISOFT")
                    {
                        List<object> CurrDataSF = new List<object>();
                        for (int j = 0; j < ListSF.GetLength(1); j++) CurrDataSF.Add(ListSF[i, j]);
                        if (ListSF[i, 4].ToString() == "0") //УПД
                        {
                            if (ListSF[i, 2].ToString() == "X5")
                            {
                                EDIformat.CreateEdiX5UPD(CurrDataSF, ListSF[i, 10].ToString());
                            }
                            if (ListSF[i, 2].ToString() == "Auchan")
                            {
                                EDIformat.CreateEdiAuchanUPD(CurrDataSF, ListSF[i, 10].ToString());
                            }
                            if (ListSF[i, 2].ToString() == "Lenta")
                            {
                                EDIformat.CreateEdiLenta_UPD(CurrDataSF, ListSF[i, 10].ToString());
                            }
                            if (ListSF[i, 2].ToString() == "Tander")
                            {
                                EDIformat.CreateEdiTander_UPD(CurrDataSF, ListSF[i, 10].ToString());
                            }
                        }
                        if (ListSF[i, 4].ToString() == "3") //УКД
                        {
                            if (ListSF[i, 2].ToString() == "X5")
                            {
                                EDIformat.CreateEdiX5UKD(CurrDataSF);
                            }
                            if (ListSF[i, 2].ToString() == "Auchan")
                            {
                                EDIformat.CreateEdiAuchanUKD(CurrDataSF);
                            }
                            if (ListSF[i, 2].ToString() == "Lenta")
                            {
                                EDIformat.CreateEdiLenta_UKD(CurrDataSF);
                            }
                            if (ListSF[i, 2].ToString() == "Tander")
                            {
                                EDIformat.CreateEdiTanderUKD(CurrDataSF);
                            }
                        }

                    }                    

                }
            }
            catch (Exception err)
            {
                Program.WriteLine("Ошибка выгрузки УПД " + Convert.ToString(err));
            }
        }

        public static void SentUPD_Diadoc()
        {

            //Новый формат
            object[,] ListSF = DispOrders.GetListUPDN("");//список УПД, 0 ProviderOpt, 1 ProviderZkg, 2 NastDoc_Fmt, 3 SklSf_Rcd, 4 SklSf_TpOtg, 5 SklSfA_RcdCor, 6 PrdZkg_NmrExt, 7 PrdZkg_Rcd, 8 PrdZkg_Dt, 9 SklNk_TDrvNm

            try
            {

                for (int i = 0; i < ListSF.GetLength(0); i++)
                {                    
                    
                    if (ListSF[i, 1].ToString() == "KONTUR")
                    {
                        Program.WriteLine("Подготовка к отправке SklSf_Rcd " + Convert.ToString(ListSF[i, 3]) + " провайдер заказа " + Convert.ToString(ListSF[i, 1]));
                        List<object> CurrDataSF = new List<object>();

                        for (int j = 0; j < ListSF.GetLength(1); j++) CurrDataSF.Add(ListSF[i, j]);
                        if (ListSF[i, 4].ToString() == "0") //УПД
                        {
                            if (ListSF[i, 2].ToString().Contains("Base"))
                            {
                                try
                                {
                                    //проверка на признак сводного счета фактоуры
                                    /*DateTime SvodSfDt = Verifiacation.GetEdoSvodDate(ListSF[i, 3].ToString());
                                    if (SvodSfDt != DateTime.MinValue)
                                    {
                                        Console.WriteLine("eto svodnii sf!");

                                    }*/
                                    
                                    SKBKontur.CreateKonturBase_UPD(CurrDataSF);
                                    CheckSentInv(Convert.ToString(CurrDataSF[3]));
                                }
                                catch (Exception err)
                                {
                                    DispOrders.WriteErrorLog(err.Message);
                                    continue;
                                }
                            }
                        }
                        if (ListSF[i, 4].ToString() == "3") //УКД
                        {
                            if (ListSF[i, 2].ToString().Contains("Base"))
                            {
                                try
                                {
                                    SKBKontur.CreateKonturBase_UKD(CurrDataSF);
                                    CheckSentInv(Convert.ToString(CurrDataSF[3]));
                                }
                                catch (Exception err)
                                {
                                    DispOrders.WriteErrorLog(err.Message);
                                    continue;
                                }
                            }

                        }
                    }

                }
            }
            catch (Exception err)
            {
                Program.WriteLine("Ошибка выгрузки УПД " + Convert.ToString(err));
            }
        }

    }

}
