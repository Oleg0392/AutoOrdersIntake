using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.IO;
using System.Configuration;
using System.Diagnostics;
using System.Data.SqlClient;
using System.Data;
using System.Collections;

namespace AutoOrdersIntake
{
    class EDISOFT
    {
        public static void IntakeOrders()
        {
            bool error;
            int i = 0;
            int VolumeDoc = 0;

            string _path = DispOrders.GetValueOption("EDI-СОФТ.ЗАКАЗ");
            string ErrorEDISOFT = DispOrders.GetValueOption("EDI-СОФТ.ОШИБКА");
            string ArchiveEDISOFT = DispOrders.GetValueOption("EDI-СОФТ.АРХИВ");
            string[] files = Directory.GetFiles(_path, "ORDER*.xml");
            XmlDocument doc = new XmlDocument();
            foreach (string parsefile in files)
            {
                error = false;//по умолчанию ошибок нет
                doc.Load(parsefile);
                DispOrders.ClearTmpZkg();//очищаем временную таблицу с заказом от конкретной точки 
                string gln_buyer, gln_delivery, number_order, date_order, date_delivery;//общее поле заказа - gln плательщика, gln грузополучателя, номер заказа, дата заказа, дата отгрузки.
                string EAN, OrderedQuantity, EI, buyer_code;
                string function_code;
                gln_buyer = doc.SelectSingleNode("/Document-Order/Order-Parties/Buyer/ILN").InnerText;
                gln_delivery = doc.SelectSingleNode("/Document-Order/Order-Parties/DeliveryPoint/ILN").InnerText;
                string[] res_verf_buyer = Verifiacation.Verification_gln_buyer(gln_buyer);//верификация покупателя по gln
                string[] res_verf_deliv = Verifiacation.Verification_gln(gln_delivery);////верификация точки доставки по gln
                number_order = doc.SelectSingleNode("/Document-Order/Order-Header/OrderNumber").InnerText;
                //Program.WriteLine("Внешний номер заказа " + number_order);
                date_order = doc.SelectSingleNode("/Document-Order/Order-Header/OrderDate").InnerText;
                date_delivery = doc.SelectSingleNode("/Document-Order/Order-Header/ExpectedDeliveryDate").InnerText;
                function_code = doc.SelectSingleNode("/Document-Order/Order-Header/DocumentFunctionCode").InnerText;
                if(function_code.Trim() == "3") //delete
                {

                }

                string sellerCodeByBuyer;
                try
                {
                    sellerCodeByBuyer = doc.SelectSingleNode("/Document-Order/Order-Parties/Seller/CodeByBuyer").InnerText;
                }
                catch
                {
                    sellerCodeByBuyer = "";
                }

                bool Exists = Verifiacation.CheckExistsOrder(number_order);
                if(Exists == true && function_code == "4")
                {
                    int st;
                    st = Verifiacation.deleteOrder(number_order, date_delivery);
                    if (st == 1) //заказ удален
                    {
                        Exists = false;
                        DispOrders.WriteOrderLog("EDI-Софт", res_verf_buyer[0] + " - " + res_verf_buyer[1], res_verf_deliv[0] + " - " + res_verf_deliv[1], Path.GetFileName(doc.BaseURI), number_order, 201, "Заказ " + number_order + " удален из базы. Будет загружен новый заказ", DateTime.Today, DateTime.Now, 0);
                    }
                    else
                    {
                        DispOrders.WriteOrderLog("EDI-Софт", res_verf_buyer[0] + " - " + res_verf_buyer[1], res_verf_deliv[0] + " - " + res_verf_deliv[1], Path.GetFileName(doc.BaseURI), number_order, 202, "Заказ " + number_order + " изменен. Но загрузка не прошла", DateTime.Today, DateTime.Now, 0);
                    }

                }
                if (function_code.Trim() == "3") //delete
                {
                    int st;
                    st = Verifiacation.deleteOrder(number_order, date_delivery);
                    Exists = true;
                    if (st == 1) //заказ удален
                    {
                        DispOrders.WriteOrderLog("EDI-Софт", res_verf_buyer[0] + " - " + res_verf_buyer[1], res_verf_deliv[0] + " - " + res_verf_deliv[1], Path.GetFileName(doc.BaseURI), number_order, 201, "Заказ " + number_order + " удален из базы. Отмена заказа", DateTime.Today, DateTime.Now, 0);
                    }
                    else
                    {
                        DispOrders.WriteOrderLog("EDI-Софт", res_verf_buyer[0] + " - " + res_verf_buyer[1], res_verf_deliv[0] + " - " + res_verf_deliv[1], Path.GetFileName(doc.BaseURI), number_order, 202, "Заказ " + number_order + " отсутствует в системе", DateTime.Today, DateTime.Now, 0);
                    }
                }
                //bool Exists = false;//test
                if (Exists == false)
                {
                    if (DateTime.Parse(date_delivery) >= DateTime.Now)
                    {
                        if (res_verf_buyer[0] != null)//успешная верификация - такой покупатель есть
                        {
                            if (res_verf_deliv[0] != null)//успешная верификация - такая точка доставки есть
                            {
                                foreach (XmlNode n in doc.SelectNodes("/Document-Order/Order-Lines/Line"))
                                {
                                    EAN = n.ChildNodes[0].SelectSingleNode("EAN").InnerText;
                                    OrderedQuantity = n.ChildNodes[0].SelectSingleNode("OrderedQuantity").InnerText; //количество товара
                                    object[] res_verf_item = Verifiacation.Verification_gtin(EAN);//верификация штрих-кода товара по gtin
                                    if (res_verf_item[0] != null)//нашли товар по gtin и получили по нему все данные -артикул-цена и прочее.
                                    {
                                        if (OrderedQuantity == "0.000")
                                        {
                                            //error = true;
                                            DispOrders.WriteOrderLog("EDI-Софт", res_verf_buyer[0] + " - " + res_verf_buyer[1], res_verf_deliv[0] + " - " + res_verf_deliv[1], Path.GetFileName(doc.BaseURI), number_order, 1, " позиция с нулевым количеством: " + Convert.ToString(res_verf_item[2]) + ". Заказ будет принят без данной позиции!", DateTime.Today, DateTime.Now, 0);
                                            Program.WriteLine("Позиция с нулевым количеством");
                                        } else
                                        {
                                            //получим цену из файла
                                            string PriceOrder = DispOrders.getPriceWithNds(n.ChildNodes[0].SelectSingleNode("OrderedUnitGrossPrice"), n.ChildNodes[0].SelectSingleNode("OrderedUnitNetPrice"), Convert.ToString(res_verf_item[6]));

                                            EI = n.ChildNodes[0].SelectSingleNode("UnitOfMeasure").InnerText;
                                            buyer_code = n.ChildNodes[0].SelectSingleNode("BuyerItemCode").InnerText;
                                            i++;
                                            object[] PriceList = Verifiacation.GetPriceList(res_verf_deliv[0], Convert.ToInt32(res_verf_item[5]));////выдает тольтко одно значение - проверить в понедельник.
                                            DispOrders.RecordToTmpZkg(Convert.ToString(res_verf_buyer[0]), Convert.ToString(res_verf_deliv[0]), date_delivery, Convert.ToString(res_verf_item[1]), Convert.ToString(res_verf_item[4]), OrderedQuantity, date_order, number_order, Convert.ToString(PriceList[0]), Convert.ToInt16(res_verf_item[5]), Path.GetFileName(doc.BaseURI), Convert.ToString(PriceList[1]), "0", PriceOrder);
                                            try
                                            {
                                                DispOrders.CheckBuyerCode(buyer_code, Convert.ToString(res_verf_item[2]), Convert.ToString(res_verf_buyer[0]));
                                            }
                                            catch
                                            {
                                                DispOrders.WriteErrorLog("Ошибка процедуры CheckBuyerCode в EDISOFT");
                                            }
                                        }
                                    }
                                    else
                                    {
                                        //error = true;
                                        DispOrders.WriteOrderLog("EDI-Софт", res_verf_buyer[0] + " - " + res_verf_buyer[1], res_verf_deliv[0] + " - " + res_verf_deliv[1], Path.GetFileName(doc.BaseURI), number_order, 1, " не найден штрих-код: " + EAN + ".Заказ будет принят без данной позиции!", DateTime.Today, DateTime.Now, 0);
                                        Program.WriteLine("Не найден штрих код товара");
                                    }
                                }
                            }
                            else
                            {
                                error = true;
                                DispOrders.WriteOrderLog("EDI-Софт", res_verf_buyer[0] + " - " + res_verf_buyer[1], " ", Path.GetFileName(doc.BaseURI), number_order, 2, "Не найден адрес доставки: " + gln_delivery, DateTime.Today, DateTime.Now, 0);
                                Program.WriteLine("Ошибка- в базе нет такой точки доставки!");
                            }
                        }
                        else
                        {
                            error = true;
                            DispOrders.WriteOrderLog("EDI-Софт", " ", " ", Path.GetFileName(doc.BaseURI), number_order, 3, "Не найден плательщик" + gln_buyer, DateTime.Today, DateTime.Now, 0);
                            Program.WriteLine("Ошибка- в базе нет такого плательщика!");
                        }
                    }
                    else
                    {
                        DispOrders.WriteOrderLog("EDI-Софт", " ", " ", Path.GetFileName(doc.BaseURI), number_order, 5, "неправильная дата доставки" + gln_buyer, DateTime.Today, DateTime.Now, 0);
                        error = true;
                    }
                    //перенос заказов в постоянку
                    if (error == false)
                    {
                        DispOrders.TMPtoPrdZkg(res_verf_buyer, res_verf_deliv, Path.GetFileName(doc.BaseURI), "EDI-Софт", number_order, sellerCodeByBuyer);
                        VolumeDoc++;

                        //Directory.Move(Path.GetFullPath(parsefile), ArchiveEDISOFT + Path.GetFileName(doc.BaseURI));

                        string oldP = Path.GetFullPath(parsefile);
                        string newP = ArchiveEDISOFT + Path.GetFileName(doc.BaseURI);
                        try
                        {
                            Directory.Move(oldP, newP);
                        }
                        catch
                        {
                            string id = Convert.ToString(Guid.NewGuid()); ;
                            string nameInv = "ORDERS_" + id + ".xml";
                            string ReserveNewP = ArchiveEDISOFT + DateTime.Now.ToString("@yyyyMMdd_HHmmss_") + nameInv;
                            Directory.Move(oldP, ReserveNewP);
                        }
                    }
                    else
                    {
                        Program.WriteLine("Файл " + Path.GetFileName(doc.BaseURI) + " содержит ошибки");
                        Program.WriteLine("заказ не принят");
                        Program.WriteLine("---------------");

                        //Directory.Move(Path.GetFullPath(parsefile), ErrorEDISOFT + Path.GetFileName(doc.BaseURI));

                        string oldP = Path.GetFullPath(parsefile);
                        string newP = ErrorEDISOFT + Path.GetFileName(doc.BaseURI);
                        try
                        {
                            Directory.Move(oldP, newP);
                        }
                        catch
                        {
                            string id = Convert.ToString(Guid.NewGuid()); ;
                            string nameInv = "ORDERS_" + id + ".xml";
                            string ReserveNewP = ErrorEDISOFT + DateTime.Now.ToString("@yyyyMMdd_HHmmss_") + nameInv;
                            Directory.Move(oldP, ReserveNewP);
                        }
                    }
                }
                else
                {
                    DispOrders.WriteOrderLog("EDI-Софт", res_verf_buyer[0] + " - " + res_verf_buyer[1], res_verf_deliv[0] + " - " + res_verf_deliv[1], Path.GetFileName(doc.BaseURI), number_order, 102, "Заказ " + number_order + " уже существует в системе. Файл заказа пропущен и перенесен в архив", DateTime.Today, DateTime.Now, 0);
                    //Directory.Move(Path.GetFullPath(parsefile), ErrorEDISOFT + Path.GetFileName(doc.BaseURI));
                    string oldP = Path.GetFullPath(parsefile);
                    string newP = ErrorEDISOFT + Path.GetFileName(doc.BaseURI);
                    try
                    {
                        Directory.Move(oldP, newP);
                    }
                    catch
                    {
                        string id = Convert.ToString(Guid.NewGuid()); ;
                        string nameInv = "ORDERS_" + id + ".xml";
                        string ReserveNewP = ErrorEDISOFT + DateTime.Now.ToString("@yyyyMMdd_HHmmss_") + nameInv;

                        Directory.Move(oldP, ReserveNewP);
                    }
                }
                

            }

            ReportEDI.RecordCountEDoc("EDI-Софт", "Orders", VolumeDoc);
            //Console.WriteLine("Заказ от EDI Soft принят");

        }

        public static void TransferOrders()
        {

            string _path = DispOrders.GetValueOption("EDI-СОФТ.ИМПОРТ");
            string ErrorEdiSoft = DispOrders.GetValueOption("EDI-СОФТ.ОШИБКА");
            string ArchiveEdiSoft = DispOrders.GetValueOption("EDI-СОФТ.АРХИВ");
            string ZakazEdiSoft = DispOrders.GetValueOption("EDI-СОФТ.ЗАКАЗ");

            string[] files = Directory.GetFiles(_path, "ORDER*.xml");
            XmlDocument doc = new XmlDocument();
            foreach (string parsefile in files)
            {

                doc.Load(parsefile);
                string gln_delivery;//gln грузополучателя
                string seller_gln, main_gln_edi;
                bool error = false;

                seller_gln = doc.SelectSingleNode("/Document-Order/Order-Parties/Seller/ILN").InnerText;//поставщик
                gln_delivery = doc.SelectSingleNode("/Document-Order/Order-Parties/DeliveryPoint/ILN").InnerText; //грузополучатель
                main_gln_edi = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN"); //GLN головного предприятия
                string rcd_ptn_deliv = Verifiacation.GetRcdPtn(gln_delivery);//верификация точки доставки по gln

                if (main_gln_edi == seller_gln) //если заказ от головного предприятия
                {
                    //проверим, что грузополучатель указан в качестве получателя через главное предприятие
                    if (!String.IsNullOrWhiteSpace(rcd_ptn_deliv) && rcd_ptn_deliv != "0")
                    {
                        bool UseMasterGLN = Verifiacation.GetUseMasterGln(rcd_ptn_deliv);
                        if (UseMasterGLN) //точка выгружается из головного предприятия
                        {
                            //поместить в заказы
                            string oldP = Path.GetFullPath(parsefile);
                            string newP = ZakazEdiSoft + Path.GetFileName(doc.BaseURI);
                            try
                            {
                                Directory.Move(oldP, newP);
                            }
                            catch
                            {
                                string id = Convert.ToString(Guid.NewGuid()); ;
                                string nameInv = "ORDER_" + id + ".xml";
                                string ReserveNewP = ZakazEdiSoft + nameInv;
                                Directory.Move(oldP, ReserveNewP);
                            }
                        }
                        else error = true;
                    }
                    else error = true;
                }
                else error = true;

                if (error)
                {
                    // поместить в архив
                    string oldP = Path.GetFullPath(parsefile);
                    string newP = ErrorEdiSoft + Path.GetFileName(doc.BaseURI);
                    try
                    {
                        Directory.Move(oldP, newP);
                    }
                    catch
                    {
                        string id = Convert.ToString(Guid.NewGuid()); ;
                        string nameInv = "ORDERS_" + id + ".xml";
                        string ReserveNewP = ErrorEdiSoft + nameInv;
                        Directory.Move(oldP, ReserveNewP);
                    }
                }

            }
        }

        /*
         Метод разбирает уведомления о приемке 
        */
    /*    public static void IntakeRecAdv()
        {
            string typeDoc = "RecAdv";
            string provider = "EDOSOFT";

            string _path = DispOrders.GetValueOption("EDI-СОФТ.УОП");
            string ErrorEDISOFT = DispOrders.GetValueOption("EDI-СОФТ.ОШИБКА");
            string ArchiveEDISOFT = DispOrders.GetValueOption("EDI-СОФТ.АРХИВ");

            string[] files = Directory.GetFiles(_path, "RECADV_*.xml");
            XmlDocument doc = new XmlDocument();
            foreach (string parsefile in files)
            {
                Program.WriteLine("Загрузка файла EDI-Софт " + parsefile);
                doc.Load(parsefile);
                int countRecordInAct = 0; //количество позиций в акте
                //достанем основные переменные (необходимы для записи в таблицу U_MgEdiPrFile)
                string deliveryILN = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Parties/DeliveryPoint/ILN").InnerText; //GLN грузополучателя
                string deliveryName = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Parties/DeliveryPoint/Name").InnerText; //наименование грузополучателя
                string deliveryAddress = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Parties/DeliveryPoint/CityName").InnerText + " " + doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Parties/DeliveryPoint/StreetAndNumber").InnerText; //адрес грузополучателя
                string senderILN = doc.SelectSingleNode("/Document-ReceivingAdvice/Document-Parties/Sender/ILN").InnerText; //GLN отправителя
                string senderName = doc.SelectSingleNode("/Document-ReceivingAdvice/Document-Parties/Sender/Name").InnerText; //наименование отправителя
                string buyerILN = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Parties/Buyer/ILN").InnerText; //GLN плательщика
                string buyerName = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Parties/Buyer/Name").InnerText; //наименование плательщика
                string receivingAdviceNumber = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Header/ReceivingAdviceNumber").InnerText; //Номер RecAdv
                string buyerOrderNumber = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Header/BuyerOrderNumber").InnerText; //Номер заказа

                string[] res_verf_deliv = Verifiacation.Verification_gln(deliveryILN);////верификация точки доставки по gln
                if (res_verf_deliv[0] == null)//успешная верификация - такая точка доставки есть
                {

                    Program.WriteLine("Не найден грузополучатель");
                    DispOrders.WriteEDIProcessedFile(typeDoc, DateTime.Now, receivingAdviceNumber, buyerOrderNumber, Path.GetFileName(parsefile), "Не найден грузополучатель. Документ перенесен в архив", senderILN, senderName, buyerILN, buyerName, deliveryILN, deliveryName, deliveryAddress, provider, 0);
                    //перенести в архив
                    DispOrders.MoveToFolder(parsefile, ErrorEDISOFT);
                    continue; //прекращаем разбор файла, берем следующий
                }


                //Проверка назначен ли данный документ грузополучателю
                DateTime startDate = Verifiacation.Verification_NastDoc("RECADV", res_verf_deliv[3]);
                if (startDate == DateTime.MinValue)
                {
                    Program.WriteLine("Документ RecAdv не назначен данному грузополучателю");
                    DispOrders.WriteEDIProcessedFile(typeDoc, DateTime.Now, receivingAdviceNumber, buyerOrderNumber, Path.GetFileName(parsefile), "Документ RecAdv не назначен данному грузополучателю. Документ перенесен в архив", senderILN, senderName, buyerILN, buyerName, deliveryILN, deliveryName, deliveryAddress, provider, 0);
                    //перенести в архив
                    DispOrders.MoveToFolder(parsefile, ArchiveEDISOFT);
                    continue; //прекращаем разбор файла, берем следующий
                }

                //основные данные шапки
                string receivingAdviceDate = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Header/ReceivingAdviceDate").InnerText; //Дата RecAdv
                string goodsReceiptDate = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Header/GoodsReceiptDate").InnerText; //Дата приемки
                string buyerOrderDate = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Header/BuyerOrderDate").InnerText; //Дата заказа
                string despatchNumber = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Header/DespatchNumber").InnerText; //номер накладной
                //итоги шапки
                string totalLines = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Summary/TotalLines").InnerText; //Всего товарных строк
                string totalGoodsReceiptAmount = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Summary/TotalGoodsReceiptAmount").InnerText; //Всего количество
                string totalNetAmount = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Summary/TotalNetAmount").InnerText; //Сумма без НДС
                string totalGrossAmount = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Summary/TotalGrossAmount").InnerText; //Сумма с НДС
                //привести номер накладной в порядок
                string transformed_despatchNumber = Verifiacation.combDespatchNumber(despatchNumber);
                //Проверка: может мы уже загружали с таким номером, датой? Если загружали, тогда пропустить
                bool existsRecAdv = DispOrders.checkExistsRecAdv(receivingAdviceNumber, buyerOrderNumber);
                if(existsRecAdv)
                {
                    Program.WriteLine("Документ RecAdv с таким номер акта и номером заказа был обработан ранее");
                    DispOrders.WriteEDIProcessedFile(typeDoc, DateTime.Now, receivingAdviceNumber, buyerOrderNumber, Path.GetFileName(parsefile), "Документ RecAdv с таким номер акта и номером заказа был обработан ранее. Документ перенесен в архив", senderILN, senderName, buyerILN, buyerName, deliveryILN, deliveryName, deliveryAddress, provider, 0);
                    //перенести в архив
                    DispOrders.MoveToFolder(parsefile, ArchiveEDISOFT);
                    continue; //прекращаем разбор файла, берем следующий
                }

                if (startDate > DateTime.ParseExact(receivingAdviceDate, "yyyy-MM-dd", null))
                {
                    Program.WriteLine("Дата начала использования документа RecAdv для грузополучателя больше даты документа");
                    DispOrders.WriteEDIProcessedFile(typeDoc, DateTime.Now, receivingAdviceNumber, buyerOrderNumber, Path.GetFileName(parsefile), "Дата начала использования документа RecAdv для грузополучателя больше даты документа. Документ перенесен в архив", senderILN, senderName, buyerILN, buyerName, deliveryILN, deliveryName, deliveryAddress, provider, 0);
                    //перенести в архив
                    DispOrders.MoveToFolder(parsefile, ArchiveEDISOFT);
                    continue; //прекращаем разбор файла, берем следующий
                }

                //достать по номеру заказа расходную накладную, а также позиции, сформированные в ИС-ПРО
                object[,] Item = Verifiacation.GetItemsFromSklnk(buyerOrderNumber, transformed_despatchNumber);
                if (Item.GetLongLength(0) > 0) //разбор только если нашли документ
                {
                    //занесем данные табличной части во временную таблицу, только если есть разница в количестве 
                    foreach (XmlNode n in doc.SelectNodes("Document-ReceivingAdvice/ReceivingAdvice-Lines/Line"))
                    {
                        ArrayList list = new ArrayList();
                        countRecordInAct = countRecordInAct + 1;
                        bool identical = false;
                        string EAN = Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("EAN")); //штрихкод
                        string QuantityReceived = Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("QuantityReceived")); //количество принятое
                        string UnitGrossPrice = Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("UnitGrossPrice")); //Цена с НДС 
                        string SupplierItemCode = Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("SupplierItemCode")); //код товара у покупателя
                        string BuyerItemCode = Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("BuyerItemCode")); //код товара у плательщика
                        string cmt = "Не найден товар по штрихкоду = " + EAN + " ( " + Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("ItemDescription")) + ")";
                        //осуществим поиск в массиве товаров, полученным из ИС-ПРО по штрихкоду
                        for (int k = 0; k < Item.GetLongLength(0); k++) //Item[] //0 BarCode_Code, 1 SklN_Rcd, 2 SklN_Cd, 3 SklN_NmAlt, 4 Кол-во, 5 Цена без НДC, 6 Цена с НДС, 7 Код ЕИ EDI, 8 ОКЕЙ, 9 Ставка, 10 'S', 11 Сумма НДС, 12 Сумма с НДС, 13 шифр ЕИ, 14 Вес
                        {
                            if (Item[k, 0].ToString() == EAN || Item[k, 16].ToString() == SupplierItemCode || Item[k, 17].ToString() == BuyerItemCode) //если штрихкод нашли
                            {
                                //сравниваем по количеству принятого и цене
                                if (Convert.ToDecimal(Item[k, 4].ToString()) == Convert.ToDecimal(QuantityReceived.Replace('.', ',')) && Convert.ToDecimal(Item[k, 6].ToString()) == Convert.ToDecimal(UnitGrossPrice.Replace('.', ',')))
                                {
                                    identical = true; //совпадают
                                    cmt = "";
                                    Item[k, 15] = 0; //отмечаем, что позиция обработана
                                }
                                else
                                {
                                    cmt = "Не совпадают по количеству или цене";
                                    Item[k, 15] = 0; //отмечаем, что позиция обработана
                                }
                                break;
                            }
                        }
                        if (!identical) //если не совпадают
                        {
                            list.Add(receivingAdviceNumber); //номер акта --1
                            list.Add(buyerOrderNumber); //номер заказа  --2
                            list.Add(receivingAdviceDate); //дата акта  --3
                            list.Add(deliveryILN); //ГЛН грузополучателя  --4
                            list.Add(Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("LineNumber"))); //номер строки --5
                            list.Add(EAN); //штрихкод  --6
                            list.Add(BuyerItemCode); //код товара у плательщика --7
                            list.Add(Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("QuantityOrdered"))); //количество заказанное  --8
                            list.Add(QuantityReceived); //количество принятое  --9
                            list.Add(UnitGrossPrice); //Цена с НДС  --10
                            list.Add(Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("UnitNetPrice"))); //Цена без НДС --11
                            list.Add(Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("UnitOfMeasure"))); //Единица измерения : PCE - штука , PA - коробка, KGM - кг, GRM - грамм, PF - палета --12
                            list.Add(SupplierItemCode); //код товара у покупателя --13
                            list.Add(Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("QuantityDamaged"))); //количество поврежденного товара --14
                            list.Add(Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("QuantityUndelivered"))); //количество недопоставленного товара --15
                            list.Add(Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("QuantityOverOrdered"))); //количество сверх заказанного  --16
                            list.Add(cmt); //комментарий  --17

                            DispOrders.InsertTmpD(list); //запись во временную таблицу
                        }


                    }
                    //проверить количество в акте и количество позиций в накладной
                    if (countRecordInAct != Item.GetLongLength(0))
                    {
                        //занесем данные позиции во временную таблицу
                        for (int k = 0; k < Item.GetLongLength(0); k++) //Item[] //0 BarCode_Code, 1 SklN_Rcd, 2 SklN_Cd, 3 SklN_NmAlt, 4 Кол-во, 5 Цена без НДC, 6 Цена с НДС, 7 Код ЕИ EDI, 8 ОКЕЙ, 9 Ставка, 10 'S', 11 Сумма НДС, 12 Сумма с НДС, 13 шифр ЕИ, 14 Вес
                        {
                            if (string.IsNullOrEmpty(Item[k, 15].ToString()))
                            {//0 BarCode_Code, 1 SklN_Rcd, 2 SklN_Cd, 3 SklN_NmAlt, 4 Кол-во, 5 Цена без НДC, 6 Цена с НДС, 7 Код ЕИ EDI, 8 ОКЕЙ, 9 Ставка, 10 'S', 11 Сумма НДС, 12 Сумма с НДС, 13 шифр ЕИ, 14 Вес
                                ArrayList list = new ArrayList();
                                list.Add(receivingAdviceNumber); //номер акта --1
                                list.Add(buyerOrderNumber); //номер заказа  --2
                                list.Add(receivingAdviceDate); //дата акта  --3
                                list.Add(deliveryILN); //ГЛН грузополучателя  --4
                                list.Add(k + 1); //номер строки --5
                                list.Add(Item[k, 0]); //штрихкод  --6
                                list.Add(""); //код товара у плательщика --7
                                list.Add(Item[k, 4]); //количество заказанное  --8
                                list.Add(""); //количество принятое  --9
                                list.Add(Item[k, 6]); //Цена с НДС  --10
                                list.Add(Item[k, 5]); //Цена без НДС --11
                                list.Add(Item[k, 7]); //Единица измерения : PCE - штука , PA - коробка, KGM - кг, GRM - грамм, PF - палета --12
                                list.Add(Item[k, 16]); //код товара у покупателя --13
                                list.Add(""); //количество поврежденного товара --14
                                list.Add(""); //количество недопоставленного товара --15
                                list.Add(""); //количество сверх заказанного  --16
                                list.Add("товар отсутствует в акте"); //комментарий  --17

                                DispOrders.InsertTmpD(list); //запись во временную таблицу    
                            }
                        }
                    }
                }
                //создание recAdv из временной в постоянную
                DispOrders.CreateRecAdv(receivingAdviceDate, receivingAdviceNumber, Convert.ToInt64(buyerILN), Convert.ToInt64(deliveryILN), transformed_despatchNumber, totalGrossAmount, Convert.ToInt16(totalLines), totalNetAmount, buyerOrderDate, buyerOrderNumber, totalGoodsReceiptAmount, goodsReceiptDate);

                DispOrders.WriteEDIProcessedFile(typeDoc, DateTime.Now, receivingAdviceNumber, buyerOrderNumber, Path.GetFileName(parsefile), "", senderILN, senderName, buyerILN, buyerName, deliveryILN, deliveryName, deliveryAddress, provider, 1);
                //перенести в архив
                DispOrders.MoveToFolder(parsefile, ArchiveEDISOFT);

            }

        }*/



        public static void IntakeRecAdv()
        {
            int error;
            string typeDoc = "RecAdv";
            string provider = "EDOSOFT";

            string _path = DispOrders.GetValueOption("EDI-СОФТ.УОП"); //анализируемая папка
            string ErrorEDISOFT = DispOrders.GetValueOption("EDI-СОФТ.ОШИБКА");
            string ArchiveEDISOFT = DispOrders.GetValueOption("EDI-СОФТ.АРХИВ");

            string[] files = Directory.GetFiles(_path, "RECADV_*.xml"); //какие файлы читаем
            XmlDocument doc = new XmlDocument();
            foreach (string parsefile in files)
            {
                error = 0;//по умолчанию ошибок нет
                Program.WriteLine("Загрузка файла EDI-Софт " + parsefile);
                doc.Load(parsefile);
                XmlNode tmpSingleNode;

                //достанем основные переменные (необходимы для записи в таблицу U_MgEdiPrFile)

                tmpSingleNode = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Parties/DeliveryPoint/ILN"); //GLN грузополучателя
                string deliveryILN = string.Empty;
                if (tmpSingleNode != null) deliveryILN = tmpSingleNode.InnerText;

                tmpSingleNode = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Parties/DeliveryPoint/Name");
                string deliveryName = string.Empty; //наименование грузополучателя
                if (tmpSingleNode != null) deliveryName = tmpSingleNode.InnerText;

                tmpSingleNode = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Parties/DeliveryPoint/CityName"); //адрес грузополучателя
                string deliveryAddress = string.Empty;
                if (tmpSingleNode != null) deliveryAddress = tmpSingleNode.InnerText + " ";
                tmpSingleNode = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Parties/DeliveryPoint/StreetAndNumber");
                if (tmpSingleNode != null) deliveryAddress = deliveryAddress + tmpSingleNode.InnerText;

                tmpSingleNode = doc.SelectSingleNode("/Document-ReceivingAdvice/Document-Parties/Sender/ILN");
                string senderILN = string.Empty; //GLN отправителя
                if (tmpSingleNode != null) senderILN = tmpSingleNode.InnerText;

                tmpSingleNode = doc.SelectSingleNode("/Document-ReceivingAdvice/Document-Parties/Sender/Name");
                string senderName = string.Empty; //наименование отправителя
                if (tmpSingleNode != null) senderName = tmpSingleNode.InnerText;

                tmpSingleNode = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Parties/Buyer/ILN");
                string buyerILN = string.Empty; //GLN плательщика
                if (tmpSingleNode != null) buyerILN = tmpSingleNode.InnerText;

                tmpSingleNode = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Parties/Buyer/Name");
                string buyerName = string.Empty; //наименование плательщика
                if (tmpSingleNode != null) buyerName = tmpSingleNode.InnerText;

                tmpSingleNode = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Header/ReceivingAdviceNumber");
                string receivingAdviceNumber = string.Empty;//Номер RecAdv
                if (tmpSingleNode != null) receivingAdviceNumber = tmpSingleNode.InnerText;

                tmpSingleNode = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Header/BuyerOrderNumber");
                string buyerOrderNumber = string.Empty; //Номер заказа
                if (tmpSingleNode != null) buyerOrderNumber = tmpSingleNode.InnerText;

                Program.WriteLine("Номер заказа " + buyerOrderNumber);

                string[] res_verf_deliv = Verifiacation.Verification_gln(deliveryILN);//верификация точки доставки по gln
                if (res_verf_deliv[0] == null)//грузополучатель не найден
                {
                    error++;
                    Program.WriteLine("Не найден грузополучатель");
                    DispOrders.WriteEDIProcessedFile(typeDoc, DateTime.Now, receivingAdviceNumber, buyerOrderNumber, Path.GetFileName(parsefile), "Не найден грузополучатель. Документ перенесен в архив", senderILN, senderName, buyerILN, buyerName, deliveryILN, deliveryName, deliveryAddress, provider, 0);
                    //перенести в архив
                    DispOrders.MoveToFolder(parsefile, ErrorEDISOFT);
                    continue; //прекращаем разбор файла, берем следующий
                }

                //Проверка назначен ли данный документ грузополучателю
                DateTime startDate = Verifiacation.Verification_NastDoc("RECADV", res_verf_deliv[3]);
                if (startDate == DateTime.MinValue)
                {
                    error++;
                    Program.WriteLine("Документ RecAdv не назначен данному грузополучателю");
                    DispOrders.WriteEDIProcessedFile(typeDoc, DateTime.Now, receivingAdviceNumber, buyerOrderNumber, Path.GetFileName(parsefile), "Документ RecAdv не назначен данному грузополучателю. Документ перенесен в архив", senderILN, senderName, buyerILN, buyerName, deliveryILN, deliveryName, deliveryAddress, provider, 0);
                    //перенести в архив
                    DispOrders.MoveToFolder(parsefile, ArchiveEDISOFT);
                    continue; //прекращаем разбор файла, берем следующий
                }

                //основные данные шапки

                tmpSingleNode = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Header/ReceivingAdviceDate");
                string receivingAdviceDate = string.Empty; //Дата RecAdv
                if (tmpSingleNode != null) receivingAdviceDate = tmpSingleNode.InnerText;

                tmpSingleNode = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Header/GoodsReceiptDate");
                string goodsReceiptDate = string.Empty; //Дата приемки
                if (tmpSingleNode != null) goodsReceiptDate = tmpSingleNode.InnerText;

                tmpSingleNode = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Header/BuyerOrderDate"); //Дата заказа
                string buyerOrderDate = string.Empty; //Дата заказа
                if (tmpSingleNode != null)
                {
                    buyerOrderDate = tmpSingleNode.InnerText;
                }
                else
                {
                    buyerOrderDate = "1900-01-01";
                }

                tmpSingleNode = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Header/DespatchNumber"); //номер накладной
                string despatchNumber = string.Empty;  //номер накладной
                if (tmpSingleNode != null)
                {
                    despatchNumber = tmpSingleNode.InnerText;
                    Program.WriteLine("Номер накладной " + despatchNumber);
                }
                else
                {
                    error++;
                    Program.WriteLine("В файле не указан номер накладной!!!");
                    DispOrders.WriteEDIProcessedFile(typeDoc, DateTime.Now, receivingAdviceNumber, buyerOrderNumber, Path.GetFileName(parsefile), "Не указан номер накладной. Документ перенесен в архив", senderILN, senderName, buyerILN, buyerName, deliveryILN, deliveryName, deliveryAddress, provider, 0);
                    //перенести в архив
                    DispOrders.MoveToFolder(parsefile, ErrorEDISOFT);
                    continue; //прекращаем разбор файла, берем следующий
                }
                //итоги шапки
                tmpSingleNode = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Summary/TotalLines");
                string totalLines = "0"; //Всего товарных строк
                if (tmpSingleNode != null) totalLines = tmpSingleNode.InnerText;

                Program.WriteLine("Количество товарных строк " + totalLines);

                tmpSingleNode = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Summary/TotalGoodsReceiptAmount");
                string totalGoodsReceiptAmount = "0"; //Всего количество
                if (tmpSingleNode != null) totalGoodsReceiptAmount = tmpSingleNode.InnerText;

                tmpSingleNode = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Summary/TotalNetAmount");
                string totalNetAmount = "0"; //Сумма без НДС
                if (tmpSingleNode != null) totalNetAmount = tmpSingleNode.InnerText;

                tmpSingleNode = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Summary/TotalGrossAmount");
                string totalGrossAmount = "0"; //Сумма с НДС
                if (tmpSingleNode != null) totalGrossAmount = tmpSingleNode.InnerText;

                Program.WriteLine("Сумма с НДС " + totalGrossAmount);

                //привести номер накладной в порядок
                string transformed_despatchNumber = Verifiacation.combDespatchNumber(despatchNumber);
                //Проверка: может мы уже загружали с таким номером, датой? Если загружали, тогда пропустить
                bool existsRecAdv = DispOrders.checkExistsRecAdv(receivingAdviceNumber, buyerOrderNumber);
                if (existsRecAdv)
                {
                    Program.WriteLine("Документ RecAdv с таким номер акта и номером заказа был обработан ранее");
                    DispOrders.WriteEDIProcessedFile(typeDoc, DateTime.Now, receivingAdviceNumber, buyerOrderNumber, Path.GetFileName(parsefile), "Документ RecAdv с таким номер акта и номером заказа был обработан ранее. Документ перенесен в архив", senderILN, senderName, buyerILN, buyerName, deliveryILN, deliveryName, deliveryAddress, provider, 0);
                    //перенести в архив
                    DispOrders.MoveToFolder(parsefile, ArchiveEDISOFT);
                    continue; //прекращаем разбор файла, берем следующий
                }

                if (startDate > DateTime.ParseExact(receivingAdviceDate, "yyyy-MM-dd", null))
                {
                    Program.WriteLine("Дата начала использования документа RecAdv для грузополучателя больше даты документа");
                    DispOrders.WriteEDIProcessedFile(typeDoc, DateTime.Now, receivingAdviceNumber, buyerOrderNumber, Path.GetFileName(parsefile), "Дата начала использования документа RecAdv для грузополучателя больше даты документа. Документ перенесен в архив", senderILN, senderName, buyerILN, buyerName, deliveryILN, deliveryName, deliveryAddress, provider, 0);
                    //перенести в архив
                    DispOrders.MoveToFolder(parsefile, ArchiveEDISOFT);
                    continue; //прекращаем разбор файла, берем следующий
                }

                if (error == 0)
                {
                    Program.WriteLine("Разбор спецификации");

                    //обработать xml файл
                    //занесем данные табличной части в переменную table
                    List<List<string>> table = new List<List<string>>();
                    foreach (XmlNode n in doc.SelectNodes("Document-ReceivingAdvice/ReceivingAdvice-Lines/Line"))
                    {
                        List<string> row = new List<string>();
                        string EAN = Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("EAN")); //штрихкод
                        string QuantityReceived = Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("QuantityReceived")); //количество принятое
                        string UnitGrossPrice = Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("UnitGrossPrice")); //Цена с НДС 
                        string SupplierItemCode = Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("SupplierItemCode")); //код товара у покупателя
                        string BuyerItemCode = Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("BuyerItemCode")); //код товара у плательщика
                        row.Add(receivingAdviceNumber); //номер акта --1
                        row.Add(buyerOrderNumber); //номер заказа  --2
                        row.Add(receivingAdviceDate); //дата акта  --3
                        row.Add(deliveryILN); //ГЛН грузополучателя  --4
                        row.Add(Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("LineNumber"))); //номер строки --5
                        row.Add(EAN); //штрихкод  --6
                        row.Add(BuyerItemCode); //код товара у плательщика --7
                        row.Add(Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("QuantityOrdered"))); //количество заказанное  --8
                        row.Add(QuantityReceived); //количество принятое  --9
                        row.Add(UnitGrossPrice); //Цена с НДС  --10
                        row.Add(Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("UnitNetPrice"))); //Цена без НДС --11
                        row.Add(Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("UnitOfMeasure"))); //Единица измерения : PCE - штука , PA - коробка, KGM - кг, GRM - грамм, PF - палета --12
                        row.Add(SupplierItemCode); //код товара у покупателя --13
                        row.Add(Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("QuantityDamaged"))); //количество поврежденного товара --14
                        row.Add(Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("QuantityUndelivered"))); //количество недопоставленного товара --15
                        row.Add(Verifiacation.getInnerTextforXmlNode(n.ChildNodes[0].SelectSingleNode("QuantityOverOrdered"))); //количество сверх заказанного  --16
                        row.Add(""); //комментарий  --17
                        table.Add(row);
                    }
                    //очистим временную таблицу
                    DispOrders.DeleteTmpDet();
                    //перенесем данные из переменной table во временную таблицу
                    DispOrders.InsertTmpDet(table); //запись во временную таблицу   
                                                    //сравнить расходную накладную и акт, разницу отразить в таблице U_MGEDITMPDOC
                    string cmt = "";
                    int status = DispOrders.CompareActAndInvoice(buyerOrderNumber, despatchNumber, ref cmt);
                    //создание recAdv из временной в постоянную
                    DispOrders.CreateRecAdv(receivingAdviceDate, receivingAdviceNumber, Convert.ToInt64(buyerILN), Convert.ToInt64(deliveryILN), transformed_despatchNumber, totalGrossAmount, Convert.ToInt16(totalLines), totalNetAmount, buyerOrderDate, buyerOrderNumber, totalGoodsReceiptAmount, goodsReceiptDate, cmt);
                    DispOrders.WriteEDIProcessedFile(typeDoc, DateTime.Now, receivingAdviceNumber, buyerOrderNumber, Path.GetFileName(parsefile), cmt, senderILN, senderName, buyerILN, buyerName, deliveryILN, deliveryName, deliveryAddress, provider, status);
                    //перенести в архив
                    DispOrders.MoveToFolder(parsefile, ArchiveEDISOFT);
                }
            }
        }
    }
}
