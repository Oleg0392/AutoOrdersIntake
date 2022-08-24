using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Xml;
using System.IO;
using System.Configuration;
using System.Diagnostics;
using System.Data.SqlClient;
using System.Data;

namespace AutoOrdersIntake
{
    class SKBKontur
    {
        public static void IntakeOrders()
        {
            bool error;
            int i = 0;
            int VolumeDoc = 0;
            string number_order;

            string _path = DispOrders.GetValueOption("СКБ-КОНТУР.ЗАКАЗ");
            string ErrorSKBKontur = DispOrders.GetValueOption("СКБ-КОНТУР.ОШИБКА");
            string ArchiveSKBKontur = DispOrders.GetValueOption("СКБ-КОНТУР.АРХИВ");

            string[] files = Directory.GetFiles(_path, "ORDERS*.xml");
            XmlDocument doc = new XmlDocument();
            foreach (string parsefile in files)
            {
                string EI;
                error = false;//по умолчанию ошибок нет
                doc.Load(parsefile);
                DispOrders.ClearTmpZkg();//очищаем временную таблицу с заказом от конкретной точки 
                string gln_buyer, gln_delivery, date_order, date_delivery, status;//общее поле заказа - gln плательщика, gln грузополучателя, номер заказа, дата заказа, дата отгрузки, комментарий.
                string gtin, quantity, buyer_code;
                //int typeSkln = 5;

                try
                {
                    status = doc.SelectSingleNode("/eDIMessage/order").Attributes["status"].Value;
                }
                catch
                {
                    status = "0";
                }

                number_order = doc.SelectSingleNode("/eDIMessage/order").Attributes["number"].Value;
                string contains_init = number_order.Substring(number_order.Length-4);
                if (contains_init == "init" || contains_init == "INIT")
                {
                    string oldDS = Path.GetFullPath(parsefile);
                    string newDS = ArchiveSKBKontur + Path.GetFileName(doc.BaseURI);
                    try
                    {
                        Directory.Move(oldDS, newDS);
                    }
                    catch
                    {
                        string id = Convert.ToString(Guid.NewGuid()); ;
                        string nameInv = "ORDERS_" + id + ".xml";//потом раскоментировать
                        string ReserveNewD = ArchiveSKBKontur + nameInv;
                        Directory.Move(oldDS, ReserveNewD);
                    }
                }
                else
                {
                    gln_buyer = doc.SelectSingleNode("/eDIMessage/order/buyer/gln").InnerText;
                    gln_delivery = doc.SelectSingleNode("/eDIMessage/order/deliveryInfo/shipTo/gln").InnerText;
                    date_order = doc.SelectSingleNode("/eDIMessage/order").Attributes["date"].Value;
                    date_delivery = doc.SelectSingleNode("/eDIMessage/order/deliveryInfo/requestedDeliveryDateTime").InnerText;
                    date_delivery = date_delivery.Remove(10);
                    string[] res_verf_deliv = Verifiacation.Verification_gln(gln_delivery);////верификация точки доставки по gln
                    string[] res_verf_buyer = Verifiacation.Verification_gln_buyer(gln_buyer);//верификация покупателя по gln


                    switch (status)
                    {
                        case "Canceled":
                            bool del = DispOrders.DeleteOrder(number_order);
                            if (del == true)
                            {
                                DispOrders.WriteOrderLog("СКБ-Контур", res_verf_buyer[0] + " - " + res_verf_buyer[1], res_verf_deliv[0] + " - " + res_verf_deliv[1], Path.GetFileName(doc.BaseURI), number_order, 100, "заказ: " + number_order + "отменен покупателем. Заказ удален из журнала заказов", DateTime.Today, DateTime.Now, 0);
                            }
                            else
                            {
                                DispOrders.WriteOrderLog("СКБ-Контур", res_verf_buyer[0] + " - " + res_verf_buyer[1], res_verf_deliv[0] + " - " + res_verf_deliv[1], Path.GetFileName(doc.BaseURI), number_order, 101, "заказ: " + number_order + "отменен покупателем. Заказ не удален, т.к. создана накладная!", DateTime.Today, DateTime.Now, 0);
                            }

                            string oldD = Path.GetFullPath(parsefile);
                            string newD = ArchiveSKBKontur + Path.GetFileName(doc.BaseURI);
                            try
                            {
                                Directory.Move(oldD, newD);
                            }
                            catch
                            {
                                string id = Convert.ToString(Guid.NewGuid()); ;
                                string nameInv = "ORDERS_" + id + ".xml";//потом раскоментировать
                                string ReserveNewD = ArchiveSKBKontur + nameInv;
                                Directory.Move(oldD, ReserveNewD);
                            }
                            break;
                        default:


                            bool Exists = Verifiacation.CheckExistsOrder(number_order);
                            //bool Exists = false;//test
                            if (Exists == false)//заказ не существует
                            {
                                if (DateTime.Parse(date_delivery) >= DateTime.Now)
                                {
                                    if (res_verf_buyer[0] != null)//успешная верификация - такой покупатель есть 
                                    {
                                        if (res_verf_deliv[0] != null)//успешная верификация - такая точка доставки есть
                                        {
                                            XmlNode firstItemNode = doc.SelectSingleNode("/eDIMessage/order/lineItems/lineItem");  // проверка на отсутствие прейскуранта по мороженному
                                            object[] firstItem = Verifiacation.Verification_gtin(firstItemNode.SelectSingleNode("gtin").InnerText);
                                            object[] checkPreiskurant = Verifiacation.GetPriceList(res_verf_deliv[0], Convert.ToInt32(firstItem[5]));
                                            if (checkPreiskurant[0].ToString().Equals("56") && checkPreiskurant[1].ToString().Equals("226"))
                                            {
                                                DispOrders.WriteErrorLog("Отсутствует прейскурант по мороженному! У контрагента с кодом: " + res_verf_deliv[0]);
                                                Program.WriteLine("СКБ-Контур, [Ошибка]: Отсутствует прейскурант по мороженному! У контрагента с кодом: " + res_verf_deliv[0]);
                                                continue;
                                            }
                                            foreach (XmlNode n in doc.SelectNodes("/eDIMessage/order/lineItems/lineItem"))//проход по позициям заказа
                                            {
                                                //EI = n.ChildNodes[3].Attributes["unitOfMeasure"].Value;//единицы измерения из заказа!
                                                EI = n.SelectSingleNode("requestedQuantity").Attributes["unitOfMeasure"].Value;
                                                //gtin = n.ChildNodes[0].InnerText;
                                                gtin = n.SelectSingleNode("gtin").InnerText;
                                                buyer_code = n.SelectSingleNode("internalBuyerCode").InnerText;
                                                object[] res_verf_item = Verifiacation.Verification_gtin(gtin);//верификация штрих-кода товара по gtin
                                                quantity = n.SelectSingleNode("requestedQuantity").InnerText;
                                                if (res_verf_item[0] != null)//нашли товар по gtin и получили по нему все данные -артикул-цена и прочее.
                                                {
                                                    if (quantity == "0.000")
                                                    {
                                                        //error = true;
                                                        //DispOrders.WriteOrderLog("EDI-Софт", res_verf_buyer[0] + " - " + res_verf_buyer[1], res_verf_deliv[0] + " - " + res_verf_deliv[1], Path.GetFileName(doc.BaseURI), number_order, 1, " позиция с нулевым количеством: " + Convert.ToString(res_verf_item[2]) + ". Заказ будет принят без данной позиции!", DateTime.Today, DateTime.Now, 0);
                                                        DispOrders.WriteOrderLog("СКБ-Контур", res_verf_buyer[0] + " - " + res_verf_buyer[1], res_verf_deliv[0] + " - " + res_verf_deliv[1], Path.GetFileName(doc.BaseURI), number_order, 1, " позиция с нулевым количеством: " + Convert.ToString(res_verf_item[2]) + ". Заказ будет принят без данной позиции!", DateTime.Today, DateTime.Now, 0);
                                                        Program.WriteLine("Позиция с нулевым количеством");
                                                    }
                                                    else
                                                    {
                                                        //получим цену из файла
                                                        string PriceOrder = DispOrders.getPriceWithNds(n.SelectSingleNode("netPriceWithVAT"), n.SelectSingleNode("netPrice"), Convert.ToString(res_verf_item[6]));

                                                        //quantity = n.SelectSingleNode("requestedQuantity").InnerText;
                                                        i++;
                                                        object[] PriceList = Verifiacation.GetPriceList(res_verf_deliv[0], Convert.ToInt32(res_verf_item[5])); 
                                                        DispOrders.RecordToTmpZkg(Convert.ToString(res_verf_buyer[0]), Convert.ToString(res_verf_deliv[0]), date_delivery, Convert.ToString(res_verf_item[1]), Convert.ToString(res_verf_item[4]), quantity, date_order, number_order, Convert.ToString(PriceList[0]), Convert.ToInt16(res_verf_item[5]), Path.GetFileName(doc.BaseURI), Convert.ToString(PriceList[1]), "0", PriceOrder);
                                                        try
                                                        {
                                                            DispOrders.CheckBuyerCode(buyer_code, Convert.ToString(res_verf_item[2]), Convert.ToString(res_verf_buyer[0]));
                                                        }
                                                        catch
                                                        {
                                                            DispOrders.WriteErrorLog("Ошибка процедуры CheckBuyerCode в СКБ-Контур. Buyer code: " + buyer_code + ", товар: " + res_verf_item[2] + ", PtnCd: " + res_verf_buyer[0]);
                                                        }
                                                    }
                                                }
                                                else
                                                {
                                                    //error = true;
                                                    DispOrders.WriteOrderLog("СКБ-Контур", res_verf_buyer[0] + " - " + res_verf_buyer[1], res_verf_deliv[0] + " - " + res_verf_deliv[1], Path.GetFileName(doc.BaseURI), number_order, 1, "не найден штрих-код: " + gtin + ". Заказ будет принят без данной позиции!", DateTime.Today, DateTime.Now, 0);
                                                    Program.WriteLine("не найден штрих-код: " + gtin);
                                                    //DispOrders.ClearTmpZkg();
                                                }
                                            }
                                        }
                                        else
                                        {
                                            error = true;
                                            DispOrders.WriteOrderLog("СКБ-Контур", res_verf_buyer[0] + " - " + res_verf_buyer[1], " ", Path.GetFileName(doc.BaseURI), number_order, 2, "Не найден адрес доставки: " + gln_delivery, DateTime.Today, DateTime.Now, 0);
                                            Program.WriteLine("Не найден адрес доставки: " + gln_delivery);
                                            DispOrders.ClearTmpZkg();

                                        }
                                    }
                                    else
                                    {
                                        error = true;
                                        DispOrders.WriteOrderLog("СКБ-Контур", " ", " ", Path.GetFileName(doc.BaseURI), number_order, 3, "Не найден плательщик" + gln_buyer, DateTime.Today, DateTime.Now, 0);
                                        Program.WriteLine("Не найден плательщик " + gln_buyer);
                                        DispOrders.ClearTmpZkg();
                                    }
                                }
                                else
                                {
                                    DispOrders.WriteOrderLog("СКБ-Контур", " ", " ", Path.GetFileName(doc.BaseURI), number_order, 5, "неправильная дата доставки" + gln_buyer, DateTime.Today, DateTime.Now, 0);
                                    error = true;
                                }
                                //перенос заказов в постоянку
                                if (error == false)
                                {
                                    
                                    DispOrders.TMPtoPrdZkg(res_verf_buyer, res_verf_deliv, Path.GetFileName(doc.BaseURI), "СКБ-Контур", number_order);
                                    VolumeDoc++;

                                    string oldP = Path.GetFullPath(parsefile);
                                    string newP = ArchiveSKBKontur + Path.GetFileName(doc.BaseURI);
                                    try
                                    {
                                        Directory.Move(oldP, newP);
                                    }
                                    catch
                                    {
                                        string id = Convert.ToString(Guid.NewGuid()); ;
                                        string nameInv = "ORDERS_" + id + ".xml";//потом раскоментировать
                                        string ReserveNewP = ArchiveSKBKontur + nameInv;
                                        Directory.Move(oldP, ReserveNewP);
                                    }
                                }
                                else
                                {
                                    Program.WriteLine("Файл " + Path.GetFileName(doc.BaseURI) + " содержит ошибки");
                                    Program.WriteLine("заказ не принят");
                                    Program.WriteLine("---------------");

                                    string oldPe = Path.GetFullPath(parsefile);
                                    string newPe = ErrorSKBKontur + Path.GetFileName(doc.BaseURI);
                                    try
                                    {
                                        Directory.Move(oldPe, newPe);
                                    }
                                    catch
                                    {
                                        string id = Convert.ToString(Guid.NewGuid()); ;
                                        string nameInv = "ORDERS_" + id + ".xml";
                                        string ReserveNewPe = ErrorSKBKontur + nameInv;

                                        Directory.Move(oldPe, ReserveNewPe);
                                    }


                                }

                            }
                            else
                            {
                                DispOrders.WriteOrderLog("СКБ-Контур", res_verf_buyer[0] + " - " + res_verf_buyer[1], res_verf_deliv[0] + " - " + res_verf_deliv[1], Path.GetFileName(doc.BaseURI), number_order, 102, "заказ: " + number_order + " уже существует в системе!. Файл заказа пропущен и перенесен в архив", DateTime.Today, DateTime.Now, 0);
                                string oldP = Path.GetFullPath(parsefile);
                                string newP = ArchiveSKBKontur + Path.GetFileName(doc.BaseURI);
                                try
                                {
                                    Directory.Move(oldP, newP);
                                }
                                catch
                                {
                                    string id = Convert.ToString(Guid.NewGuid()); ;
                                    string nameInv = "ORDERS_" + id + ".xml";
                                    string ReserveNewP = ArchiveSKBKontur + nameInv;
                                    Directory.Move(oldP, ReserveNewP);
                                }
                            }
                            break;
                    }
                }
            }
            ReportEDI.RecordCountEDoc("СКБ-Контур", "Orders", VolumeDoc);
            //Program.WriteLine("Прием заказов от СКБ закончен");
            //Console.ReadLine();
        }


        public static void TransferOrders()
        {

            string _path = DispOrders.GetValueOption("СКБ-КОНТУР.ИМПОРТ");
            string ErrorSKBKontur = DispOrders.GetValueOption("СКБ-КОНТУР.ОШИБКА");
            string ArchiveSKBKontur = DispOrders.GetValueOption("СКБ-КОНТУР.АРХИВ");
            string ZakazSKBKontur = DispOrders.GetValueOption("СКБ-КОНТУР.ЗАКАЗ");

            string[] files = Directory.GetFiles(_path, "ORDERS*.xml");
            XmlDocument doc = new XmlDocument();
            foreach (string parsefile in files)
            {
                
                doc.Load(parsefile);
                string gln_delivery;//gln грузополучателя
                string seller_gln, main_gln_edi;
                bool error = false;
               
                seller_gln = doc.SelectSingleNode("/eDIMessage/order/seller/gln").InnerText; //поставщик
                gln_delivery = doc.SelectSingleNode("/eDIMessage/order/deliveryInfo/shipTo/gln").InnerText; //грузополучатель
                main_gln_edi = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN"); //GLN головного предприятия
                string rcd_ptn_deliv = Verifiacation.GetRcdPtn(gln_delivery);//верификация точки доставки по gln

                if (main_gln_edi == seller_gln ) //если заказ от головного предприятия
                {
                    //проверим, что грузополучатель указан в качестве получателя через главное предприятие
                    if (!String.IsNullOrWhiteSpace(rcd_ptn_deliv) && rcd_ptn_deliv != "0")
                    {
                        bool UseMasterGLN = Verifiacation.GetUseMasterGln(rcd_ptn_deliv);
                        if (UseMasterGLN) //точка выгружается из головного предприятия
                        {
                            //поместить в заказы
                            string oldP = Path.GetFullPath(parsefile);
                            string newP = ZakazSKBKontur + Path.GetFileName(doc.BaseURI);
                            try
                            {
                                Directory.Move(oldP, newP);
                            }
                            catch
                            {
                                string id = Convert.ToString(Guid.NewGuid()); ;
                                string nameInv = "ORDERS_" + id + ".xml";
                                string ReserveNewP = ZakazSKBKontur + nameInv;
                                Directory.Move(oldP, ReserveNewP);
                            }
                        }
                        else error = true;       
                    }
                    else error = true;
                }
                else  error = true;

                if (error)
                {
                    // поместить в архив
                    string oldP = Path.GetFullPath(parsefile);
                    string newP = ErrorSKBKontur + Path.GetFileName(doc.BaseURI);
                    try
                    {
                        Directory.Move(oldP, newP);
                    }
                    catch
                    {
                        string id = Convert.ToString(Guid.NewGuid()); ;
                        string nameInv = "ORDERS_" + id + ".xml";
                        string ReserveNewP = ErrorSKBKontur + nameInv;
                        Directory.Move(oldP, ReserveNewP);
                    }
                }
            }
        }


        public static void IntakeRecAdv()
        {
            string typeDoc = "RecAdv";
            string provider = "KONTUR";

            string _path = DispOrders.GetValueOption("СКБ-КОНТУР.УОП"); //анализируемая папка
            string ErrorPath = DispOrders.GetValueOption("СКБ-КОНТУР.ОШИБКА");
            string ArchivePath = DispOrders.GetValueOption("СКБ-КОНТУР.АРХИВ");

            string[] files = Directory.GetFiles(_path, "RECADV_*.xml"); //какие файлы читаем
            XmlDocument doc = new XmlDocument();
            foreach (string parsefile in files)
            {
                Program.WriteLine("Загрузка файла СКБ-КОНТУР " + parsefile);
                doc.Load(parsefile);
                //достанем основные переменные (необходимы для записи в таблицу U_MgEdiPrFile)
                string deliveryILN = doc.SelectSingleNode("/eDIMessage/receivingAdvice/deliveryInfo/shipTo/gln").InnerText; //GLN грузополучателя
                string deliveryName = "";
                if (doc.SelectSingleNode("/eDIMessage/receivingAdvice/deliveryInfo/shipTo/organization/name") != null) //наименование грузополучателя
                {
                    deliveryName = doc.SelectSingleNode("/eDIMessage/receivingAdvice/deliveryInfo/shipTo/organization/name").InnerText;
                }
                string deliveryAddress = "";
                if (doc.SelectSingleNode("/eDIMessage/receivingAdvice/deliveryInfo/shipTo/organization/russianAddress/city") != null) //адрес грузополучателя - город
                {
                    deliveryAddress = doc.SelectSingleNode("/eDIMessage/receivingAdvice/deliveryInfo/shipTo/organization/russianAddress/city").InnerText;
                }
                if (doc.SelectSingleNode("/eDIMessage/receivingAdvice/deliveryInfo/shipTo/organization/russianAddress/street") != null) //адрес грузополучателя - улица
                {
                    deliveryAddress = deliveryAddress + " " + doc.SelectSingleNode("/eDIMessage/deliveryInfo/shipTo/organization/russianAddress/street").InnerText;
                }
                if (doc.SelectSingleNode("/eDIMessage/receivingAdvice/deliveryInfo/shipTo/organization/russianAddress/house") != null) //адрес грузополучателя - номер улицы
                {
                    deliveryAddress = deliveryAddress + " " + doc.SelectSingleNode("/eDIMessage/receivingAdvice/deliveryInfo/shipTo/organization/russianAddress/house").InnerText;
                }
                string senderILN = doc.SelectSingleNode("/eDIMessage/receivingAdvice/seller/gln").InnerText; //GLN отправителя
                string senderName = "";//наименование отправителя
                string buyerName = "";//наименование плательщика
                string buyerOrderNumber = ""; //номер заказа
                if (doc.SelectSingleNode("/eDIMessage/receivingAdvice/seller/organization/name") != null)
                    senderName = doc.SelectSingleNode("/eDIMessage/receivingAdvice/seller/organization/name").InnerText;
                string buyerILN = doc.SelectSingleNode("/eDIMessage/receivingAdvice/buyer/gln").InnerText; //GLN плательщика
                if (doc.SelectSingleNode("/eDIMessage/receivingAdvice/buyer/organization/name") != null)
                    buyerName = doc.SelectSingleNode("/eDIMessage/receivingAdvice/buyer/organization/name").InnerText; //наименование плательщика
                string receivingAdviceNumber = doc.SelectSingleNode("/eDIMessage/receivingAdvice").Attributes["number"].Value; //Номер RecAdv
                if (doc.SelectSingleNode("/eDIMessage/receivingAdvice/blanketOrderIdentificator") != null && doc.SelectSingleNode("/eDIMessage/receivingAdvice/originOrder") != null)
                    buyerOrderNumber = doc.SelectSingleNode("/eDIMessage/receivingAdvice/blanketOrderIdentificator").Attributes["number"].Value +"." +doc.SelectSingleNode("/eDIMessage/receivingAdvice/originOrder").Attributes["number"].Value; //Номер заказа
                else if (doc.SelectSingleNode("/eDIMessage/receivingAdvice/originOrder") != null)
                    buyerOrderNumber = doc.SelectSingleNode("/eDIMessage/receivingAdvice/originOrder").Attributes["number"].Value; //Номер заказа

                string[] res_verf_deliv = Verifiacation.Verification_gln(deliveryILN);////верификация точки доставки по gln
                if (res_verf_deliv[0] == null)//успешная верификация - такая точка доставки есть
                {
                    Program.WriteLine("Не найден грузополучатель");
                    DispOrders.WriteEDIProcessedFile(typeDoc, DateTime.Now, receivingAdviceNumber, buyerOrderNumber, Path.GetFileName(parsefile), "Не найден грузополучатель. Документ перенесен в архив", senderILN, senderName, buyerILN, buyerName, deliveryILN, deliveryName, deliveryAddress, provider, 0);
                    //перенести в архив
                    DispOrders.MoveToFolder(parsefile, ErrorPath);
                    continue; //прекращаем разбор файла, берем следующий
                }

                //Проверка назначен ли данный документ грузополучателю
                DateTime startDate = Verifiacation.Verification_NastDoc("RECADV", res_verf_deliv[3]);
                if (startDate == DateTime.MinValue)
                {
                    Program.WriteLine("Документ RecAdv не назначен данному грузополучателю");
                    DispOrders.WriteEDIProcessedFile(typeDoc, DateTime.Now, receivingAdviceNumber, buyerOrderNumber, Path.GetFileName(parsefile), "Документ RecAdv не назначен данному грузополучателю. Документ перенесен в архив", senderILN, senderName, buyerILN, buyerName, deliveryILN, deliveryName, deliveryAddress, provider, 0);
                    //перенести в архив
                    DispOrders.MoveToFolder(parsefile, ArchivePath);
                    continue; //прекращаем разбор файла, берем следующий
                }

                //основные данные шапки
                string receivingAdviceDate = doc.SelectSingleNode("/eDIMessage/receivingAdvice").Attributes["date"].Value; //Дата RecAdv
                string goodsReceiptDate = "1900-01-01"; //Дата приемки
                try
                {
                    goodsReceiptDate = doc.SelectSingleNode("/eDIMessage/receivingAdvice/deliveryInfo").Attributes["date"].Value; //Дата приемки
                }
                catch
                {
                    goodsReceiptDate = "1900-01-01";//Verifiacation.getInnerTextforXmlNode(doc.SelectSingleNode("/eDIMessage/receivingAdvice/deliveryInfo/receptionDateTime")).Substring(0,10); //Дата приемки
                }
                string buyerOrderDate;
                try
                {
                    buyerOrderDate = doc.SelectSingleNode("/eDIMessage/receivingAdvice/originOrder").Attributes["date"].Value; //Дата заказа
                }
                catch
                {
                    buyerOrderDate = "1900-01-01";
                }
                string despatchNumber = "";
                try
                {
                    despatchNumber = doc.SelectSingleNode("/eDIMessage/receivingAdvice/despatchIdentificator").Attributes["number"].Value; //номер накладной
                }
                catch
                {
                    despatchNumber = "";
                }

                //итоги шапки
                // string totalLines = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Summary/TotalLines").InnerText; //Всего товарных строк
                decimal totalGoodsReceiptAmount = 0;// = doc.SelectSingleNode("/Document-ReceivingAdvice/ReceivingAdvice-Summary/TotalGoodsReceiptAmount").InnerText; //Всего количество
                decimal totalNetAmount = 0;//doc.SelectSingleNode("/eDIMessage/receivingAdvice/lineItems/totalSumExcludingTaxes").InnerText; //Сумма без НДС
                decimal totalGrossAmount = 0;
                //if (doc.SelectSingleNode("/eDIMessage/receivingAdvice/totalAmount") != null)
                //    totalGrossAmount = doc.SelectSingleNode("/eDIMessage/receivingAdvice/totalAmount").InnerText; //Сумма с НДС

                //привести номер накладной в порядок
                string transformed_despatchNumber = Verifiacation.combDespatchNumber(despatchNumber);
                //Проверка: может мы уже загружали с таким номером, датой? Если загружали, тогда пропустить
                bool existsRecAdv = DispOrders.checkExistsRecAdv(receivingAdviceNumber, buyerOrderNumber);
                if (existsRecAdv)
                {
                    Program.WriteLine("Документ RecAdv с таким номер акта и номером заказа был обработан ранее");
                    DispOrders.WriteEDIProcessedFile(typeDoc, DateTime.Now, receivingAdviceNumber, buyerOrderNumber, Path.GetFileName(parsefile), "Документ RecAdv с таким номер акта и номером заказа был обработан ранее. Документ перенесен в архив", senderILN, senderName, buyerILN, buyerName, deliveryILN, deliveryName, deliveryAddress, provider, 0);
                    //перенести в архив
                    DispOrders.MoveToFolder(parsefile, ArchivePath);
                    continue; //прекращаем разбор файла, берем следующий
                }

                if (startDate > DateTime.ParseExact(receivingAdviceDate, "yyyy-MM-dd", null))
                {
                    Program.WriteLine("Дата начала использования документа RecAdv для грузополучателя больше даты документа");
                    DispOrders.WriteEDIProcessedFile(typeDoc, DateTime.Now, receivingAdviceNumber, buyerOrderNumber, Path.GetFileName(parsefile), "Дата начала использования документа RecAdv для грузополучателя больше даты документа. Документ перенесен в архив", senderILN, senderName, buyerILN, buyerName, deliveryILN, deliveryName, deliveryAddress, provider, 0);
                    //перенести в архив
                    DispOrders.MoveToFolder(parsefile, ArchivePath);
                    continue; //прекращаем разбор файла, берем следующий
                }

                //обработать xml файл
                //занесем данные табличной части в переменную table
                List<List<string>> table = new List<List<string>>();
                int i = 0;
                foreach (XmlNode n in doc.SelectNodes("/eDIMessage/receivingAdvice/lineItems/lineItem"))
                {
                    if (n.SelectSingleNode("gtin") != null) //штрихкод)
                    {
                        List<string> row = new List<string>();
                        i = i + 1;
                        totalGoodsReceiptAmount = totalGoodsReceiptAmount + Convert.ToDecimal(n.SelectSingleNode("acceptedQuantity").InnerText.Replace('.', ','));
                        if (n.SelectSingleNode("netAmount") != null)
                            totalNetAmount = totalNetAmount + Convert.ToDecimal(n.SelectSingleNode("netAmount").InnerText.Replace('.', ',')); //сумма без НДС
                        if (n.SelectSingleNode("netPriceWithVAT") != null)
                            totalGrossAmount = totalGrossAmount + Convert.ToDecimal(n.SelectSingleNode("acceptedQuantity").InnerText.Replace('.', ',')) * Convert.ToDecimal(n.SelectSingleNode("netPriceWithVAT").InnerText.Replace('.', ',')); //сумма с НДС;
                        else
                            totalGrossAmount = totalGrossAmount + 0;
                        string EAN = Verifiacation.getInnerTextforXmlNode(n.SelectSingleNode("gtin")); //штрихкод
                        string QuantityReceived = Verifiacation.getInnerTextforXmlNode(n.SelectSingleNode("acceptedQuantity")); //количество принятое
                        string UnitGrossPrice = Verifiacation.getInnerTextforXmlNode(n.SelectSingleNode("netPriceWithVAT")); //Цена с НДС 
                        string SupplierItemCode = Verifiacation.getInnerTextforXmlNode(n.SelectSingleNode("internalBuyerCode")); //код товара у покупателя
                        string BuyerItemCode = Verifiacation.getInnerTextforXmlNode(n.SelectSingleNode("internalBuyerCode")); //код товара у плательщика
                        row.Add(receivingAdviceNumber); //номер акта --1
                        row.Add(buyerOrderNumber); //номер заказа  --2
                        row.Add(receivingAdviceDate); //дата акта  --3
                        row.Add(deliveryILN); //ГЛН грузополучателя  --4
                        row.Add(i.ToString()); //номер строки --5
                        row.Add(EAN); //штрихкод  --6
                        row.Add(BuyerItemCode); //код товара у плательщика --7
                        row.Add(Verifiacation.getInnerTextforXmlNode(n.SelectSingleNode("orderedQuantity"))); //количество заказанное  --8
                        row.Add(QuantityReceived); //количество принятое  --9
                        row.Add(UnitGrossPrice); //Цена с НДС  --10
                        row.Add(Verifiacation.getInnerTextforXmlNode(n.SelectSingleNode("netPrice"))); //Цена без НДС --11
                        row.Add(n.SelectSingleNode("acceptedQuantity").Attributes["unitOfMeasure"].Value); //Единица измерения : PCE - штука , PA - коробка, KGM - кг, GRM - грамм, PF - палета --12
                        row.Add(SupplierItemCode); //код товара у покупателя --13
                        row.Add(Verifiacation.getInnerTextforXmlNode(n.SelectSingleNode("QuantityDamaged"))); //количество поврежденного товара --14
                        row.Add(Verifiacation.getInnerTextforXmlNode(n.SelectSingleNode("QuantityUndelivered"))); //количество недопоставленного товара --15
                        row.Add(Verifiacation.getInnerTextforXmlNode(n.SelectSingleNode("QuantityOverOrdered"))); //количество сверх заказанного  --16
                        row.Add(""); //комментарий  --17
                        table.Add(row);
                    }
                    
                }
                //очистим временную таблицу
                DispOrders.DeleteTmpDet();
                //перенесем данные из переменной table во временную таблицу
                DispOrders.InsertTmpDet(table); //запись во временную таблицу   
                //сравнить расходную накладную и акт, разницу отразить в таблице U_MGEDITMPDOC
                string cmt = "";
                int status = DispOrders.CompareActAndInvoice(buyerOrderNumber, despatchNumber, ref cmt);
                //создание recAdv из временной в постоянную
                DispOrders.CreateRecAdv(receivingAdviceDate, receivingAdviceNumber, Convert.ToInt64(buyerILN), Convert.ToInt64(deliveryILN), transformed_despatchNumber, Convert.ToString(totalGrossAmount), Convert.ToInt16(i), Convert.ToString(totalNetAmount), buyerOrderDate, buyerOrderNumber, Convert.ToString(totalGoodsReceiptAmount), goodsReceiptDate, cmt);
                DispOrders.WriteEDIProcessedFile(typeDoc, DateTime.Now, receivingAdviceNumber, buyerOrderNumber, Path.GetFileName(parsefile), cmt, senderILN, senderName, buyerILN, buyerName, deliveryILN, deliveryName, deliveryAddress, provider, status);
                //перенести в архив
                DispOrders.MoveToFolder(parsefile, ArchivePath);
            }
        }
        public static void CreateKonturBase_UPD(List<object> CurrDataUPD) //список УПД, 0 ProviderOpt, 1 ProviderZkg, 2 NastDoc_Fmt, 3 SklSf_Rcd, 4 SklSf_TpOtg, 5 SklSfA_RcdCor, 6 PrdZkg_NmrExt, 7 PrdZkg_Rcd, 8 PrdZkg_Dt ,9 SklNk_TDrvNm
        {
            Program.WriteLine("Формирование УПД через Диадок ");
            //получение путей
            string pathArchiveEDI = /*"D:\\Edi\\Archive\\"; //*/ DispOrders.GetValueOption("СКБ-КОНТУР.АРХИВ");
            string pathUPDEDI;

            //Запрос данных СФ
            object[] infoSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataUPD[3])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO
            Program.WriteLine("Номер СФ " + infoSf[0].ToString());

            //Запрос данных накладной по рсд заказа
            object[] infoNk = Verifiacation.GetNkDataFromZkg(Convert.ToInt64(CurrDataUPD[7])); //0 SklNk_Nmr, 1 SklNk_Dat
            Program.WriteLine("Номер ТН " + infoNk[0].ToString());

            //Запрос номера и даты заказа по рсд заказа
            object[] infoOrder = Verifiacation.GetU_CHEDIEXCHDataFromZkg(Convert.ToInt64(CurrDataUPD[7])); //0 Exch_OrdNmrExt, 1 Exch_OrdDat
            if (infoOrder[0] != null) Program.WriteLine("Номер заказа " + infoOrder[0].ToString());

            //запрос данных спецификации
            object[,] Item = Verifiacation.GetItemsFromSF(Convert.ToString(CurrDataUPD[3]), true); //0 BarCode_Code, 1 SklN_Rcd, 2 SklN_Cd, 3 SklN_Nm, 4 Кол-во, 5 Цена без НДC, 6 Цена с НДС, 7 Код ЕИ EDI, 8 ОКЕЙ, 9 Ставка, 10 'S', 11 Сумма НДС, 12 Сумма с НДС, 13 шифр ЕИ, 14 Вес
            
            //Запрос данных покупателя (в основном ИП)
            object[] infoKag = Verifiacation.GetDataFromPtnRCD_IP(Convert.ToInt64(infoSf[2]), Convert.ToInt64(infoSf[3])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование, 12 Полное наименование
            
            //Запрос данных грузополучателя
            object[] infoGpl = Verifiacation.GetDataFromPtnRCD_IP(Convert.ToInt64(infoSf[4]), Convert.ToInt64(infoSf[5])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование
            
            //какой gln номер использовать
            bool useMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(infoSf[4]));
            string ilnFirm;
            string NameOrFio = "";
            string FamilijaIP = "";
            string ImjaIP = "";
            string OtchestvoIP = "";

            object[] infoFirm;
            object[] infoFirmAdr;
            object[] infoFirmAdrG;
            object[] infoFirmG;

            object[] infoFirmGrOt; //данные грузоотправителя
            object[] infoFirmAdrGrOt; //адрес грузоотправителя

            infoFirmG = Verifiacation.GetMasterFirmInfo();
            infoFirmAdrG = Verifiacation.GetMasterFirmAdr();
            
            if (useMasterGLN == false)//используем данные текущего предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ИЛН");
                pathUPDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/ DispOrders.GetValueOption("СКБ-КОНТУР.УПД");
                if (Convert.ToDateTime(infoKag[13]) > Convert.ToDateTime(infoSf[1]))      // 13 это дата с которой надо ставить новые данные
                {
                    infoFirm = Verifiacation.GetFirmInfo("20171130"); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO        // берём как до 01.12.2017
                }
                else
                {
                    infoFirm = Verifiacation.GetFirmInfo(Convert.ToDateTime(infoSf[1]).ToString(@"yyyyMMdd")); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO
                }
                infoFirmAdr = Verifiacation.GetFirmAdr(); // 0 CrtAdr_StrNm+','+CrtAdr_House, 1 CrtAdr_TowNm, 2 CrtAdr_RegNm, 3 CrtAdr_Ind, 4 CrtAdr_RegCd, 5 CrtAdr_StrNm, 6 CrtAdr_House 
                infoFirmGrOt = infoFirm;
                infoFirmAdrGrOt = infoFirmAdr;
            }
            else//используем данные головного предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");
                infoFirm = Verifiacation.GetMasterFirmInfo();
                infoFirmAdr = Verifiacation.GetMasterFirmAdr();
                if (Convert.ToDateTime(infoKag[13]) > Convert.ToDateTime(infoSf[1]))      // 13 это дата с которой надо ставить новые данные
                {
                    infoFirmGrOt = Verifiacation.GetFirmInfo("20171130"); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO        // берём как до 01.12.2017
                }
                else
                {
                    infoFirmGrOt = Verifiacation.GetFirmInfo(Convert.ToDateTime(infoSf[1]).ToString(@"yyyyMMdd")); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO
                }
                infoFirmAdrGrOt = Verifiacation.GetFirmAdr(); // 0 CrtAdr_StrNm+','+CrtAdr_House, 1 CrtAdr_TowNm, 2 CrtAdr_RegNm, 3 CrtAdr_Ind, 4 CrtAdr_RegCd, 5 CrtAdr_StrNm, 6 CrtAdr_House 

                try
                {
                    pathUPDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/ DispOrders.GetValueOption("СКБ-КОНТУР.ЭКСПОРТ");
                }
                catch
                {
                    pathUPDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/ DispOrders.GetValueOption("СКБ-КОНТУР.УПД");
                }
            }
            
            string idEdo = DispOrders.GetValueOption("СКБ-КОНТУР.ИДЭДО");  //ИдЭДО

            //string idOtpr = idEdo + ilnFirm; //ИдОтпр
            //string idPol = idEdo + infoGpl[2].ToString(); //ИдПол

            string InnKag = "";
            string KppKag = "";
            InnKag = infoKag[3].ToString();
            KppKag = infoKag[4].ToString();
            string idPol = ""; //ИдПолуч
            string BoxIdPol = ""; //ЯщикПолуч
            string idOtpr = ""; //ИдОтпр
            string BoxIdOtpr = ""; //ЯщикОтпр
            string IdProvaiderPol = "";
            IdProvaiderPol = DispOrders.GetValueOption("СКБ-КОНТУР.ИДЭДО"); //Id провайдера плательщика (иногда через роуминг, по умолчанию Диадок)
            string IdProvaiderOtpr = "";
            IdProvaiderOtpr = DispOrders.GetValueOption("СКБ-КОНТУР.ИДЭДО"); //Id провайдера отправителя

            string[] PolInfo;
            string[] OtprInfo;
            
            object[] PtnInfo = Verifiacation.GetIdProviderFromPtnCD(infoKag[0].ToString());  //Информация о плательщике
            
            string IdProviderPlat = ""; //Код провайдера ЭДО в карточке контрагента
            IdProviderPlat = Convert.ToString(PtnInfo[2]);
            if (IdProviderPlat.Length > 0)  //Если код справочника заполнен
            {
                Program.WriteLine("В карточке контрагента код оператора ЭДО " + IdProviderPlat);
                IdProvaiderPol = IdProviderPlat;
            }

            Program.WriteLine("Информация о получателе ИНН " + InnKag + " КПП " + KppKag);
            PolInfo = DiadocAuthenticate.OrganizationInfo(InnKag, KppKag, IdProvaiderPol);
            idPol = PolInfo[0];
            BoxIdPol = PolInfo[1];

            if (idPol == "")  //у провайдера бывает код идентификатора ЭДО в нижнем регистре
            {
                PolInfo = DiadocAuthenticate.OrganizationInfo(InnKag, KppKag, IdProvaiderPol.ToLower());
                idPol = PolInfo[0];
                BoxIdPol = PolInfo[1];
            }

            Program.WriteLine("Информация об отправителе ИНН " + infoFirm[1].ToString() + " КПП " + infoFirm[2].ToString());
            OtprInfo = DiadocAuthenticate.OrganizationInfo(infoFirm[1].ToString(), infoFirm[2].ToString(), IdProvaiderOtpr);
            idOtpr = OtprInfo[0];
            BoxIdOtpr = OtprInfo[1];
            
            if (idPol != "" && BoxIdPol != "")
            {
                if (idOtpr != "" && BoxIdOtpr != "")
                {
                    string guid = Convert.ToString(Guid.NewGuid());
                    //string fileName = "ON_SCHFDOPPR_" + idPol + "_" + idOtpr + "_" + DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid;//ИдФайл
                    string idName;
                    if (CurrDataUPD[2].ToString().Equals("BaseMark")) idName = "ON_NSCHFDOPPRMARK_";
                    else idName = "ON_NSCHFDOPPR_";
                    
                    string fileName = idName + idPol + "_" + idOtpr + "_" + DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid;//ИдФайл

                    /************************** 1 уровень. <Файл> ******************************/

                    XDocument xdoc = new XDocument(new XDeclaration("1.0", "windows-1251", ""));

                    XElement File = new XElement("Файл");
                    XAttribute IdFile = new XAttribute("ИдФайл", idName + idPol + "_" + idOtpr + "_" + DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid/*fileName*/);
                    XAttribute VersForm = new XAttribute("ВерсФорм", "5.01");
                    XAttribute VersProg = new XAttribute("ВерсПрог", "Diadoc 1.0");

                    xdoc.Add(File);
                    File.Add(IdFile);
                    File.Add(VersForm);
                    File.Add(VersProg);

                    /************************** 2 уровень. <СвУчДокОбор> ************************/

                    XElement ID = new XElement("СвУчДокОбор");

                    XAttribute IdSender = new XAttribute("ИдОтпр", idOtpr/*idOtpr*/);
                    XAttribute IdReciever = new XAttribute("ИдПол", idPol/*idPol*/);

                    File.Add(ID);
                    ID.Add(IdSender);
                    ID.Add(IdReciever);

                    //<СвУчДокОбор><СвОЭДОтпр>
                    XElement InfOrg = new XElement("СвОЭДОтпр");
                    string providerNm = DispOrders.GetValueOption("СКБ-КОНТУР.НМ");
                    string providerInn = DispOrders.GetValueOption("СКБ-КОНТУР.ИНН");
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
                    XAttribute Function = new XAttribute("Функция", "СЧФДОП");
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

                    //<Документ><СвСчФакт><ИспрСчФ>
                    XElement IsprSchf = new XElement("ИспрСчФ");
                    XAttribute DefNomIsprSchf = new XAttribute("ДефНомИспрСчФ", "-");
                    XAttribute DefDateIsprSchf = new XAttribute("ДефДатаИспрСчФ", "-");
                    SVSF.Add(IsprSchf);
                    IsprSchf.Add(DefNomIsprSchf);
                    IsprSchf.Add(DefDateIsprSchf);

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
                    XAttribute SvProdIdSvName = new XAttribute("НаимОрг", infoFirmG[0].ToString());
                    XAttribute SvProdIdSvINN = new XAttribute("ИННЮЛ", infoFirm[1].ToString());
                    XAttribute SvProdIdSvKPP = new XAttribute("КПП", infoFirm[2].ToString());
                    if (useMasterGLN) //используем данные головного предприятия
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
                    if (infoFirmAdrG[3].ToString() != "")
                    {
                        XAttribute SvProdIndex = new XAttribute("Индекс", infoFirmAdrG[3].ToString());
                        SvProdAdrRF.Add(SvProdIndex);
                    }
                    if (infoFirmAdrG[4].ToString() != "")
                    {
                        XAttribute SvProdKodReg = new XAttribute("КодРегион", infoFirmAdrG[4].ToString());
                        SvProdAdrRF.Add(SvProdKodReg);
                    }

                    if (infoFirmAdrG[1].ToString() != "")
                    {
                        XAttribute SvProdGorod = new XAttribute("Город", infoFirmAdrG[1].ToString());
                        SvProdAdrRF.Add(SvProdGorod);
                    }
                    if (infoFirmAdrG[0].ToString() != "")
                    {
                        XAttribute SvProdStreet = new XAttribute("Улица", infoFirmAdrG[0].ToString());
                        SvProdAdrRF.Add(SvProdStreet);
                    }
                    
                    //<Документ><СвСчФакт><ГрузОт>
                    XElement GruzOt = new XElement("ГрузОт");

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
                    
                    //XElement GruzOtOnJe = new XElement("ОнЖе", "он же");
                    //SVSF.Add(GruzOt);
                    //GruzOt.Add(GruzOtOnJe);            

                    //<Документ><СвСчФакт><ГрузПолуч>
                    XElement GruzPoluch = new XElement("ГрузПолуч");
                    SVSF.Add(GruzPoluch);

                    //<Документ><СвСчФакт><ГрузПолуч><ИдСв>
                    XElement GruzPoluchIdSv = new XElement("ИдСв");
                    GruzPoluch.Add(GruzPoluchIdSv);
                    
                    if (infoGpl[3].ToString().Length == 10)
                    {
                        //<Документ><СвСчФакт><ГрузПолуч><ИдСв><СвЮЛУч>
                        //Сведения о юридическом лице, состоящем на учете в налоговых органах, ИННЮЛ(=10 симв)
                        XElement GruzPoluchSvUluch = new XElement("СвЮЛУч");
                        XAttribute GruzPoluchName = new XAttribute("НаимОрг", infoGpl[12]);
                        XAttribute GruzPoluchINN = new XAttribute("ИННЮЛ", infoGpl[3]);
                        XAttribute GruzPoluchKPP = new XAttribute("КПП", infoGpl[4]);

                        GruzPoluchIdSv.Add(GruzPoluchSvUluch);
                        GruzPoluchSvUluch.Add(GruzPoluchName);
                        GruzPoluchSvUluch.Add(GruzPoluchINN);
                        GruzPoluchSvUluch.Add(GruzPoluchKPP);
                    }
                    
                    if (infoGpl[3].ToString().Length == 12)
                    {
                        //Сведения об индивидуальном предпринимателе, ИННФЛ(=12 симв)
                        XElement GruzPoluchSvIP = new XElement("СвИП");
                        XAttribute GruzPoluchInnFl = new XAttribute("ИННФЛ", infoGpl[3]);
                        XElement GruzPoluchSvIPFIO = new XElement("ФИО");

                        NameOrFio = infoKag[1].ToString();  //Берем ФИО у головного, так как в его карточке более корректное
                        NameOrFio = NameOrFio.Replace("ИП", "");
                        NameOrFio = NameOrFio.Trim();
                        FamilijaIP = "";
                        ImjaIP = "";
                        OtchestvoIP = "";
                        FamilijaIP = NameOrFio.Substring(0, NameOrFio.IndexOf(' '));
                        ImjaIP = NameOrFio.Substring(NameOrFio.IndexOf(' ') + 1, 2);
                        OtchestvoIP = NameOrFio.Substring(NameOrFio.IndexOf('.') + 1, 2);

                        XAttribute GruzPoluchSvIPFamilija = new XAttribute("Фамилия", FamilijaIP);
                        XAttribute GruzPoluchSvIPImja = new XAttribute("Имя", ImjaIP);
                        XAttribute GruzPoluchSvIPOtchestvo = new XAttribute("Отчество", OtchestvoIP);

                        GruzPoluchIdSv.Add(GruzPoluchSvIP);
                        GruzPoluchSvIP.Add(GruzPoluchInnFl);
                        GruzPoluchSvIP.Add(GruzPoluchSvIPFIO);

                        GruzPoluchSvIPFIO.Add(GruzPoluchSvIPFamilija);
                        GruzPoluchSvIPFIO.Add(GruzPoluchSvIPImja);
                        GruzPoluchSvIPFIO.Add(GruzPoluchSvIPOtchestvo);
                    }
                    
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

                    if (infoKag[3].ToString().Length == 10)
                    {
                        //<Документ><СвСчФакт><СвПокуп><ИдСв><СвЮЛУч>             
                        XElement SvPokupSvUluch = new XElement("СвЮЛУч");
                        XAttribute SvPokupName = new XAttribute("НаимОрг", infoKag[12]);
                        XAttribute SvPokupINN = new XAttribute("ИННЮЛ", infoKag[3]);
                        XAttribute SvPokupKPP = new XAttribute("КПП", infoKag[4]);
                        SvPokupIdSv.Add(SvPokupSvUluch);
                        SvPokupSvUluch.Add(SvPokupName);
                        SvPokupSvUluch.Add(SvPokupINN);
                        SvPokupSvUluch.Add(SvPokupKPP);
                    }

                    if (infoKag[3].ToString().Length == 12)
                    {
                        //Сведения об индивидуальном предпринимателе, ИННФЛ(=12 симв)
                        XElement SvPokupSvIP = new XElement("СвИП");
                        XAttribute SvPokupInnFl = new XAttribute("ИННФЛ", infoKag[3]);
                        XElement SvPokupSvIPFIO = new XElement("ФИО");

                        NameOrFio = infoKag[1].ToString();
                        NameOrFio = NameOrFio.Replace("ИП", "");
                        NameOrFio = NameOrFio.Trim();
                        FamilijaIP = "";
                        ImjaIP = "";
                        OtchestvoIP = "";
                        FamilijaIP = NameOrFio.Substring(0, NameOrFio.IndexOf(' '));
                        ImjaIP = NameOrFio.Substring(NameOrFio.IndexOf(' ') + 1, 2);
                        OtchestvoIP = NameOrFio.Substring(NameOrFio.IndexOf('.') + 1, 2);

                        XAttribute SvPokupSvIPFamilija = new XAttribute("Фамилия", FamilijaIP);
                        XAttribute SvPokupSvIPImja = new XAttribute("Имя", ImjaIP);
                        XAttribute SvPokupSvIPOtchestvo = new XAttribute("Отчество", OtchestvoIP);

                        SvPokupIdSv.Add(SvPokupSvIP);
                        SvPokupSvIP.Add(SvPokupInnFl);
                        SvPokupSvIP.Add(SvPokupSvIPFIO);

                        SvPokupSvIPFIO.Add(SvPokupSvIPFamilija);
                        SvPokupSvIPFIO.Add(SvPokupSvIPImja);
                        SvPokupSvIPFIO.Add(SvPokupSvIPOtchestvo);
                    }

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
                    if (infoKag[15].ToString().Length > 0)
                    {
                        XAttribute SvPokupFlat = new XAttribute("Кварт", infoKag[15]);
                        SvPokupAdrRF.Add(SvPokupFlat);
                    }

                    //<Документ><СвСчФакт><ИнфПолФХЖ1>
                    XElement DopSvFHJ1 = new XElement("ДопСвФХЖ1");
                    XAttribute NaimOKV = new XAttribute("НаимОКВ", "Российский рубль");
                    //XAttribute ObstFormSchf = new XAttribute("ОбстФормСЧФ", "1");
                    SVSF.Add(DopSvFHJ1);
                    DopSvFHJ1.Add(NaimOKV);
                    //DopSvFHJ1.Add(ObstFormSchf);

                    //<Документ><СвСчФакт><ДокПодтвОтгр>
                    XElement DocPodtvOtgr = new XElement("ДокПодтвОтгр");
                    //XAttribute NaimDocOtgr = new XAttribute("НаимДокОтгр", "Документ об отгрузке товаров (выполнении работ), передаче имущественных прав (документа об оказании услуг)");
                    //XAttribute NomDocOtgr = new XAttribute("НомДокОтгр", infoSf[0].ToString()); //В качестве документа об отгрузке СФ
                    //XAttribute DataDocOtgr = new XAttribute("ДатаДокОтгр", Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));

                    XAttribute NaimDocOtgr = new XAttribute("НаимДокОтгр", "Товарная накладная");
                    XAttribute NomDocOtgr = new XAttribute("НомДокОтгр", Convert.ToString(infoNk[0]));  //В качестве документа об отгрузке накладная
                    XAttribute DataDocOtgr = new XAttribute("ДатаДокОтгр", Convert.ToDateTime(infoNk[1]).ToString(@"dd.MM.yyyy"));

                    SVSF.Add(DocPodtvOtgr);
                    DocPodtvOtgr.Add(NaimDocOtgr);
                    DocPodtvOtgr.Add(NomDocOtgr);
                    DocPodtvOtgr.Add(DataDocOtgr);

                    //<Документ><СвСчФакт><ИнфПолФХЖ1>
                    /*
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
                    XElement TxtInf5 = new XElement("ТекстИнф");
                    XAttribute TxtInf5Identif = new XAttribute("Идентиф", "GLN_грузополучателя");
                    XAttribute TxtInf5Znachen = new XAttribute("Значен", infoGpl[2]);
                    InfPolFHJ1.Add(TxtInf5);
                    TxtInf5.Add(TxtInf5Identif);
                    TxtInf5.Add(TxtInf5Znachen);
                    */
                    
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
                        
                        //<Документ><ТаблСчФакт><СведТов><ДопСведТов>
                        XElement DopSvedTov = new XElement("ДопСведТов");
                        XAttribute PrTovRav = new XAttribute("ПрТовРаб", "1");
                        XAttribute NaimEdIzm = new XAttribute("НаимЕдИзм", Item[i, 13]);
                        SvedTov.Add(DopSvedTov);
                        DopSvedTov.Add(PrTovRav);
                        DopSvedTov.Add(NaimEdIzm);

                        if (CurrDataUPD[2].ToString().Equals("BaseMark") && Item[i, 9].ToString().Equals("10%"))
                        {
                            // TODO:  (отправка маркировки)
                            /*
                             * <ДопСведТов АртикулТов="03020000000000000218" НаимЕдИзм="шт" ПрТовРаб="1">
                                    <НомСредИдентТов>
                                            <НомУпак>[02][gtin][37][количество]</НомУпак>
                                    </НомСредИдентТов>
                                </ДопСведТов>
                             */

                            string nomUpakValue = "020" + Item[i, 0] + "37";
                            nomUpakValue += (Math.Round(Convert.ToDecimal(Item[i, 4]))).ToString();
                            XElement NomSredIdent = new XElement("НомСредИдентТов");
                            XElement NomUpak = new XElement("НомУпак", nomUpakValue);
                            DopSvedTov.Add(NomSredIdent);
                            NomSredIdent.Add(NomUpak);
                        }

                        //<Документ><ТаблСчФакт><СведТов><ИнфПолФХЖ2>
                        /* //у ИП нет таблиц соответствия продукции
                        string nomBuyerCd = Verifiacation.GetBuyerItemCodeRcd(Convert.ToString(infoKag[5]), Convert.ToInt64(Item[i, 1]));

                        XElement InfPolFHJ21 = new XElement("ИнфПолФХЖ2");
                        XAttribute ItmTxtInf1Identif = new XAttribute("Идентиф", "код_материала");
                        XAttribute ItmTxtInf1Znachen = new XAttribute("Значен", nomBuyerCd);
                        SvedTov.Add(InfPolFHJ21);
                        InfPolFHJ21.Add(ItmTxtInf1Identif);
                        InfPolFHJ21.Add(ItmTxtInf1Znachen);*/

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
                    SvPer.Add(OsnPer);
                    if (infoOrder[0] != null)
                    {
                        XAttribute NaimOsn = new XAttribute("НаимОсн", "Заказ");
                        XAttribute NomOsn = new XAttribute("НомОсн", infoOrder[0]);
                        XAttribute DataOsn = new XAttribute("ДатаОсн", Convert.ToDateTime(infoOrder[1]).ToString(@"dd.MM.yyyy"));
                        OsnPer.Add(NaimOsn);
                        OsnPer.Add(NomOsn);
                        OsnPer.Add(DataOsn);
                    }
                    else
                    {
                        XAttribute NaimOsn = new XAttribute("НаимОсн", "Без документа-основания");
                        OsnPer.Add(NaimOsn);
                    }
                    
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
                    //XAttribute dolj = new XAttribute("Должн", "Генеральный Директор");
                    Podp.Add(UL);
                    UL.Add(innUl);
                    UL.Add(naimOrg);
                    UL.Add(dolj);

                    //<Документ><Подписант><ЮЛ><ФИО>
                    XElement FIO = new XElement("ФИО");
                    XAttribute famdir = new XAttribute("Фамилия", infoSigner[1]);
                    XAttribute namedir = new XAttribute("Имя", infoSigner[2]);
                    XAttribute otchesdir = new XAttribute("Отчество", infoSigner[3]);

                    //XAttribute famdir = new XAttribute("Фамилия", "Еремин");
                    //XAttribute namedir = new XAttribute("Имя", "Дмитрий");
                    //XAttribute otchesdir = new XAttribute("Отчество", "Владимирович");

                    UL.Add(FIO);
                    FIO.Add(famdir);
                    FIO.Add(namedir);
                    FIO.Add(otchesdir);
                    
                    //------сохранение документа-----------
                    fileName = fileName + ".xml";
                    string fileNameSokr = DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid + ".xml";
                    try
                    {
                        xdoc.Save(pathArchiveEDI + fileName);
                        try
                        {
                            xdoc.Save(pathUPDEDI + fileName);
                            string message = "СКБ-Контур. УПД " + fileName + " создан в " + pathUPDEDI;
                            Program.WriteLine(message);
                            //имя файла слишком длинное, обрежим его до дата + гуид, поиск можно будет делать по регулярному выражению .*fileNameSokr
                            DispOrders.WriteProtocolEDI("УПД", fileNameSokr, infoKag[0] + " - " + infoKag[1], 0, infoGpl[0] + " - " + infoGpl[1], "УПД сформирован", DateTime.Now, Convert.ToString(CurrDataUPD[6]), "KONTUR");
                            DispOrders.WriteEDiSentDoc("8", fileNameSokr, Convert.ToString(CurrDataUPD[3]), Convert.ToString(infoSf[0]), "123", Convert.ToString(sumWthNds), Convert.ToString(CurrDataUPD[7]), 1);

                        }
                        catch (Exception e)
                        {
                            string message_error = "СКБ-Контур. Не могу создать xml файл УПД в " + pathUPDEDI + ". Нет доступа или диск переполнен.";
                            DispOrders.WriteProtocolEDI("УПД", fileNameSokr, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УПД не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUPD[6]), "KONTUR");
                            Program.WriteLine(message_error);
                            DispOrders.WriteErrorLog(e.Message);
                        }

                        try
                        {
                            Program.WriteLine("Отправляем УПД в Диадок через Диадок API");
                            DiadocAuthenticate.SendInvoiceXml(pathUPDEDI, fileName, idPol, idOtpr, BoxIdPol, BoxIdOtpr, "UniversalTransferDocument", infoSf[0].ToString(), Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));  //Отправляем УПД в Диадок через Диадок API
                        }
                        catch (Exception e)
                        {
                            Program.WriteLine("Ошибка отправки УПД в Диадок через Диадок API");
                            DispOrders.WriteErrorLog(e.Message);
                        }
                    }

                    catch (Exception e)
                    {
                        string message_error = "СКБ-Контур. Не могу создать xml файл УПД в " + pathArchiveEDI + ". Нет доступа или диск переполнен.";
                        DispOrders.WriteProtocolEDI("УПД", fileNameSokr, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УПД не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUPD[6]), "KONTUR");
                        Program.WriteLine(message_error);
                        DispOrders.WriteErrorLog(e.Message);
                        //запись в лог о неудаче
                    }
                }
                else
                {
                    Program.WriteLine("Невозможно отправить документ УПД через Диадок API, данные об отправителе отсутствуют");
                }
            }
            else
            {
                Program.WriteLine("Невозможно отправить документ УПД через Диадок API, данные о получателе отсутствуют");
            }
        }

        public static void CreateKonturBase_UKD(List<object> CurrDataUKD) //список УПД, 0 ProviderOpt, 1 ProviderZkg, 2 NastDoc_Fmt, 3 SklSf_Rcd, 4 SklSf_TpOtg, 5 SklSfA_RcdCor, 6 PrdZkg_NmrExt, 7 PrdZkg_Rcd, 8 PrdZkg_Dt ,9 SklNk_TDrvNm
        {
            Program.WriteLine("Формирование УКД через Диадок ");
            //получение путей
            string pathArchiveEDI = /*"D:\\Edi\\Archive\\"; //*/ DispOrders.GetValueOption("СКБ-КОНТУР.АРХИВ");
            string pathUPDEDI;

            //Запрос данных КорректировочнойСФ
            object[] infoSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataUKD[3])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO
            //Запрос данных Корректируемой (отгрузочной) СФ
            object[] infoCorSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataUKD[5])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO
            Program.WriteLine("Номер КСФ " + infoSf[0].ToString());

            //запрос данных спецификации
            object[,] Item = Verifiacation.GetItemsFromKSF(Convert.ToString(CurrDataUKD[3]), Convert.ToString(CurrDataUKD[5]), true); //0 BarCode_Code, 1 SklN_Rcd, 2 SklN_Cd, 3 SklN_Nm, 4 Кол-во, 5 Цена без НДC, 6 Цена с НДС, 7 Код ЕИ EDI, 8 ОКЕЙ, 9 Ставка, 10 'S', 11 Сумма НДС, 12 Сумма с НДС, 13 шифр ЕИ, 14 Вес

            //Запрос данных покупателя
            object[] infoKag = Verifiacation.GetDataFromPtnRCD_IP(Convert.ToInt64(infoSf[2]), Convert.ToInt64(infoSf[3])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование, 12 Полное наименование
            //Запрос данных грузополучателя
            object[] infoGpl = Verifiacation.GetDataFromPtnRCD_IP(Convert.ToInt64(infoSf[4]), Convert.ToInt64(infoSf[5])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование

            //какой gln номер использовать
            bool useMasterGLN = Verifiacation.GetUseMasterGln(Convert.ToString(infoSf[4]));
            string ilnFirm;
            string NameOrFio = "";
            string FamilijaIP = "";
            string ImjaIP = "";
            string OtchestvoIP = "";

            object[] infoFirm;
            object[] infoFirmAdr;
            object[] infoFirmGrOt; //данные грузоотправителя
            object[] infoFirmAdrGrOt; //адрес грузоотправителя
            object[] infoFirmG;
            object[] infoFirmAdrG;

            infoFirmG = Verifiacation.GetMasterFirmInfo();
            infoFirmAdrG = Verifiacation.GetMasterFirmAdr();

            if (useMasterGLN == false)//используем данные текущего предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ИЛН");
                pathUPDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/ DispOrders.GetValueOption("СКБ-КОНТУР.УКД");
                if (Convert.ToDateTime(infoKag[13]) > Convert.ToDateTime(infoSf[1]))      // 13 это дата с которой надо ставить новые данные
                {
                    infoFirm = Verifiacation.GetFirmInfo("20171130"); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO        // берём как до 01.12.2017
                }
                else
                {
                    infoFirm = Verifiacation.GetFirmInfo(Convert.ToDateTime(infoSf[1]).ToString(@"yyyyMMdd")); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO
                }
                infoFirmAdr = Verifiacation.GetFirmAdr(); // 0 CrtAdr_StrNm+','+CrtAdr_House, 1 CrtAdr_TowNm, 2 CrtAdr_RegNm, 3 CrtAdr_Ind, 4 CrtAdr_RegCd, 5 CrtAdr_StrNm, 6 CrtAdr_House 
                infoFirmGrOt = infoFirm;
                infoFirmAdrGrOt = infoFirmAdr;
            }
            else//используем данные головного предприятия
            {
                ilnFirm = DispOrders.GetValueOption("ОБЩИЕ.ГЛАВНЫЙ GLN");
                infoFirm = Verifiacation.GetMasterFirmInfo();
                infoFirmAdr = Verifiacation.GetMasterFirmAdr();
                if (Convert.ToDateTime(infoKag[13]) > Convert.ToDateTime(infoSf[1]))      // 13 это дата с которой надо ставить новые данные
                {
                    infoFirmGrOt = Verifiacation.GetFirmInfo("20171130"); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO        // берём как до 01.12.2017
                }
                else
                {
                    infoFirmGrOt = Verifiacation.GetFirmInfo(Convert.ToDateTime(infoSf[1]).ToString(@"yyyyMMdd")); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO
                }
                infoFirmAdrGrOt = Verifiacation.GetFirmAdr(); // 0 CrtAdr_StrNm+','+CrtAdr_House, 1 CrtAdr_TowNm, 2 CrtAdr_RegNm, 3 CrtAdr_Ind, 4 CrtAdr_RegCd, 5 CrtAdr_StrNm, 6 CrtAdr_House 

                try
                {
                    pathUPDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/ DispOrders.GetValueOption("СКБ-КОНТУР.ЭКСПОРТ");
                }
                catch
                {
                    pathUPDEDI = /*"d:\\EDI\\SHFDOPPR\\"; //*/ DispOrders.GetValueOption("СКБ-КОНТУР.УКД");
                }
            }

            string idEdo = DispOrders.GetValueOption("СКБ-КОНТУР.ИДЭДО");  //ИдЭДО

            //string idOtpr = idEdo + ilnFirm; //ИдОтпр
            //string idPol = idEdo + infoGpl[2].ToString(); //ИдПол

            string InnKag = "";
            string KppKag = "";
            InnKag = infoGpl[3].ToString();
            KppKag = infoGpl[4].ToString();
            string idPol = ""; //ИдПолуч
            string BoxIdPol = ""; //ЯщикПолуч
            string idOtpr = ""; //ИдОтпр
            string BoxIdOtpr = ""; //ЯщикОтпр
            string IdProvaiderPol = "";
            IdProvaiderPol = DispOrders.GetValueOption("СКБ-КОНТУР.ИДЭДО"); //Id провайдера плательщика (иногда через роуминг, по умолчанию Диадок)
            string IdProvaiderOtpr = "";
            IdProvaiderOtpr = DispOrders.GetValueOption("СКБ-КОНТУР.ИДЭДО"); //Id провайдера отправителя

            string[] PolInfo;
            string[] OtprInfo;

            object[] PtnInfo = Verifiacation.GetIdProviderFromPtnCD(infoKag[0].ToString());  //Информация о плательщике
            string IdProviderPlat = ""; //Код провайдера ЭДО в карточке контрагента
            IdProviderPlat = Convert.ToString(PtnInfo[2]);
            if (IdProviderPlat.Length > 0)  //Если код справочника заполнен
            {
                Program.WriteLine("В карточке контрагента код оператора ЭДО " + IdProviderPlat);
                IdProvaiderPol = IdProviderPlat;
            }

            Program.WriteLine("Информация о получателе ИНН " + InnKag + " КПП " + KppKag);
            PolInfo = DiadocAuthenticate.OrganizationInfo(InnKag, KppKag, IdProvaiderPol);
            idPol = PolInfo[0];
            BoxIdPol = PolInfo[1];

            if (idPol == "")  //у провайдера бывает код идентификатора ЭДО в нижнем регистре
            {
                PolInfo = DiadocAuthenticate.OrganizationInfo(InnKag, KppKag, IdProvaiderPol.ToLower());
                idPol = PolInfo[0];
                BoxIdPol = PolInfo[1];
            }

            Program.WriteLine("Информация об отправителе ИНН " + infoFirm[1].ToString() + " КПП " + infoFirm[2].ToString());
            OtprInfo = DiadocAuthenticate.OrganizationInfo(infoFirm[1].ToString(), infoFirm[2].ToString(), IdProvaiderOtpr);
            idOtpr = OtprInfo[0];
            BoxIdOtpr = OtprInfo[1];

            if (idPol != "" && BoxIdPol != "")
            {
                if (idOtpr != "" && BoxIdOtpr != "")
                {

                    string guid = Convert.ToString(Guid.NewGuid());
                    //string fileName = "ON_SCHFDOPPR_" + idPol + "_" + idOtpr + "_" + DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid;//ИдФайл
                    string idName;
                    if (CurrDataUKD[2].ToString().Equals("BaseMark")) idName = "ON_NKORSCHFDOPPRMARK_";
                    else idName = "ON_NKORSCHFDOPPR_";
                    
                    string fileName = idName + idPol + "_" + idOtpr + "_" + DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid;//ИдФайл

                    /************************** 1 уровень. <Файл> ******************************/

                    XDocument xdoc = new XDocument(new XDeclaration("1.0", "windows-1251", ""));

                    XElement File = new XElement("Файл");
                    XAttribute IdFile = new XAttribute("ИдФайл", idName + idPol + "_" + idOtpr + "_" + DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid/*fileName*/);
                    XAttribute VersForm = new XAttribute("ВерсФорм", "5.01");
                    XAttribute VersProg = new XAttribute("ВерсПрог", "Diadoc 1.0");

                    xdoc.Add(File);
                    File.Add(IdFile);
                    File.Add(VersForm);
                    File.Add(VersProg);

                    /************************** 2 уровень. <СвУчДокОбор> ************************/

                    XElement ID = new XElement("СвУчДокОбор");

                    XAttribute IdSender = new XAttribute("ИдОтпр", idOtpr/*idOtpr*/);
                    XAttribute IdReciever = new XAttribute("ИдПол", idPol/*idPol*/);

                    File.Add(ID);
                    ID.Add(IdSender);
                    ID.Add(IdReciever);

                    //<СвУчДокОбор><СвОЭДОтпр>
                    XElement InfOrg = new XElement("СвОЭДОтпр");
                    string providerNm = DispOrders.GetValueOption("СКБ-КОНТУР.НМ");
                    string providerInn = DispOrders.GetValueOption("СКБ-КОНТУР.ИНН");
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
                    XAttribute NaimDocOpr = new XAttribute("НаимДокОпр", "Корректировочный счет-фактура и документ об изменении стоимости отгруженных товаров (выполненных работ, оказанных услуг), переданных имущественных прав");
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
                    if (useMasterGLN) //используем данные головного предприятия
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
                    if (infoFirmAdrG[3].ToString() != "")
                    {
                        XAttribute SvProdIndex = new XAttribute("Индекс", infoFirmAdrG[3].ToString());
                        SvProdAdrRF.Add(SvProdIndex);
                    }
                    if (infoFirmAdrG[4].ToString() != "")
                    {
                        XAttribute SvProdKodReg = new XAttribute("КодРегион", infoFirmAdrG[4].ToString());
                        SvProdAdrRF.Add(SvProdKodReg);
                    }

                    if (infoFirmAdrG[1].ToString() != "")
                    {
                        XAttribute SvProdGorod = new XAttribute("Город", infoFirmAdrG[1].ToString());
                        SvProdAdrRF.Add(SvProdGorod);
                    }
                    if (infoFirmAdrG[0].ToString() != "")
                    {
                        XAttribute SvProdStreet = new XAttribute("Улица", infoFirmAdrG[0].ToString());
                        SvProdAdrRF.Add(SvProdStreet);
                    }

                    //<Документ><СвСчФакт><СвПокуп>
                    XElement SvPokup = new XElement("СвПокуп");
                    SVSF.Add(SvPokup);

                    //<Документ><СвСчФакт><СвПокуп><ИдСв>
                    XElement SvPokupIdSv = new XElement("ИдСв");
                    SvPokup.Add(SvPokupIdSv);

                    if (infoKag[3].ToString().Length == 10)
                    {
                        //<Документ><СвСчФакт><СвПокуп><ИдСв><СвЮЛУч>             
                        XElement SvPokupSvUluch = new XElement("СвЮЛУч");
                        XAttribute SvPokupName = new XAttribute("НаимОрг", infoKag[12]);
                        XAttribute SvPokupINN = new XAttribute("ИННЮЛ", infoKag[3]);
                        XAttribute SvPokupKPP = new XAttribute("КПП", infoKag[4]);
                        SvPokupIdSv.Add(SvPokupSvUluch);
                        SvPokupSvUluch.Add(SvPokupName);
                        SvPokupSvUluch.Add(SvPokupINN);
                        SvPokupSvUluch.Add(SvPokupKPP);
                    }

                    if (infoKag[3].ToString().Length == 12)
                    {
                        //Сведения об индивидуальном предпринимателе, ИННФЛ(=12 симв)
                        XElement SvPokupSvIP = new XElement("СвИП");
                        XAttribute SvPokupInnFl = new XAttribute("ИННФЛ", infoKag[3]);
                        XElement SvPokupSvIPFIO = new XElement("ФИО");

                        NameOrFio = infoKag[1].ToString();
                        NameOrFio = NameOrFio.Replace("ИП", "");
                        NameOrFio = NameOrFio.Trim();
                        FamilijaIP = "";
                        ImjaIP = "";
                        OtchestvoIP = "";
                        FamilijaIP = NameOrFio.Substring(0, NameOrFio.IndexOf(' '));
                        ImjaIP = NameOrFio.Substring(NameOrFio.IndexOf(' ') + 1, 2);
                        OtchestvoIP = NameOrFio.Substring(NameOrFio.IndexOf('.') + 1, 2);

                        XAttribute SvPokupSvIPFamilija = new XAttribute("Фамилия", FamilijaIP);
                        XAttribute SvPokupSvIPImja = new XAttribute("Имя", ImjaIP);
                        XAttribute SvPokupSvIPOtchestvo = new XAttribute("Отчество", OtchestvoIP);

                        SvPokupIdSv.Add(SvPokupSvIP);
                        SvPokupSvIP.Add(SvPokupInnFl);
                        SvPokupSvIP.Add(SvPokupSvIPFIO);

                        SvPokupSvIPFIO.Add(SvPokupSvIPFamilija);
                        SvPokupSvIPFIO.Add(SvPokupSvIPImja);
                        SvPokupSvIPFIO.Add(SvPokupSvIPOtchestvo);
                    }

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
                    if (infoKag[15].ToString().Length > 0)
                    {
                        XAttribute SvPokupFlat = new XAttribute("Кварт", infoKag[15]);
                        SvPokupAdrRF.Add(SvPokupFlat);
                    }

                    //<Документ><СвСчФакт><ИнфПолФХЖ1>
                    XElement DopSvFHJ1 = new XElement("ДопСвФХЖ1");
                    XAttribute NaimOKV = new XAttribute("НаимОКВ", "Российский рубль");
                    //XAttribute ObstFormSchf = new XAttribute("ОбстФормСЧФ", "1");
                    SVSF.Add(DopSvFHJ1);
                    DopSvFHJ1.Add(NaimOKV);
                    //DopSvFHJ1.Add(ObstFormSchf);            

                    //<Документ><СвСчФакт><ИнфПолФХЖ1>
                    /*
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
                    XElement TxtInf5 = new XElement("ТекстИнф");
                    XAttribute TxtInf5Identif = new XAttribute("Идентиф", "GLN_грузополучателя");
                    XAttribute TxtInf5Znachen = new XAttribute("Значен", infoGpl[2]);
                    InfPolFHJ1.Add(TxtInf5);
                    TxtInf5.Add(TxtInf5Identif);
                    TxtInf5.Add(TxtInf5Znachen);
                    */

                    /************************** 3 уровень. <ТаблСчФакт> ************************/
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
                        /*
                        XElement AkcizRazn = new XElement("АкцизРазн");
                        XElement AkcizRaznSumUvel = new XElement("СумУвел", "0.00");
                        XElement AkcizRaznSumUm = new XElement("СумУм", "0.00");
                        SvedTov.Add(AkcizRazn);
                        if (summWoNds_A < summWoNds_B) AkcizRazn.Add(AkcizRaznSumUvel);
                        if (summWoNds_A > summWoNds_B) AkcizRazn.Add(AkcizRaznSumUm);
                        */
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
                        /* //у ИП нет справочников соответствия продукции
                        string nomBuyerCd = Verifiacation.GetBuyerItemCodeRcd(Convert.ToString(infoKag[5]), Convert.ToInt64(Item[i, 0]));

                        XElement InfPolFHJ21 = new XElement("ИнфПолФХЖ2");
                        XAttribute ItmTxtInf1Identif = new XAttribute("Идентиф", "код_материала");
                        XAttribute ItmTxtInf1Znachen = new XAttribute("Значен", nomBuyerCd);
                        SvedTov.Add(InfPolFHJ21);
                        InfPolFHJ21.Add(ItmTxtInf1Identif);
                        InfPolFHJ21.Add(ItmTxtInf1Znachen);
                        */
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

                        XElement DopSvedTov = new XElement("ДопСведТов");

                        XAttribute NaimEdIzmDo = new XAttribute("НаимЕдИзмДо", Verifiacation.GetEdIzm(Convert.ToString(Item[i, 4])));
                        XAttribute NaimEdIzmPosle = new XAttribute("НаимЕдИзмПосле", Verifiacation.GetEdIzm(Convert.ToString(Item[i, 14])));
                        SvedTov.Add(DopSvedTov);
                        DopSvedTov.Add(NaimEdIzmDo);
                        DopSvedTov.Add(NaimEdIzmPosle);

                        //<Документ><ТаблКСчФ><СведТов><ДопСведТов><НомСредИдентТов[До/После]><НомУпак>
                        if (Item[i, 10].ToString().Contains("10") && CurrDataUKD[2].ToString().Equals("BaseMark"))     //Item[i, 10] - налоговая ставка после (как бы)
                        {
                            string nomUpakValueDo = "020" + Item[i, 0] + "37" + (Math.Round(Convert.ToDecimal(Item[i, 8]))).ToString();             //Item[i, 8] - количество до
                            string nomUpakValuePosle = "020" + Item[i, 0] + "37" + (Math.Round(Convert.ToDecimal(Item[i, 18]))).ToString();         //Item[i, 18] - количество после
                            XElement NomSredIdentDo = new XElement("НомСредИдентТовДо");
                            XElement NomUpakDo = new XElement("НомУпак", nomUpakValueDo);
                            XElement NomSredIdentPosle = new XElement("НомСредИдентТовПосле");
                            XElement NomUpakPosle = new XElement("НомУпак", nomUpakValuePosle);
                            DopSvedTov.Add(NomSredIdentDo);
                            NomSredIdentDo.Add(NomUpakDo);
                            DopSvedTov.Add(NomSredIdentPosle);
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
                    DOC.Add(SodFHJ3);
                    XAttribute SodOper = new XAttribute("СодОпер", "Изменение стоимости товаров и услуг");
                    SodFHJ3.Add(SodOper);

                    string allPeredSf = infoCorSf[0].ToString() + " от " + Convert.ToDateTime(infoCorSf[1]).ToString(@"dd.MM.yyyy");

                    XElement PeredatDocum = new XElement("ПередатДокум");
                    SodFHJ3.Add(PeredatDocum);
                    XAttribute NaimOsnPeredatDocum = new XAttribute("НаимОсн", "УПД");
                    XAttribute NomOsnPeredatDocum = new XAttribute("НомОсн", infoCorSf[0].ToString());
                    XAttribute DataOsnPeredatDocum = new XAttribute("ДатаОсн", Convert.ToDateTime(infoCorSf[1]).ToString(@"dd.MM.yyyy"));
                    PeredatDocum.Add(NaimOsnPeredatDocum);
                    PeredatDocum.Add(NomOsnPeredatDocum);
                    PeredatDocum.Add(DataOsnPeredatDocum);

                    //<Документ><СодФХЖ3><ОснКор>
                    XElement DokumOsnKorr = new XElement("ДокумОснКор");
                    XAttribute NaimOsnDokumOsnKorr = new XAttribute("НаимОсн", "Без документа-основания");
                    SodFHJ3.Add(DokumOsnKorr);
                    DokumOsnKorr.Add(NaimOsnDokumOsnKorr);

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
                    //XAttribute dolj = new XAttribute("Должн", "Генеральный Директор");
                    Podp.Add(UL);
                    UL.Add(innUl);
                    UL.Add(naimOrg);
                    UL.Add(dolj);

                    //<Документ><Подписант><ЮЛ><ФИО>
                    XElement FIO = new XElement("ФИО");
                    XAttribute famdir = new XAttribute("Фамилия", infoSigner[1]);
                    XAttribute namedir = new XAttribute("Имя", infoSigner[2]);
                    XAttribute otchesdir = new XAttribute("Отчество", infoSigner[3]);

                    //XAttribute famdir = new XAttribute("Фамилия", "Еремин");
                    //XAttribute namedir = new XAttribute("Имя", "Дмитрий");
                    //XAttribute otchesdir = new XAttribute("Отчество", "Владимирович");

                    UL.Add(FIO);
                    FIO.Add(famdir);
                    FIO.Add(namedir);
                    FIO.Add(otchesdir);

                    //------сохранение документа-----------
                    fileName = fileName + ".xml";
                    string fileNameSokr = DateTime.Today.ToString(@"yyyyMMdd") + "_" + guid + ".xml";
                    try
                    {
                        xdoc.Save(pathArchiveEDI + fileName);
                        try
                        {
                            xdoc.Save(pathUPDEDI + fileName);
                            string message = "СКБ-Контур. УКД " + fileName + " создан в " + pathUPDEDI;
                            Program.WriteLine(message);
                            //имя файла слишком длинное, обрежим его до дата + гуид, поиск можно будет делать по регулярному выражению .*fileNameSokr
                            DispOrders.WriteProtocolEDI("УКД", fileNameSokr, infoKag[0] + " - " + infoKag[1], 0, infoGpl[0] + " - " + infoGpl[1], "УКД сформирован", DateTime.Now, Convert.ToString(CurrDataUKD[6]), "KONTUR");
                            DispOrders.WriteEDiSentDoc("8", fileNameSokr, Convert.ToString(CurrDataUKD[3]), Convert.ToString(infoSf[0]), "123", Convert.ToString(sumWthNds_V - sumWthNds_G), Convert.ToString(CurrDataUKD[7]), 1);
                        }
                        catch (Exception e)
                        {
                            string message_error = "СКБ-Контур. Не могу создать xml файл УКД в " + pathUPDEDI + ". Нет доступа или диск переполнен.";
                            DispOrders.WriteProtocolEDI("УКД", fileNameSokr, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УКД не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUKD[6]), "KONTUR");
                            Program.WriteLine(message_error);
                            DispOrders.WriteErrorLog(e.Message);
                        }

                        try
                        {
                            Program.WriteLine("Отправляем УКД в Диадок через Диадок API");
                            DiadocAuthenticate.SendInvoiceXml(pathUPDEDI, fileName, idPol, idOtpr, BoxIdPol, BoxIdOtpr, "UniversalCorrectionDocument", infoSf[0].ToString(), Convert.ToDateTime(infoSf[1]).ToString(@"dd.MM.yyyy"));  //Отправляем УКД в Диадок через Диадок API
                        }
                        catch (Exception e)
                        {
                            Program.WriteLine("Ошибка отправки УКД в Диадок через Диадок API");
                            Program.WriteLine(e.Message);
                        }
                    }
                    catch (Exception e)
                    {
                        string message_error = "СКБ-Контур. Не могу создать xml файл УКД в " + pathArchiveEDI + ". Нет доступа или диск переполнен.";
                        DispOrders.WriteProtocolEDI("УКД", fileNameSokr, infoKag[0] + " - " + infoKag[1], 10, infoGpl[0] + " - " + infoGpl[1], "УКД не сформирован. Нет доступа или диск переполнен.", DateTime.Now, Convert.ToString(CurrDataUKD[6]), "KONTUR");
                        Program.WriteLine(message_error);
                        DispOrders.WriteErrorLog(e.Message);
                        //запись в лог о неудаче
                    }
                }
                else
                {
                    Program.WriteLine("Невозможно отправить документ УКД через Диадок API, данные об отправителе отсутствуют");
                }
            }
            else
            {
                Program.WriteLine("Невозможно отправить документ УКД через Диадок API, данные о получателе отсутствуют");
            }
        }
        public static void SendFiles_toDiadoc()
        {
            //получение путей
            string ArchiveSKBKontur = DispOrders.GetValueOption("СКБ-КОНТУР.АРХИВ");
            string pathUPDISPRO = "\\\\EDI-MGN\\ZAKAZ\\EXPIMP\\InOut\\Diadoc\\out\\";

            string[] files = Directory.GetFiles(pathUPDISPRO, "ON_NSCHFDOPPR*.xml");
            XmlDocument doc = new XmlDocument();
            bool error_format;

            string idPol = ""; //ИдПолуч
            string BoxIdPol = ""; //ЯщикПолуч
            string idOtpr = ""; //ИдОтпр
            string BoxIdOtpr = ""; //ЯщикОтпр                       
            string IdProvaiderOtpr = "";
            IdProvaiderOtpr = DispOrders.GetValueOption("СКБ-КОНТУР.ИДЭДО"); //Id провайдера отправителя

            string[] PolInfo;
            string[] OtprInfo;

            object[] infoFirm;
            XmlNode singleNode;

            foreach (string sendfile in files)
            {
                error_format = false;//по умолчанию ошибок нет
                try
                {
                    doc.Load(sendfile);
                    Program.WriteLine("Для обработки взят файл " + Path.GetFileName(doc.BaseURI));
                }
                catch
                {
                    Program.WriteLine("Ошибка загрузки или синтаксического анализа в XML " + sendfile);
                    error_format = true;
                }
                if (error_format == false) //файл загружен, синтаксический анализ XML успешен
                {
                    singleNode = doc.SelectSingleNode("/Файл/СвУчДокОбор");
                    if (singleNode != null)
                    {
                        if (singleNode.Attributes["ИдПол"] != null)
                        {
                            idPol = singleNode.Attributes["ИдПол"].Value;
                            Program.WriteLine("Информация об получателе ID ЭДО " + idPol);
                        }
                        else
                        {
                            Program.WriteLine("ID ЭДО получателя не определен. Отправка документа не возможна");
                        }                        
                    }
                    else
                    {
                        Program.WriteLine("Узел СвУчДокОбор в файле не найден");
                    }
                    
                    PolInfo = DiadocAuthenticate.OrganizationInfo_forfnsParticipantId(idPol);
                    BoxIdPol = PolInfo[0];

                    String NumberSF = "";
                    String DateSF = "";

                    singleNode = doc.SelectSingleNode("/Файл/Документ/СвСчФакт");
                    if (singleNode != null)
                    {
                        if (singleNode.Attributes["НомерСчФ"] != null)
                        {
                            NumberSF = singleNode.Attributes["НомерСчФ"].Value;
                            Program.WriteLine("Номер СФ " + NumberSF);
                        }
                        else
                        {
                            Program.WriteLine("Номер СФ не определен. Отправка документа не возможна");
                        }
                        if (singleNode.Attributes["ДатаСчФ"] != null)
                        {
                            DateSF = singleNode.Attributes["ДатаСчФ"].Value;
                            Program.WriteLine("Дата СФ " + DateSF);
                        }
                        else
                        {
                            Program.WriteLine("Дата СФ не определена. Отправка документа не возможна");
                        }
                    }
                    else
                    {
                        Program.WriteLine("Узел СФ в файле не найден");
                    }
                    
                    if (NumberSF != "" && DateSF != "")
                    {
                        infoFirm = Verifiacation.GetFirmInfo(Convert.ToDateTime(DateSF).ToString(@"yyyyMMdd")); //0 CrtFrm_Nm, 1 CrtFrm_INN, 2 CrtFrm_KPP, 3 CrtFrm_OKPO            

                        Program.WriteLine("Информация об отправителе ИНН " + infoFirm[1].ToString() + " КПП " + infoFirm[2].ToString());
                        OtprInfo = DiadocAuthenticate.OrganizationInfo(infoFirm[1].ToString(), infoFirm[2].ToString(), IdProvaiderOtpr);
                        idOtpr = OtprInfo[0];
                        BoxIdOtpr = OtprInfo[1];

                        try
                        {
                            Program.WriteLine("Отправляем УПД в Диадок через Диадок API");
                            DiadocAuthenticate.SendInvoiceXml(pathUPDISPRO, Path.GetFileName(doc.BaseURI), idPol, idOtpr, BoxIdPol, BoxIdOtpr, "UniversalTransferDocument", NumberSF, Convert.ToDateTime(DateSF).ToString(@"dd.MM.yyyy"));  //Отправляем УПД в Диадок через Диадок API
                            string oldP = Path.GetFullPath(sendfile);
                            string newP = ArchiveSKBKontur + Path.GetFileName(doc.BaseURI);
                            Directory.Move(oldP, newP);
                        }
                        catch (Exception e)
                        {
                            Program.WriteLine("Ошибка отправки УПД в Диадок через Диадок API");
                            DispOrders.WriteErrorLog(e.Message);
                        }
                    }
                }
            }
        }

        public static void CreateKonturBaseSvod_UPD(List<object> CurrDataSf)  // 0 ProviderOpt, 1 ProviderZkg, 2 NastDoc_Fmt, 3 SklSf_Rcd, 4 SklSf_TpOtg, 5 SklSfA_RcdCor, 6 PrdZkg_NmrExt, 7 PrdZkg_Rcd, 8 PrdZkg_Dt, 9 SklNk_TDrvNm, 10 typeSf, 11 NISF, 12 sklnkDat, 13 sklnkNmr, 14 dtOtgr
        {
            string fNameXML = "ON_NSCHFDOPPR_";
            string XmlText = "empty";
            string sfGUID = "", IdPoluch = "", IdOtprav = "";
            int GuidTaked = 0;

            SqlConnection conn = new SqlConnection(Settings.Default.ConnStringISPRO);
            SqlDataReader dReader = null;
            SqlCommand command;

            Program.WriteLine("Формирование сводного УПД.");

            // получим более детальную инфу от СФ
            object[] detailInfoSf = Verifiacation.GetDataFromSF(Convert.ToInt64(CurrDataSf[3])); //0 SklSf_Nmr, 1 SklSf_Dt, 2 SklSf_KAgID, 3 SklSf_KAgAdr, 4 SklSf_RcvrID, 5 SklSf_RcvrAdr, 6 SVl_CdISO
            object[] infoKag = Verifiacation.GetDataFromPtnRCD_IP(Convert.ToInt64(detailInfoSf[2]), Convert.ToInt64(detailInfoSf[3])); // 0 Ptn_Cd, 1 Ptn_NmSh, 2 Filia_GLN, 3 Ptn_Inn, 4 Ptn_KPP, 5 ProdCode, 6 Filia_Adr, 7 Filia_Index, 8 Filia_Rgn, 9 Город, 10 Улица, 11 Дом, 12 Полное наименование, 12 Полное наименование
            object[] infoGpl = Verifiacation.GetDataFromPtnRCD_IP(Convert.ToInt64(detailInfoSf[4]), Convert.ToInt64(detailInfoSf[5]));

            conn.Open();

            try   //проверка GUID
            {
                string checkSfGUID = "SELECT prv.UF_RkValS sfGUID FROM dbo.UFPRV prv, dbo.UFRKV rkv WHERE rkv.UFR_RkRcd = prv.UF_RkRcd\n"; // проверка сф на наличие GUID
                checkSfGUID += $"AND prv.UF_TblId = rkv.UFR_DbRcd AND rkv.UFR_Id = 'U_sfGUID' and prv.UF_TblRcd = {CurrDataSf[3]}";
                command = new SqlCommand(checkSfGUID, conn);
                dReader = command.ExecuteReader();
                if (dReader.Read() == false)
                {
                    try
                    {
                        // get new GUID
                        sfGUID = Convert.ToString(Guid.NewGuid());
                        dReader.Close();
                        string insertGUID = $"insert into dbo.UFPRV select n.UFR_DbRcd,{CurrDataSf[3]},n.UFR_RkRcd,'{sfGUID}',0,0,0 ";
                        insertGUID += "from dbo.UFRKV n where n.UFR_Id = 'U_sfGUID'";
                        command = new SqlCommand(insertGUID, conn);
                        GuidTaked = command.ExecuteNonQuery();
                        conn.Close();
                    }
                    catch (Exception ex) { DispOrders.WriteErrorLog(ex.Message + " Источник: " + ex.Source); }
                }
                else
                {
                    // get existing GUID for file name
                    object[] results = new object[dReader.VisibleFieldCount];
                    dReader.GetValues(results);
                    sfGUID = results[0].ToString();
                    GuidTaked = 1;
                }
                dReader.Close();
            }
            catch (Exception ex) { DispOrders.WriteErrorLog(ex.Message + " Источник: " + ex.Source); }

            if (conn.State != ConnectionState.Open) conn.Open();

            // если GUID получен то далее получаем IdPoluch and IdOtprav
            if (GuidTaked > 0)
            {
                try  //-----------------------------------IdPoluch
                {
                    string selectIdUserEdo = "select v.UF_RkValS from dbo.UFPRV v,dbo.UFRKV n where n.UFR_RkRcd = v.UF_RkRcd and v.UF_TblId = n.UFR_DbRcd ";
                    selectIdUserEdo += $"and n.UFR_Id = 'U_IdUserEDO' and UF_TblRcd = {detailInfoSf[2]}";    // SklSf_KAgId
                    command = new SqlCommand(selectIdUserEdo, conn);
                    dReader = command.ExecuteReader();
                    if (dReader.Read())
                    {
                        object[] result = new object[dReader.VisibleFieldCount];
                        dReader.GetValues(result);
                        IdPoluch = result[0].ToString();
                    }
                    else Program.WriteLine("Отсутствует код участника ЭДО у данного контрагента!");
                    dReader.Close();
                }
                catch (Exception ex) { DispOrders.WriteErrorLog(ex.Message + " Источник: " + ex.Source); }


                try  //-----------------------------------IdOtprav
                {
                    string selectIdOtp = "SELECT UFS_Nm FROM UFLstSpr t LEFT JOIN UFSPR spr ON spr.UFS_Rcd = t.UFS_Rcd AND spr.UFS_CdS = 'ИдОтп' ";
                    selectIdOtp += "WHERE t.UFS_CdSpr = 119";
                    command = new SqlCommand(selectIdOtp, conn);
                    dReader = command.ExecuteReader();
                    if (dReader.Read())
                    {
                        object[] result = new object[dReader.VisibleFieldCount];
                        dReader.GetValues(result);
                        IdOtprav = result[0].ToString();
                    }
                    dReader.Close();
                }
                catch (Exception ex) { DispOrders.WriteErrorLog(ex.Message + " Источник: " + ex.Source); }

                // если получили IdPoluch and IdOtprav то выполняем T-SQL функцию, получаем XML-текст и сохраняем в файл
                if (!String.IsNullOrEmpty(IdPoluch) && !String.IsNullOrEmpty(IdOtprav))
                {
                    try
                    {
                        fNameXML += (IdPoluch + "_" + IdOtprav + "_" + DateTime.Now.ToString("yyyyMMdd") + "_" + sfGUID).Trim(' ');
                        string execTableFunction = "SELECT NoErr,ErrDescription,Vers,cast(xDoc as varchar(max)) xx \n";
                        execTableFunction += $"FROM [dbo].[ItXmlNSCHFDOPPR22]({CurrDataSf[3]},'{CurrDataSf[10]}','{fNameXML}')";

                        command = new SqlCommand(execTableFunction, conn);
                        dReader = command.ExecuteReader();

                        if (dReader.Read())
                        {
                            object[] results = new object[dReader.VisibleFieldCount];
                            dReader.GetValues(results);
                            if (Convert.ToInt32(results[0]) == 0)   // If No Errors in T-SQL table-function
                            {
                                XmlText = "<?xml version=\"1.0\" encoding=\"windows-1251\"?>";
                                XmlText += results[3].ToString();
                            }
                            else
                            {
                                string ErrMessage = "Ошибка в при выполнении функции ItXmlNSCHFDOPPR22: " + results[1].ToString();   // ErrDescription 
                                Program.WriteLine(ErrMessage);
                                throw new Exception(ErrMessage);
                            }

                        }
                        dReader.Close();
                    }
                    catch (Exception exception) { DispOrders.WriteErrorLog(exception.Message + " Источник: " + exception.Source); }

                    // если XML текст получен, то формируем файл и сохраняем его
                    if (!(XmlText.Equals("empty")))
                    {
                        try
                        {
                            string xmlPath = @"C:\Temp\";
                            if (!dReader.IsClosed) dReader.Close();

                            // взятие нужного пути для файла
                            string selectDefaultXMLPath = "SELECT UFS_Nm FROM UFLstSpr t LEFT JOIN UFSPR spr ON spr.UFS_Rcd = t.UFS_Rcd AND spr.UFS_CdS = 'Каталог' ";
                            selectDefaultXMLPath += "WHERE t.UFS_CdSpr = 119";
                            command = new SqlCommand(selectDefaultXMLPath, conn);
                            dReader = command.ExecuteReader();
                            if (dReader.Read())
                            {
                                object[] result = new object[dReader.VisibleFieldCount];
                                dReader.GetValues(result);
                                xmlPath = result[0].ToString();
                            }
                            dReader.Close();

                            // создание файла и запись xml-текста в файл
                            fNameXML = xmlPath + fNameXML + ".xml";
                            Stream stream = File.OpenWrite(fNameXML);
                            StreamWriter streamWriter = new StreamWriter(stream, Encoding.GetEncoding("windows-1251"));
                            streamWriter.Write(XmlText);
                            streamWriter.Close();

                            // взятие суммы с НДС для WriteEDISentDoc
                            string sumWthNds = "0.00";
                            XmlDocument document = new XmlDocument();
                            document.Load(fNameXML);
                            XmlNode needNode = document.SelectSingleNode(@"/Файл/Документ/ТаблСчФакт/ВсегоОпл");
                            if (needNode != null) sumWthNds = needNode.Attributes["СтТовУчНалВсего"].Value;

                            // иформирование о завершении и запись в протокол и таблицу SentDoc 
                            string fileNameSokr = DateTime.Now.ToString("yyyyMMdd") + "_" + sfGUID + ".xml";
                            Program.WriteLine("Сводный УПД сформирован в файл: " + fileNameSokr);
                            DispOrders.WriteProtocolEDI("УПД", fileNameSokr, infoKag[0] + " - " + infoKag[1], 0, infoGpl[0] + " - " + infoGpl[1], "УПД сформирован", DateTime.Now, Convert.ToString(CurrDataSf[6]), "KONTUR");
                            DispOrders.WriteEDiSentDoc("8", fileNameSokr, Convert.ToString(CurrDataSf[3]), Convert.ToString(detailInfoSf[0]), "123", sumWthNds, Convert.ToString(CurrDataSf[7]), 1);
                            //resultCode = 2;
                        }
                        catch (Exception ex) { DispOrders.WriteErrorLog(ex.Message + " Источник: " + ex.Source); }
                    }
                }
            }

            conn.Close();
            //return resultCode;          // 0 - Ошибка, 1 - удалось сформировать XML-текст, 2 - удалось сохранить XML-файл

            // такая проверка есть в коде скрипта отчета, но по-моему она не нужна вообще
            /*string SklSf_Type = "1";  
            SqlCommand checkSklSfType = new SqlCommand($"SELECT SklSf_Type FROM SKLSF WHERE SklSf_Rcd = {CurrDataSf[3]}",conn);
            dReader = checkSklSfType.ExecuteReader();
            if (dReader.Read())
            {
                object[] result = new object[dReader.VisibleFieldCount];
                dReader.GetValues(result); 
                SklSf_Type = result[0].ToString(); 
            }
            if (SklSf_Type.Equals("2")) FuncDoc = "СЧФ";*/

        }

    }
}

