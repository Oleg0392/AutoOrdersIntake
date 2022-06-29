using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;
using System.IO;
using System.Configuration;
using System.Diagnostics;
using System.Data.SqlClient;
using System.Data;
using ICSharpCode.SharpZipLib;
using Excel;

namespace AutoOrdersIntake
{
    class Proviant
    {
        public static void IntakeOrders()
        {
            //string _path = DispOrders.GetValueOption("ПРОВИАНТ.ЗАКАЗ");
            string _path = "\\\\fileshare\\EXPIMP\\Milk\\Loading\\Proviant\\";
            string ArchiveProviant = DispOrders.GetValueOption("ПРОВИАНТ.АРХИВ");

            string JurSootvKA = DispOrders.GetValueOption("ПРОВИАНТ.ЖУРНАЛ КА");
            string  JurSootvProd = DispOrders.GetValueOption("ПРОВИАНТ.ЖУРНАЛ ПРОДУКЦИИ");

            int RowDelivery = Convert.ToInt32(DispOrders.GetValueOption("ПРОВИАНТ.СТРОКА ДОСТАВКИ"));
            int ColumnDelivery = Convert.ToInt32(DispOrders.GetValueOption("ПРОВИАНТ.КОЛОНКА ДОСТАВКИ"));
            int RowDateDelivery = Convert.ToInt32(DispOrders.GetValueOption("ПРОВИАНТ.СТРОКА ДД"));
            int ColumnDateDelivery = Convert.ToInt32(DispOrders.GetValueOption("ПРОВИАНТ.КОЛОНКА ДД"));
            int RowStartBarCode = Convert.ToInt32(DispOrders.GetValueOption("ПРОВИАНТ.СТРОКА ШТРИХКОДА"));
            int ColumnStartBarCode = Convert.ToInt32(DispOrders.GetValueOption("ПРОВИАНТ.КОЛОНКА ШТРИХКОДА"));
            int ColumnStartItems = Convert.ToInt32(DispOrders.GetValueOption("ПРОВИАНТ.КОЛОНКА КОЛИЧЕСТВО"));

            
            int CntUsedRow = 0;//количество позиций заказа из эксель не всегда может быть правильным, т.к. ячейки могут быть пустыми
            string[] files = Directory.GetFiles(_path, "*.xls");
            foreach (string parsfile in files)
            {
                try
                {
                    FileStream stream = File.Open(parsfile, FileMode.Open, FileAccess.Read);//try catch
                    IExcelDataReader excelReader = ExcelReaderFactory.CreateBinaryReader(stream);
                    //IExcelDataReader excelReader = ExcelReaderFactory.CreateOpenXmlReader(stream);
                    excelReader.IsFirstRowAsColumnNames = false;
                    DataSet result = excelReader.AsDataSet();
                    excelReader.Close();
                    try
                    {
                        object[] res_verf_buyer;
                        DispOrders.ClearTmpZkg();//очищаем временую таблицу
                        string PtnAccord_Nm = Convert.ToString(result.Tables[0].Rows[RowDelivery][ColumnDelivery]).Remove(0, 15);//адрес доставки

                        string sdate = Convert.ToString(result.Tables[0].Rows[RowDateDelivery][ColumnDateDelivery]);
                        int L = sdate.Length;


                        DateTime date_delivery = DateTime.Parse((Convert.ToString(result.Tables[0].Rows[RowDateDelivery][ColumnDateDelivery]).Remove(0, 13)));//дата доставки
                        object[] res_verf_deliv = Verifiacation.Verification_Delivery_Proviant(PtnAccord_Nm, JurSootvKA);
                        if ((res_verf_deliv[0] != null) && (date_delivery > DateTime.Now))
                        {
                            string dd = date_delivery.ToString(@"yyyyMMdd");
                            res_verf_buyer = Verifiacation.Verification_Plat_Proviant(res_verf_deliv[1].ToString());//данные о плательщике(провиант или купец)
                            if (res_verf_buyer[0] != null)
                            {
                                //range = wsheet.UsedRange;
                                CntUsedRow = result.Tables[0].Rows.Count - 1;



                                for (int i = Convert.ToInt32(RowStartBarCode); i < CntUsedRow; i++)//считываем штрих коды и количество
                                {
                                    string CurrBarCode = Convert.ToString(result.Tables[0].Rows[i][ColumnStartBarCode]);
                                    string CurrQt = Convert.ToString(result.Tables[0].Rows[i][ColumnStartItems]);

                                    CurrQt = CurrQt.Replace(",", ".");

                                    if ((CurrBarCode != "") && (CurrQt != ""))//есть штрих код и количество
                                    {
                                        object[] res_verf_item = Verifiacation.Verification_gtin_xls(CurrBarCode, JurSootvProd);//проверка штрих кода
                                        if (res_verf_item[0] != null)
                                        {
                                            object[] PriceList = Verifiacation.GetPriceList(Convert.ToString(res_verf_deliv[0]), Convert.ToInt32(res_verf_item[5]));
                                            DispOrders.RecordToTmpZkg(Convert.ToString(res_verf_buyer[0]), Convert.ToString(res_verf_deliv[0]), dd, Convert.ToString(res_verf_item[1]), Convert.ToString(res_verf_item[4]), CurrQt, DateTime.Today.ToString("yyyyMMdd"), " ", Convert.ToString(PriceList[0]), Convert.ToInt16(res_verf_item[5]), Path.GetFileName(parsfile), Convert.ToString(PriceList[1]));
                                        }
                                        else
                                        {
                                            DispOrders.WriteOrderLog("Excel-Провиант", res_verf_buyer[0] + " - " + res_verf_buyer[1], res_verf_deliv[0] + " - " + res_verf_deliv[1], Path.GetFileName(parsfile), "  ", 1, "не найден штрих-код:" + CurrBarCode, DateTime.Today, DateTime.Now, 0);
                                            Console.WriteLine("Не найден штрих код товара: " + CurrBarCode + "");
                                        }
                                    }
                                    else
                                    {
                                        DispOrders.WriteOrderLog("Excel-Провиант", res_verf_buyer[0] + " - " + res_verf_buyer[1], res_verf_deliv[0] + " - " + res_verf_deliv[1], Path.GetFileName(parsfile), " ", 6, "Файл с заказом содержит пустые ячейки для штрихкода или количества ", DateTime.Today, DateTime.Now, 6);
                                        Console.WriteLine("Файл с заказом содержит пустые ячейки для штрихкода или количества");
                                    }
                                }

                                //перенос на постоянку
                                string[] buyer = Array.ConvertAll<object, string>(res_verf_buyer, delegate(object from) { return Convert.ToString(from); });
                                string[] deliv = Array.ConvertAll<object, string>(res_verf_deliv, delegate(object from) { return Convert.ToString(from); });
                                DispOrders.TMPtoPrdZkg(buyer, deliv, Path.GetFileName(parsfile), "Excel-Провиант", " ");


                                string oldPath = Path.GetFullPath(parsfile);
                                string newPath = ArchiveProviant + (DateTime.Now).ToString("ddMMyyyy_HHmmss") + "_" + Path.GetFileName(parsfile);

                                Directory.Move(oldPath, newPath);
                            }
                            else
                            {
                                DispOrders.WriteOrderLog("Excel-Провиант", " ", " ", Path.GetFileName(parsfile), " ", 3, "Не найден плательщик" + res_verf_deliv[1].ToString(), DateTime.Today, DateTime.Now, 6);
                                Console.WriteLine("Не найден плательщик. Проверьте поле Ptn_RcdPlat в таблице PTNRK c Ptn_Cd = " + res_verf_deliv[1].ToString() + " ");
                                Console.WriteLine("----------------");

                                string oldPath = Path.GetFullPath(parsfile);
                                string newPath = ArchiveProviant + (DateTime.Now).ToString("ddMMyyyy_HHmmss") + "_" + Path.GetFileName(parsfile);

                                Directory.Move(oldPath, newPath);
                            }
                        }
                        else//если неправильная дата доставки или не найден адрес доставки в таблице соответствий
                        {
                            if ((res_verf_deliv[0] == null) && (date_delivery >= DateTime.Now))
                            {
                                DispOrders.WriteOrderLog("Excel-Провиант", res_verf_deliv[0] + " - " + res_verf_deliv[1], " ", Path.GetFileName(parsfile), " ", 2, "Не найден адрес доставки: " + PtnAccord_Nm, DateTime.Today, DateTime.Now, 6);
                                Console.WriteLine("Не найден адрес доставки");
                            }
                            if ((date_delivery <= DateTime.Now) && (res_verf_deliv[0] != null))
                            {
                                DispOrders.WriteOrderLog("Excel-Провиант", res_verf_deliv[0] + " - " + res_verf_deliv[1], " ", Path.GetFileName(parsfile), " ", 5, "Неверная дата доставки", DateTime.Today, DateTime.Now, 6);
                                Console.WriteLine("Неправильная дата доставки");
                            }
                            else
                            {
                                DispOrders.WriteOrderLog("Excel-Провиант", res_verf_deliv[0] + " - " + res_verf_deliv[1], " ", Path.GetFileName(parsfile), " ", 2 - 5, "Неверные дата доставки и адрес доставки ", DateTime.Today, DateTime.Now, 6);
                                Console.WriteLine("Неправильная дата доставки и не найден адрес доставки");
                            }


                            //перемещение файла
                            string oldPath = Path.GetFullPath(parsfile);
                            string newPath = ArchiveProviant + (DateTime.Now).ToString("ddMMyyyy_HHmmss") + "_" + Path.GetFileName(parsfile);
                            Directory.Move(oldPath, newPath);
                        }

                    }
                    catch (IOException e)
                    {
                        Console.WriteLine(e);
                        DispOrders.WriteErrorLog(Convert.ToString(e));
                        DispOrders.WriteErrorLog("Поврежденный файл заказа: " + parsfile);

                        //перемещение файла
                        string oldPath = Path.GetFullPath(parsfile);
                        string newPath = ArchiveProviant + (DateTime.Now).ToString("ddMMyyyy_HHmmss") + "_" + Path.GetFileName(parsfile);
                        Directory.Move(oldPath, newPath);
                    }
                    
                }
                catch(IOException e)
                {
                    Console.WriteLine(e);
                    DispOrders.WriteErrorLog(Convert.ToString(e));
                }
                }
            }
        }
}
