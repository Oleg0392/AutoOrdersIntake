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
    class Tander
    {
        public static void IntakeOrders()
        {
            //получение настроек из базы
            string _path = DispOrders.GetValueOption("ТАНДЕР.ЗАКАЗ");
            string cd_buyer = DispOrders.GetValueOption("ТАНДЕР.ПЛАТЕЛЬЩИК");
            string ArchiveTander = DispOrders.GetValueOption("ТАНДЕР.АРХИВ");

            string JurSootvProd = DispOrders.GetValueOption("ТАНДЕР.ЖУРНАЛ ПРОДУКЦИИ");
            string JurSootvKA = DispOrders.GetValueOption("ТАНДЕР.ЖУРНАЛ КА");
            int StartRowCode = Convert.ToInt32(DispOrders.GetValueOption("ТАНДЕР.СТРОКА ШТРИХКОДА"));
            int StartColumnCode = Convert.ToInt32(DispOrders.GetValueOption("ТАНДЕР.КОЛОНКА ШТРИХКОДА"));
            int StartRowDelivery = Convert.ToInt32(DispOrders.GetValueOption("ТАНДЕР.СТРОКА ДОСТАКИ"));
            int StartColumnDelivery = Convert.ToInt32(DispOrders.GetValueOption("ТАНДЕР.КОЛОНКА ДОСТАВКИ"));
            int StartRowItems = Convert.ToInt32(DispOrders.GetValueOption("ТАНДЕР.СТРОКА КОЛИЧЕСТВО"));
            int StartColumsItems = Convert.ToInt32(DispOrders.GetValueOption("ТАНДЕР.КОЛОНКА КОЛИЧЕСТВО"));



            
                int CntProdExl = 0;//количество позиций заказа из эксель не всегда может быть правильным, т.к. ячейки могут быть пустыми
                int CntDelivExl = 0;//количество адресов доставки из эксель не всегда может быть правильным, т.к. ячейки могут быть пустыми
                int p = 0;
                int d = 0;
                int L;
                DateTime date_delivery;
                string[] files = Directory.GetFiles(_path, "*.xls");
                foreach (string parsfile in files)
                {
                    try
                    {
                        FileStream stream = File.Open(parsfile, FileMode.Open, FileAccess.Read);//try catch
                        IExcelDataReader excelReader = ExcelReaderFactory.CreateBinaryReader(stream);
                        DataSet result = excelReader.AsDataSet();
                        excelReader.Close();

                        try
                        {
                             string sdate = Convert.ToString(result.Tables[0].Rows[0][0]);
                             L = sdate.Length;
                             date_delivery = Convert.ToDateTime((Convert.ToString(result.Tables[0].Rows[0][0])).Remove(0, L - 10)); //дата доставки
                             string[] res_verf_buyer = Verifiacation.Verification_Tander_Buyer(cd_buyer);
                             if (date_delivery > DateTime.Now)
                             {

                                 CntProdExl = result.Tables[0].Rows.Count - 3;
                                 CntDelivExl = result.Tables[0].Columns.Count - 3;


                                 List<string> CodeProd = new List<string>();
                                 List<string> CodeDeliv = new List<string>();



                                 for (int i = StartRowCode; i <= CntProdExl; i++)//считывание в массив штрих кодов и их верификация
                                 {
                                     string CurrCell = Convert.ToString(result.Tables[0].Rows[i][StartColumnCode]);
                                     if (CurrCell != "")
                                     {

                                         CodeProd.Add(CurrCell);
                                     }
                                     else
                                     {
                                         break;
                                     }
                                     p++;
                                 }

                                 for (int i = 0; i < CntDelivExl; i++)//считывание кода адресов доставки в массив и их верификация
                                 {
                                     string CurrCell = Convert.ToString(result.Tables[0].Rows[StartRowDelivery][StartColumnDelivery + i]);
                                     if (CurrCell != "")
                                     {
                                         CodeDeliv.Add(CurrCell);
                                     }
                                     else
                                     {
                                         break;
                                     }
                                     d++;
                                 }

                                 for (int i = 0; i < CodeDeliv.Count; i++)
                                 {
                                     if (CodeDeliv[i] != "")
                                     {
                                         string[] res_verf_deliv = Verifiacation.Verification_Tander(CodeDeliv[i], JurSootvKA);//верификация адреса доставки
                                         if (res_verf_deliv[0] != null)
                                         {
                                             DispOrders.ClearTmpZkg();//очищаем временную таблицу с заказом от конкретной предыдущей точки 
                                             Program.WriteLine("Грузополучатель: " + res_verf_deliv[0] + "-" + res_verf_deliv[1]);// +"-"+res_verf_deliv[2]);
                                             Program.WriteLine("Адрес: " + res_verf_deliv[2]);
                                             Program.WriteLine("                 ");
                                             for (int j = 0; j < CodeProd.Count; j++)
                                             {
                                                 string qt = Convert.ToString(result.Tables[0].Rows[j + StartRowItems][i + StartColumsItems]);//количество товара
                                                 if (qt != "")
                                                 {
                                                     object[] res_verf_item = Verifiacation.Verification_gtin_xls(CodeProd[j], JurSootvProd);
                                                     //if (res_verf_item[0] != null) //товар найден в таблице соответсвий
                                                     if (String.IsNullOrWhiteSpace(Convert.ToString(res_verf_item[0])))
                                                     {
                                                         DispOrders.WriteOrderLog("Excel-Тандер", res_verf_buyer[0] + " - " + res_verf_buyer[1], res_verf_deliv[0] + " - " + res_verf_deliv[1], Path.GetFileName(parsfile), "  ", 1, "не найден штрих-код товара:" + CodeProd[j] + ". Проверте журнал соответсвий Тандер.", DateTime.Today, DateTime.Now, 0);
                                                         Program.WriteLine("Ошибка в штрих коде товара. Проверте журнал соответсвий Тандер");
                                                     }
                                                     else
                                                     {
                                                         object[] PriceList = Verifiacation.GetPriceList(res_verf_deliv[0], Convert.ToInt32(res_verf_item[5]));
                                                         DispOrders.RecordToTmpZkg(Convert.ToString(res_verf_buyer[0]), Convert.ToString(res_verf_deliv[0]), Convert.ToString(date_delivery), Convert.ToString(res_verf_item[1]), Convert.ToString(res_verf_item[4]), qt, Convert.ToString(DateTime.Today), " ", Convert.ToString(PriceList[0]), Convert.ToInt16(res_verf_item[5]), Path.GetFileName(parsfile), Convert.ToString(PriceList[1]));
                                                     }
                                                 }
                                             }

                                             ///!!!!перенос на постоянку!!!!!!!!!!
                                             DispOrders.TMPtoPrdZkg(res_verf_buyer, res_verf_deliv, Path.GetFileName(parsfile), "Тандер-Excel", " ");
                                         }
                                         else
                                         {
                                             DispOrders.WriteOrderLog("Тандер-Excel", res_verf_buyer[0] + " - " + res_verf_buyer[1], " ", Path.GetFileName(parsfile), " ", 2, "Не найден адрес доставки: " + CodeDeliv[i], DateTime.Today, DateTime.Now, 0);
                                             Program.WriteLine("Ошибка- в базе нет такой точки доставки!(Тандер)");
                                         }
                                     }
                                     else
                                     {
                                         break;
                                     }
                                 }
                             }
                             else
                             {
                                 DispOrders.WriteOrderLog("Тандер-Excel", res_verf_buyer[0] + " - " + res_verf_buyer[1], " ", Path.GetFileName(parsfile), " ", 5, "Неверная дата доставки: ", DateTime.Today, DateTime.Now, 0);
                                 Program.WriteLine("Неверная дата доставки!");
                             }

                             //перемещение файла
                             string oldPath = Path.GetFullPath(parsfile);
                             string newPath = ArchiveTander + (DateTime.Now).ToString("ddMMyyyy_HHmmss") + "_" + Path.GetFileName(parsfile);
                             Directory.Move(oldPath, newPath.Replace(",","_"));
                        }
                        catch(IOException e)
                        {
                            Program.WriteLine(Convert.ToString(e));
                            DispOrders.WriteErrorLog(Convert.ToString(e));
                            DispOrders.WriteErrorLog("Поврежденный файл заказа: " + parsfile);

                            //перемещение файла
                            string oldPath = Path.GetFullPath(parsfile);
                            string newPath = ArchiveTander + (DateTime.Now).ToString("ddMMyyyy_HHmmss") + "_" + Path.GetFileName(parsfile);
                            Directory.Move(oldPath, newPath.Replace(",", "_"));
                        }   
                    }
                    catch (IOException e)
                    {
                        Program.WriteLine(Convert.ToString(e));
                        DispOrders.WriteErrorLog(Convert.ToString(e));
                    }

                }
            }
    }
}

