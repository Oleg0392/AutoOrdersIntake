using System.Text;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;
using System.IO;
using System.Configuration;
using System.Diagnostics;
using System.Data.SqlClient;
using System.Data;
using ICSharpCode.SharpZipLib;
using Excel;
using System.Threading.Tasks;
using System.Text.RegularExpressions;


namespace AutoOrdersIntake
{
    class AnotherBuyers
    {
        public static void IntakeOrders_TandEx()
        {
            //найдем сколько всего алгоритмов в системе
            int qua = CountPtn();
            //получим список контрагентов и алгоритмы, по которым они должны разбираться
            int[,] Ptn = GetPtnRcdAlg(qua);
            for (int ji = 0; ji < qua; ji++)
            {
                //получение настроек из базы по конкретному ка
                string cd_buyer = AnotherBuyers.GetPtnCdAlg(Ptn[ji,1]);//плательщик
                string Orders_path = AnotherBuyers.GetSettingPath(Ptn[ji,0], "PathOrd");//папку с заказами
                string Archive_path = AnotherBuyers.GetSettingPath(Ptn[ji,0], "PathArc");//папка с архивами
                int JurSootvProd = AnotherBuyers.GetSettingPtn(Ptn[ji,0], "Jurprod");
                int JurSootvKA = AnotherBuyers.GetSettingPtn(Ptn[ji,0], "JurKA");
                int RowDD = AnotherBuyers.GetSettingPtn(Ptn[ji,0], "Row_Dt");
                int ColumnDD = AnotherBuyers.GetSettingPtn(Ptn[ji,0], "Col_Dt");
                int StartRowCode = AnotherBuyers.GetSettingPtn(Ptn[ji,0], "Row_CodeProd");
                int StartColumnCode = AnotherBuyers.GetSettingPtn(Ptn[ji,0], "Col_CodeProd");
                int StartRowDelivery = AnotherBuyers.GetSettingPtn(Ptn[ji,0], "Row_CodeKa");
                int StartColumnDelivery = AnotherBuyers.GetSettingPtn(Ptn[ji,0], "Col_CodeKa");
                int StartRowItems = AnotherBuyers.GetSettingPtn(Ptn[ji,0], "Row_vol");
                int StartColumsItems = AnotherBuyers.GetSettingPtn(Ptn[ji,0], "Col_vol");

                int CntProdExl = 0;//количество позиций заказа из эксель не всегда может быть правильным, т.к. ячейки могут быть пустыми
                int CntDelivExl = 0;//количество адресов доставки из эксель не всегда может быть правильным, т.к. ячейки могут быть пустыми
                int p = 0;
                int d = 0;
                bool error = false;
                string error_as_str = "";
                DateTime date_delivery;
                object buf_date_delivery;
                string[] files = Directory.GetFiles(Orders_path, "*.xls*");

                //начинаем парсить файл
                foreach (string parsfile in files)
                {
                    try
                    {
                        FileStream stream = File.Open(parsfile, FileMode.Open, FileAccess.Read);//try catch
                        IExcelDataReader excelReader;
                        if (parsfile.Substring(parsfile.LastIndexOfAny(new char[] {'.'})) == ".xls") //в зависимости от расширения файла, инициализация происходит разными способами
                        {                                                                                
                            excelReader = ExcelReaderFactory.CreateBinaryReader(stream);                 
                        }
                        else
                        {                                                                          
                            excelReader = ExcelReaderFactory.CreateOpenXmlReader(stream);          
                        }

                        DataSet result = excelReader.AsDataSet(); //считали информацию из файла
                        excelReader.Close();  //закрыли
                        //проверить на существование колонок
                        CheckColumnsAndRows(result, RowDD, ColumnDD, StartRowCode, StartColumnCode, StartRowDelivery, StartColumnDelivery, StartRowItems, StartColumsItems, ref error_as_str, ref error);
                       
                        if(error)
                        {
                            string[] res_verf_buyer = Verifiacation.Verification_Tander_Buyer(cd_buyer);
                            DispOrders.WriteOrderLog("Excel", res_verf_buyer[0] + " - " + res_verf_buyer[1], " ", Path.GetFileName(parsfile), " ", 5, "Неверный формат файла: " + Path.GetFileName(parsfile) + ". ("+ error_as_str + "), файл заказа пропущен и перенесен в архив ", DateTime.Today, DateTime.Now, 0);
                            Program.WriteLine("Неверный формат файла(" + error_as_str + "). Файл перенесен в архив ");

                            //перемещение файла
                            string oldPath = Path.GetFullPath(parsfile);
                            string newPath = Archive_path + (DateTime.Now).ToString("ddMMyyyy_HHmmss") + "_" + Path.GetFileName(parsfile);
                            Directory.Move(oldPath, newPath.Replace(",", "_"));

                            continue;
                        }



                        buf_date_delivery = result.Tables[0].Rows[RowDD][ColumnDD]; //получим дату
                       
                        //преобразование числа в дату
                        if (buf_date_delivery.GetType() == typeof(Double)) //если дата как число
                        {
                            double dat = double.Parse(Convert.ToString(result.Tables[0].Rows[RowDD][ColumnDD]));
                            date_delivery = DateTime.FromOADate(dat);
                        }
                        else if (buf_date_delivery.GetType() == typeof(String)) //если дата как строка
                        {
                            Regex newReg = new Regex(@"\d{2}\.\d{2}\.\d{4}"); //дата в виде dd.mm.yyyy
                            MatchCollection matches = newReg.Matches(Convert.ToString(buf_date_delivery)); //некоторые заказы могут иметь в строке даты слова.
                            if (matches.Count == 1) //если найдено одно совпадение
                                date_delivery = Convert.ToDateTime(matches[0].Value);
                            else
                            {
                                DispOrders.WriteErrorLog("Не смог получить дату заказа в файле " + parsfile);
                                //перемещение файла
                                string oldPath = Path.GetFullPath(parsfile);
                                string newPath = Archive_path + (DateTime.Now).ToString("ddMMyyyy_HHmmss") + "_" + Path.GetFileName(parsfile);
                                Directory.Move(oldPath, newPath.Replace(",", "_"));

                                error = true; //есть ошибка
                                date_delivery = new DateTime(); //для порядка
                            }
                               
                        }
                        else
                            date_delivery = Convert.ToDateTime(buf_date_delivery);

                        try
                        {
                            
                            string[] res_verf_buyer = Verifiacation.Verification_Tander_Buyer(cd_buyer);
                            if (!error || date_delivery > DateTime.Now) //дата заказа должна быть больше сегодняшнего дня (заказы принимаются будущей датой)
                            {

                                CntProdExl = result.Tables[0].Rows.Count - 1; //получим количество строк в файле, уменьшаем на единицу (отсчитываем от 0)
                                CntDelivExl = result.Tables[0].Columns.Count - 1; //получим количество столбцов в файле (отсчитываем от 0)

                                List<string> CodeProd = new List<string>(); //будет содержать список продуктов
                                List<string> CodeDeliv = new List<string>(); //будет содержать список грузополучателей

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

                                for (int i = StartColumnDelivery; i <= CntDelivExl; i++)//считывание кода адресов доставки в массив и их верификация
                                {
                                    string CurrCell = Convert.ToString(result.Tables[0].Rows[StartRowDelivery][i]);
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

                                for (int i = 0; i < CodeDeliv.Count; i++) //массив грузополучателей 
                                {
                                    if (CodeDeliv[i] != "") //если что-то есть в грузополучателях
                                    {
                                        string[] res_verf_deliv = Verifiacation.Verification_Tander(CodeDeliv[i], JurSootvKA.ToString());//верификация адреса доставки
                                        if (res_verf_deliv[0] != null)
                                        {
                                            DispOrders.ClearTmpZkg();//очищаем временную таблицу с заказом от конкретной предыдущей точки 
                                            Program.WriteLine("Грузополучатель: " + res_verf_deliv[0] + "-" + res_verf_deliv[1]);// +"-"+res_verf_deliv[2]);
                                            Program.WriteLine("Адрес: " + res_verf_deliv[2]);
                                            Program.WriteLine("                 ");
                                            for (int j = 0; j < CodeProd.Count; j++)
                                            {
                                                string qt = Convert.ToString(result.Tables[0].Rows[j + StartRowItems][i + StartColumsItems]);//количество товара
                                                if (qt != "" && qt != "0")
                                                {
                                                    object[] res_verf_item = Verifiacation.Verification_gtin_xls(CodeProd[j], JurSootvProd.ToString());
                                                    if (String.IsNullOrWhiteSpace(Convert.ToString(res_verf_item[0])))
                                                    {
                                                        DispOrders.WriteOrderLog("Excel", res_verf_buyer[0] + " - " + res_verf_buyer[1], res_verf_deliv[0] + " - " + res_verf_deliv[1], Path.GetFileName(parsfile), "  ", 1, "не найден штрих-код товара:" + CodeProd[j] + ". Проверте журнал соответсвий " + JurSootvProd.ToString() + ".", DateTime.Today, DateTime.Now, 0);
                                                        Program.WriteLine("Ошибка в товаре. Проверте журнал соответствий");
                                                    }
                                                    else
                                                    {
                                                        object[] PriceList = Verifiacation.GetPriceList(res_verf_deliv[0], Convert.ToInt32(res_verf_item[5]));
                                                        DispOrders.RecordToTmpZkg(Convert.ToString(res_verf_buyer[0]), Convert.ToString(res_verf_deliv[0]), Convert.ToString(date_delivery), Convert.ToString(res_verf_item[1]), Convert.ToString(res_verf_item[4]), qt, Convert.ToString(DateTime.Today), " ", Convert.ToString(PriceList[0]), Convert.ToInt16(res_verf_item[5]), Path.GetFileName(parsfile), Convert.ToString(PriceList[1]));
                                                    }
                                                }
                                            }

                                            ///!!!!перенос на постоянку!!!!!!!!!!
                                            DispOrders.TMPtoPrdZkg(res_verf_buyer, res_verf_deliv, Path.GetFileName(parsfile), "Excel", " ");
                                        }
                                        else
                                        {
                                            DispOrders.WriteOrderLog("Excel", res_verf_buyer[0] + " - " + res_verf_buyer[1], " ", Path.GetFileName(parsfile), " ", 2, "Не найден адрес доставки: " + CodeDeliv[i], DateTime.Today, DateTime.Now, 0);
                                            Program.WriteLine("Ошибка- в базе нет такой точки доставки!");
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
                                DispOrders.WriteOrderLog("Excel", res_verf_buyer[0] + " - " + res_verf_buyer[1], " ", Path.GetFileName(parsfile), " ", 5, "Неверная дата доставки: ", DateTime.Today, DateTime.Now, 0);
                                Program.WriteLine("Неверная дата доставки!");
                            }

                            //перемещение файла
                            string oldPath = Path.GetFullPath(parsfile);
                            string newPath = Archive_path + (DateTime.Now).ToString("ddMMyyyy_HHmmss") + "_" + Path.GetFileName(parsfile);
                            Directory.Move(oldPath, newPath.Replace(",", "_"));

                        }
                        catch (IOException e)
                        {
                            Program.WriteLine(Convert.ToString(e));
                            DispOrders.WriteErrorLog(Convert.ToString(e));
                            DispOrders.WriteErrorLog("Поврежденный файл заказа: " + parsfile);

                            //перемещение файла
                            string oldPath = Path.GetFullPath(parsfile);
                            string newPath = Archive_path + (DateTime.Now).ToString("ddMMyyyy_HHmmss") + "_" + Path.GetFileName(parsfile);
                            Directory.Move(oldPath, newPath.Replace(",", "_"));
                        }
                    }
                    catch (IOException e)
                    {
                        Program.WriteLine(Convert.ToString(e));
                        DispOrders.WriteErrorLog(Convert.ToString(e));
                    }
                }

                //int StartRowCode = Convert.ToInt32(DispOrders.GetValueOption("ТАНДЕР.СТРОКА ШТРИХКОДА"));
                //int StartColumnCode = Convert.ToInt32(DispOrders.GetValueOption("ТАНДЕР.КОЛОНКА ШТРИХКОДА"));
                //int StartRowDelivery = Convert.ToInt32(DispOrders.GetValueOption("ТАНДЕР.СТРОКА ДОСТАКИ"));
                //int StartColumnDelivery = Convert.ToInt32(DispOrders.GetValueOption("ТАНДЕР.КОЛОНКА ДОСТАВКИ"));
                //int StartRowItems = Convert.ToInt32(DispOrders.GetValueOption("ТАНДЕР.СТРОКА КОЛИЧЕСТВО"));
                //int StartColumsItems = Convert.ToInt32(DispOrders.GetValueOption("ТАНДЕР.КОЛОНКА КОЛИЧЕСТВО"));
            }
        }

        public static void IntakeOrders_TandExNew()
        {
            //найдем сколько всего алгоритмов в системе
            int qua = CountPtn();
            //получим список контрагентов и алгоритмы, по которым они должны разбираться
            int[,] Ptn = GetPtnRcdAlg(qua);
            for (int ji = 0; ji < qua; ji++)
            {
                //получение настроек из базы по конкретному ка
                //string cd_buyer = AnotherBuyers.GetPtnCdAlg(Ptn[ji, 1]);//плательщик
                string Orders_path = AnotherBuyers.GetSettingPath(Ptn[ji, 0], "PathOrd");//папку с заказами
                string Archive_path = AnotherBuyers.GetSettingPath(Ptn[ji, 0], "PathArc");//папка с архивами
                int JurSootvProd = AnotherBuyers.GetSettingPtn(Ptn[ji, 0], "Jurprod");
                int JurSootvKA = AnotherBuyers.GetSettingPtn(Ptn[ji, 0], "JurKA");
                int RowDD = AnotherBuyers.GetSettingPtn(Ptn[ji, 0], "Row_Dt");
                int ColumnDD = AnotherBuyers.GetSettingPtn(Ptn[ji, 0], "Col_Dt");
                int StartRowCode = AnotherBuyers.GetSettingPtn(Ptn[ji, 0], "Row_CodeProd");
                int StartColumnCode = AnotherBuyers.GetSettingPtn(Ptn[ji, 0], "Col_CodeProd");
                int StartRowDelivery = AnotherBuyers.GetSettingPtn(Ptn[ji, 0], "Row_CodeKa");
                int StartColumnDelivery = AnotherBuyers.GetSettingPtn(Ptn[ji, 0], "Col_CodeKa");
                int StartRowItems = AnotherBuyers.GetSettingPtn(Ptn[ji, 0], "Row_vol");
                int StartColumsItems = AnotherBuyers.GetSettingPtn(Ptn[ji, 0], "Col_vol");

                int CntProdExl = 0;//количество позиций заказа из эксель не всегда может быть правильным, т.к. ячейки могут быть пустыми
                int CntDelivExl = 0;//количество адресов доставки из эксель не всегда может быть правильным, т.к. ячейки могут быть пустыми
                int p = 0;
                int d = 0;
                bool error = false;
                string error_as_str = "";
                DateTime date_delivery;
                object buf_date_delivery;
                string[] files = Directory.GetFiles(Orders_path, "*.xls*");

                //начинаем парсить файл
                foreach (string parsfile in files)
                {
                    try
                    {
                        FileStream stream = File.Open(parsfile, FileMode.Open, FileAccess.Read);//try catch
                        IExcelDataReader excelReader;
                        if (parsfile.Substring(parsfile.LastIndexOfAny(new char[] { '.' })) == ".xls") //в зависимости от расширения файла, инициализация происходит разными способами
                        {
                            excelReader = ExcelReaderFactory.CreateBinaryReader(stream);
                        }
                        else
                        {
                            excelReader = ExcelReaderFactory.CreateOpenXmlReader(stream);
                        }

                        DataSet result = excelReader.AsDataSet(); //считали информацию из файла
                        excelReader.Close();  //закрыли
                        //проверить на существование колонок
                        CheckColumnsAndRows(result, RowDD, ColumnDD, StartRowCode, StartColumnCode, StartRowDelivery, StartColumnDelivery, StartRowItems, StartColumsItems, ref error_as_str, ref error);

                        if (error)
                        {
                           // string[] res_verf_buyer = Verifiacation.Verification_Tander_Buyer(cd_buyer);
                            DispOrders.WriteOrderLog("Excel", " - ", " ", Path.GetFileName(parsfile), " ", 5, "Неверный формат файла: " + Path.GetFileName(parsfile) + ". (" + error_as_str + "), файл заказа пропущен и перенесен в архив ", DateTime.Today, DateTime.Now, 0);
                            Program.WriteLine("Неверный формат файла(" + error_as_str + "). Файл перенесен в архив ");

                            //перемещение файла
                            string oldPath = Path.GetFullPath(parsfile);
                            string newPath = Archive_path + (DateTime.Now).ToString("ddMMyyyy_HHmmss") + "_" + Path.GetFileName(parsfile);
                            Directory.Move(oldPath, newPath.Replace(",", "_"));

                            continue;
                        }



                        buf_date_delivery = result.Tables[0].Rows[RowDD][ColumnDD]; //получим дату

                        //преобразование числа в дату
                        if (buf_date_delivery.GetType() == typeof(Double)) //если дата как число
                        {
                            double dat = double.Parse(Convert.ToString(result.Tables[0].Rows[RowDD][ColumnDD]));
                            date_delivery = Convert.ToDateTime(DateTime.FromOADate(dat));
                        }
                        else if (buf_date_delivery.GetType() == typeof(String)) //если дата как строка
                        {
                            Regex newReg = new Regex(@"\d{2}\.\d{2}\.\d{4}"); //дата в виде dd.mm.yyyy
                            MatchCollection matches = newReg.Matches(Convert.ToString(buf_date_delivery)); //некоторые заказы могут иметь в строке даты слова.
                            if (matches.Count == 1) //если найдено одно совпадение
                                date_delivery = Convert.ToDateTime(matches[0].Value);
                            else
                            {
                                DispOrders.WriteErrorLog("Не смог получить дату заказа в файле " + parsfile);
                                //перемещение файла
                                string oldPath = Path.GetFullPath(parsfile);
                                string newPath = Archive_path + (DateTime.Now).ToString("ddMMyyyy_HHmmss") + "_" + Path.GetFileName(parsfile);
                                Directory.Move(oldPath, newPath.Replace(",", "_"));

                                error = true; //есть ошибка
                                date_delivery = new DateTime(); //для порядка
                            }

                        }
                        else
                            date_delivery = Convert.ToDateTime(buf_date_delivery);

                        try
                        {

                            //string[] res_verf_buyer = Verifiacation.Verification_Tander_Buyer(cd_buyer);
                            if (!error || date_delivery > DateTime.Now) //дата заказа должна быть больше сегодняшнего дня (заказы принимаются будущей датой)
                            {

                                CntProdExl = result.Tables[0].Rows.Count - 1; //получим количество строк в файле, уменьшаем на единицу (отсчитываем от 0)
                                CntDelivExl = result.Tables[0].Columns.Count - 1; //получим количество столбцов в файле (отсчитываем от 0)

                                List<string> CodeProd = new List<string>(); //будет содержать список продуктов
                                List<string> CodeDeliv = new List<string>(); //будет содержать список грузополучателей

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

                                for (int i = StartColumnDelivery; i <= CntDelivExl; i++)//считывание кода адресов доставки в массив и их верификация
                                {
                                    string CurrCell = Convert.ToString(result.Tables[0].Rows[StartRowDelivery][i]);
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

                                for (int i = 0; i < CodeDeliv.Count; i++) //массив грузополучателей 
                                {
                                    if (CodeDeliv[i] != "") //если что-то есть в грузополучателях
                                    {

                                       // JurSootvProd = AnotherBuyers.GetSettingPtn(Ptn[ji, 0], "Jurprod");
                                       // JurSootvKA = AnotherBuyers.GetSettingPtn(Ptn[ji, 0], "JurKA");
                                        string[] res_verf_deliv = Verifiacation.Verification_Tander(CodeDeliv[i], JurSootvKA.ToString());//верификация адреса доставки
                                        if (res_verf_deliv[0] != null)
                                        {
                                            //получим плательщика 
                                            string[] res_verf_buyer = Verifiacation.Verification_Xls_Buyer(res_verf_deliv[3]);
                                            DispOrders.ClearTmpZkg();//очищаем временную таблицу с заказом от конкретной предыдущей точки 
                                            Program.WriteLine("Грузополучатель: " + res_verf_deliv[0] + "-" + res_verf_deliv[1]);// +"-"+res_verf_deliv[2]);
                                            Program.WriteLine("Адрес: " + res_verf_deliv[2]);
                                            Program.WriteLine("                 ");
                                            for (int j = 0; j < CodeProd.Count; j++)
                                            {
                                                string qt = Convert.ToString(result.Tables[0].Rows[j + StartRowItems][i + StartColumsItems]);//количество товара
                                                if (qt != "" && qt != "0")
                                                {
                                                    object[] res_verf_item = Verifiacation.Verification_gtin_xls(CodeProd[j], JurSootvProd.ToString());
                                                    if (String.IsNullOrWhiteSpace(Convert.ToString(res_verf_item[0])))
                                                    {
                                                        DispOrders.WriteOrderLog("Excel", res_verf_buyer[0] + " - " + res_verf_buyer[1], res_verf_deliv[0] + " - " + res_verf_deliv[1], Path.GetFileName(parsfile), "  ", 1, "не найден штрих-код товара:" + CodeProd[j] + ". Проверте журнал соответсвий " + JurSootvProd.ToString() + ".", DateTime.Today, DateTime.Now, 0);
                                                        Program.WriteLine("Ошибка в товаре. Проверте журнал соответствий");
                                                    }
                                                    else
                                                    {
                                                        object[] PriceList = Verifiacation.GetPriceList(res_verf_deliv[0], Convert.ToInt32(res_verf_item[5]));
                                                        DispOrders.RecordToTmpZkg(Convert.ToString(res_verf_buyer[0]), Convert.ToString(res_verf_deliv[0]), Convert.ToString(date_delivery), Convert.ToString(res_verf_item[1]), Convert.ToString(res_verf_item[4]), qt, Convert.ToString(DateTime.Today), " ", Convert.ToString(PriceList[0]), Convert.ToInt16(res_verf_item[5]), Path.GetFileName(parsfile), Convert.ToString(PriceList[1]));
                                                    }
                                                }
                                            }

                                            ///!!!!перенос на постоянку!!!!!!!!!!
                                            DispOrders.TMPtoPrdZkg(res_verf_buyer, res_verf_deliv, Path.GetFileName(parsfile), "Excel", " ");
                                        }
                                        else
                                        {
                                            DispOrders.WriteOrderLog("Excel", " - ", " ", Path.GetFileName(parsfile), " ", 2, "Не найден адрес доставки: " + CodeDeliv[i], DateTime.Today, DateTime.Now, 0);
                                            Program.WriteLine("Ошибка- в базе нет такой точки доставки!");
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
                                DispOrders.WriteOrderLog("Excel", " - ", " ", Path.GetFileName(parsfile), " ", 5, "Неверная дата доставки: ", DateTime.Today, DateTime.Now, 0);
                                Program.WriteLine("Неверная дата доставки!");
                            }

                            //перемещение файла
                            string oldPath = Path.GetFullPath(parsfile);
                            string newPath = Archive_path + (DateTime.Now).ToString("ddMMyyyy_HHmmss") + "_" + Path.GetFileName(parsfile);
                            Directory.Move(oldPath, newPath.Replace(",", "_"));

                        }
                        catch (IOException e)
                        {
                            Program.WriteLine(Convert.ToString(e));
                            DispOrders.WriteErrorLog(Convert.ToString(e));
                            DispOrders.WriteErrorLog("Поврежденный файл заказа: " + parsfile);

                            //перемещение файла
                            string oldPath = Path.GetFullPath(parsfile);
                            string newPath = Archive_path + (DateTime.Now).ToString("ddMMyyyy_HHmmss") + "_" + Path.GetFileName(parsfile);
                            Directory.Move(oldPath, newPath.Replace(",", "_"));
                        }
                    }
                    catch (IOException e)
                    {
                        Program.WriteLine(Convert.ToString(e));
                        DispOrders.WriteErrorLog(Convert.ToString(e));
                    }
                }

                //int StartRowCode = Convert.ToInt32(DispOrders.GetValueOption("ТАНДЕР.СТРОКА ШТРИХКОДА"));
                //int StartColumnCode = Convert.ToInt32(DispOrders.GetValueOption("ТАНДЕР.КОЛОНКА ШТРИХКОДА"));
                //int StartRowDelivery = Convert.ToInt32(DispOrders.GetValueOption("ТАНДЕР.СТРОКА ДОСТАКИ"));
                //int StartColumnDelivery = Convert.ToInt32(DispOrders.GetValueOption("ТАНДЕР.КОЛОНКА ДОСТАВКИ"));
                //int StartRowItems = Convert.ToInt32(DispOrders.GetValueOption("ТАНДЕР.СТРОКА КОЛИЧЕСТВО"));
                //int StartColumsItems = Convert.ToInt32(DispOrders.GetValueOption("ТАНДЕР.КОЛОНКА КОЛИЧЕСТВО"));
            }
        }


        public static int GetSettingPtn(int AlgRcd, string NameOpt)
        {
            int i = 0;
            int Value;
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "select " + NameOpt + " from U_CHPARAMALG where alg_rcd = " + AlgRcd + " ";
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
            Value = Convert.ToInt32(result[0]);
            return Value;
        }

        public static string GetSettingPath(int AlgRcd, string NamePath)
        {
            int i = 0;
            string Value;
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "select " + NamePath + " from U_CHALGORITHM where Rcd=" + AlgRcd + "";
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
            Value = Convert.ToString(result[0]);
            return Value;
        }

        public static int[,] GetPtnRcdAlg(int n)
        {
            int i = 0;
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select Rcd, Ptn_Rcd from U_CHALGORITHM";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(verification, conn);
            SqlDataReader dr = command.ExecuteReader();
            int[,] result = new int[n,2];
            while (dr.Read())
            {
                result[i, 0] = Convert.ToInt32(dr.GetValue(0));
                result[i, 1] = Convert.ToInt32(dr.GetValue(1));
                i++;
            }
            conn.Close();
            return result;
        }

        public static int CountPtn()//количество контрагентов для которых задан алгоритм
        {
            int i = 0, count;
            string connString = Settings.Default.ConnStringISPRO;
            string sql = "select count(*) from U_CHALGORITHM";
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

        public static string GetPtnCdAlg(int PtnRcd)
        {
            int i = 0;
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "select Ptn_Cd from Ptnrk where Ptn_Rcd = " + PtnRcd + " ";
            string Value;
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
            Value = Convert.ToString(result[0]);
            return Value;
        }

        public static string[] VerificationPtn(string PtnCd, string JurSootvKA)//верификация адреса доставки 
        {
            string connString = Settings.Default.ConnStringISPRO;
            string verification = "SELECT Ptn_Cd, Ptn_NmSh, Filia_Adr FROM PTNRK "
                                 + "LEFT JOIN PTNFILK on PTNFILK.Ptn_Rcd = PTNRK.Ptn_Rcd "
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
        //метод проверяет содержится ли в начальных ячейках то, что ожидает получить программа
        public static void CheckColumnsAndRows(DataSet res, int _RowDD, int _ColumnDD, int _StartRowCode, int _StartColumnCode, int _StartRowDelivery, int _StartColumnDelivery, int _StartRowItems, int _StartColumsItems,  ref string error_as_str, ref bool error)
        {
            if (res.Tables[0].Rows.Count <= _RowDD || res.Tables[0].Columns.Count <= _ColumnDD || DBNull.Value.Equals(res.Tables[0].Rows[_RowDD][_ColumnDD])) //в этом поле сидит дата
            {
                error_as_str = "Ячейка с датой пустая. ";
                error = true;
            }

            if (res.Tables[0].Rows.Count <= _StartRowCode || res.Tables[0].Columns.Count <= _StartColumnCode || DBNull.Value.Equals(res.Tables[0].Rows[_StartRowCode][_StartColumnCode])) //начальная ячейка с кодом продукции
            {
                error_as_str = error_as_str + "Ячейка с кодом продукции пустая. ";
                error = true;
            }
            if (res.Tables[0].Rows.Count <= _StartRowDelivery || res.Tables[0].Columns.Count <= _StartColumnDelivery || DBNull.Value.Equals(res.Tables[0].Rows[_StartRowDelivery][_StartColumnDelivery])) //начальная ячейка с кодом контрагента
            {
                error_as_str = error_as_str + " " + "Ячейка с кодом контрагента пустая. ";
                error = true;
            }
            if (res.Tables[0].Rows.Count <= _StartRowItems || res.Tables[0].Columns.Count <= _StartColumsItems) //начальная ячейка с количеством
            {
                error_as_str = error_as_str + " " + "Ячейка с количеством пустая. ";
                error = true;
            }
        }
    }
}
