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

namespace AutoOrdersIntake
{
    class Program
    {


        static void Main(string[] args) //string[] args
        {
            Program.WriteLine("Начало----------------------------------------------------------");
            if (args.Length != 0)
                {
                    switch (args[0])
                    {
                      case "-excel":
                            Program.WriteLine("Загрузка заказов Excel");
                            AnotherBuyers.IntakeOrders_TandExNew();
                            break;
                        case "-dbf":
                            Program.WriteLine("Загрузка заказов DBF");
                            if(DateTime.Now.Date > new DateTime(2017, 12, 26)) DBF.IntakeDBFNew();
                            else DBF.IntakeDBF();  //разбор dbf файла
                            break;
                        case "-optimum":
                            Program.WriteLine("Загрузка заказов OPTIMUM");
                            Optimum.IntakeOrders();  //разбор напрямую из базы данных оптимум
                            break;
                        case "-orders":
                            Program.WriteLine("Загрузка ORDERS");
                            //перенос файлов для импорта из головного предприятия
                            SKBKontur.TransferOrders();
                            EDISOFT.TransferOrders();
                            //*****************
                            SKBKontur.IntakeOrders();
                            EDISOFT.IntakeOrders();
                            break;
                        case "-desadv":
                            Program.WriteLine("Выгрузка DESADV");
                            int hourDESADV = Convert.ToInt32(DispOrders.GetValueOption("ОБЩИЕ.ЧАС ОТПРАВКИ УОП(DESADV)"));
                            if (DateTime.Now.Hour > hourDESADV) SentDoc.SentDesadv();
                            break;
                        case "-recadv":
                            Program.WriteLine("Загрузка RECADV");
                            EDISOFT.IntakeRecAdv();
                            SKBKontur.IntakeRecAdv();
                        break;
                        case "-invoice":
                            Program.WriteLine("Выгрузка INVOICE");
                            SentDoc.SentInvoice();
                            break;
                        case "-schfdoppr":
                            Program.WriteLine("Выгрузка SCHFDOPPR");
                             SentDoc.SentUPD();
                            break;
                       case "-doppr":
                            Program.WriteLine("Выгрузка SCHFPR");
                            SentDoc.SentUPD("ДОП");
                            break;
                       case "-diadoc":
                            Program.WriteLine("Выгрузка SCHFDOPPR в Diadoc");
                            SentDoc.SentUPD_Diadoc();
                            Program.WriteLine("Выгрузка УПД (в формате xml-файлов), сформированных в ИСПРО в Diadoc");
                            SKBKontur.SendFiles_toDiadoc();
                        break;

                       default:
                            Program.WriteLine("Неизвестный параметр " + args[0]);
                            break;
                    }
                }
                else Program.WriteLine("Не заданы параметры запуска");
                
            Program.WriteLine("Конец  ----------------------------------------------------------");
            Console.WriteLine("Готово");
            Environment.Exit(0);
        }

        public static void WriteLine(string messageToConsole)
        {
            Console.WriteLine(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " " + messageToConsole);
        }
    }
}
