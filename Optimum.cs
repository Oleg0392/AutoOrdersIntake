using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using System.IO;

namespace AutoOrdersIntake
{
    class Optimum
    {
        public static void IntakeOrders()
        {
            //int countMilk, countIcecream;
            //try
            //{
            //     countMilk = DispOrders.CountMilk();
            //     countIcecream = DispOrders.CountIceCream();
            //}
            //catch (IOException e)
            //{
            //    DispOrders.WriteErrorLog(Convert.ToString(e));
            //    DispOrders.WriteErrorLog("не могу подключиться к Optimum");
            
            //}

            int countMilk = DispOrders.CountMilk();
            int countIcecream = DispOrders.CountIceCream();

            object[,] milk = DispOrders.GetOrderMilkFromOptimum(countMilk);
            object[,] icecream = DispOrders.GetOrderIcecreamFromOptimum(countIcecream);

            if (milk.Length > 0)
            {
                for (int i = 0; i < countMilk; i++)//перенос молоко
                {
                    DispOrders.ClearTmpZkg();
                    DateTime Date = Convert.ToDateTime(milk[i, 8]);
                    string Id = Convert.ToString(milk[i, 9]);
                    string Masterfild = Convert.ToString(milk[i, 10]);
                    string DateFormat = Date.ToString("yyyyMMdd hh:mm:ss");
                    object[,] ListItems = DispOrders.GetitemsOptimum(DateFormat, Id, Masterfild);

                    string[] deliv = { Convert.ToString(milk[i, 3]), Convert.ToString(milk[i, 4]), Convert.ToString(milk[i, 5]) };
                    string[] buyer = Verifiacation.GetBuyerOptimum(deliv[0]);
                    
                    if (buyer[0] == null)
                        for (int k = 0; k < deliv.Length; k++)
                         {
                             buyer[k] = deliv[k];
                         }

                    //ListItems переносим в темпTRSD
                    for (int j = 0; j < ListItems.GetLength(0) ; j++)
                    {
                        object[] InfoItem = Verifiacation.GetDataOrderFromArt(Convert.ToString(ListItems[j, 0]));
                        object[] PL = Verifiacation.GetPriceList(Convert.ToString(milk[i, 3]), Convert.ToInt32(InfoItem[5]));
                        string quantity = (Convert.ToString(ListItems[j, 3])).Replace(",", ".");
                        string dd = Convert.ToString((Convert.ToDateTime(milk[i, 2])));
                        string dz = Convert.ToString(Convert.ToDateTime(milk[i, 1]));
                        DispOrders.RecordToTmpZkg(buyer[0], deliv[0], dd, Convert.ToString(ListItems[j, 0]), Convert.ToString(ListItems[j, 2]), quantity, dz, Convert.ToString(milk[i, 0])+" "+Convert.ToString(milk[i,6]), Convert.ToString(PL[0]), Convert.ToInt32(InfoItem[5]), "Optimum", Convert.ToString(PL[1]));
                    }

                    DispOrders.TMPtoPrdZkg(buyer, deliv, "Optimum", "Optimum",Convert.ToString(milk[i,0]));
                    Optimum.UpdateStateOrder(Convert.ToString(milk[i, 0]), Date.ToString("yyyyMMdd"));

                    //конец переноса заказа от одной точки по молоку
                    Program.WriteLine("Заказ из Optimum выгружен. Номер заказа: " + Convert.ToString(milk[i, 0]));
                    //Console.ReadLine();
                }
            }

            if (icecream.Length > 0)
            {
                for (int i = 0; i < countIcecream; i++)//перенос мороженное
                {
                    DispOrders.ClearTmpZkg();
                    DateTime Date = Convert.ToDateTime(icecream[i, 8]);
                    string Id = Convert.ToString(icecream[i, 9]);
                    string Masterfild = Convert.ToString(icecream[i, 10]);
                    string DateFormat = Date.ToString("yyyyMMdd hh:mm:ss");
                    //DateTime Masterfild = Convert.ToDateTime(milk[i, 10]);
                    object[,] ListItems = DispOrders.GetitemsOptimum(DateFormat, Id, Masterfild);
                    string[] deliv = { Convert.ToString(icecream[i, 3]), Convert.ToString(icecream[i, 4]), Convert.ToString(icecream[i, 5]) };
                    string[] buyer = Verifiacation.GetBuyerOptimum(deliv[0]);

                    if (buyer[0] == null)
                        for (int k = 0; k < deliv.Length; k++)
                        {
                            buyer[k] = deliv[k];
                        }

                    //ListItems переносим в темпTRSD
                    for (int j = 0; j < ListItems.GetLength(0); j++)
                    {
                        object[] InfoItem = Verifiacation.GetDataOrderFromArt(Convert.ToString(ListItems[j, 0]));
                        object[] PL = Verifiacation.GetPriceList(Convert.ToString(icecream[i, 3]), Convert.ToInt32(InfoItem[5]));
                        string quantity = (Convert.ToString(ListItems[j, 3])).Replace(",", ".");
                        string dd = (Convert.ToDateTime(icecream[i, 2])).ToString("yyyyMMdd");
                        string dz = (Convert.ToDateTime(icecream[i, 1])).ToString("yyyyMMdd");
                        DispOrders.RecordToTmpZkg(buyer[0], deliv[0], Convert.ToString(icecream[i, 2]), Convert.ToString(ListItems[j, 0]), Convert.ToString(ListItems[j, 2]), quantity, Convert.ToString(icecream[i, 1]), Convert.ToString(icecream[i, 0]) + " " + Convert.ToString(icecream[i, 6]), Convert.ToString(PL[0]), Convert.ToInt32(InfoItem[5]), "Optimum", Convert.ToString(PL[1]));
                    
                    }

             
                    DispOrders.TMPtoPrdZkg(buyer, deliv, "Optimum", "Optimum",Convert.ToString(icecream[i,0]));
                    Optimum.UpdateStateOrder(Convert.ToString(icecream[i, 0]), Date.ToString("yyyyMMdd"));

                    Program.WriteLine("Заказ из Optimum выгружен. Номер заказа: " + Convert.ToString(icecream[i, 0]));
                    
                }
            }
        }

        public static void UpdateStateOrder(string Id, string DateOrder)
        {
            string connString = Settings.Default.ConnStringOptimum;
            //string newId = Id.Replace()
            string Update = "UPDATE ds_orders "
           + " SET fState = 1 "
           + " WHERE "
           + "       CONVERT(varchar, orNumber) + CONVERT(varchar, MasterFID) + CONVERT(varchar, orID) = '" + Id.Replace("KPK","") +"' "
              + "   AND OrType = 0 "
              + "   AND "
              + "       orDate BETWEEN '" + DateOrder + "' "
              + "   AND DATEADD(DD, 1, '" + DateOrder + "') ";
            SqlConnection conn = new SqlConnection();
            conn.ConnectionString = connString;
            conn.Open();
            SqlCommand command = new SqlCommand(Update, conn);
            SqlDataReader dr = command.ExecuteReader();
            conn.Close();
        }
    }
}
