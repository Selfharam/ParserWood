using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Net.Http;
using Microsoft.Data.SqlClient;
using System.Globalization;
using System.Diagnostics;
using System.Threading;

namespace ParserWood
{

    class DbConnect
    {
        public SqlConnection connection;

        public DbConnect()
        {
            string connectionString = "Server=DESKTOP-24S5SPT\\MSSQLSERVER1;Database=WoodDB;User Id=test;Password=test;Encrypt=False;Trusted_Connection=False";
            connection = new SqlConnection(connectionString);
        }

        public void InsertToSql(string insertString)
        {
            SqlCommand command = new SqlCommand();

            command.CommandText = insertString;
            command.Connection = connection;
            command.Connection.Open();
            command.ExecuteNonQuery();
            command.Connection.Close();
        }

        public bool IsDeal(string dealNumber)//наличие сделки
        {
            SqlCommand command = new SqlCommand();
            command.CommandText = "SELECT * FROM DealWood WHERE dealNumber='" + dealNumber + "'"; 
            command.Connection = connection;
            command.Connection.Open();
            command.ExecuteNonQuery();

            SqlDataReader reader = command.ExecuteReader();
            if (reader.HasRows)
            {
                command.Connection.Close();
                return false;
            }
            command.Connection.Close();
            return true;
        }

        public int DealId()//кол-во строк в таблице + 1
        {
            SqlCommand command = new SqlCommand();

            command.CommandText = "SELECT * FROM DealWood";
            command.Connection = connection;
            command.Connection.Open();
            command.ExecuteNonQuery();

            SqlDataReader reader = command.ExecuteReader();
            int count = 1;
            while (reader.Read()) count++;


            command.Connection.Close();
            return count;


        }

    }

    class Variable
    {
        public int size { get; set; }
        public int number { get; set; }
        public String filters { get; set; }
        public String orders { get; set; }

    }
    class QueryRequest
    {
        public string query { get; set; }
        public Variable variables { get; set; }
        public string operationName { get; set; }
        
    }
    class Deal
    {
        public string sellerName { get; set; }

        public string sellerInn { get; set; }

        public string buyerName { get; set; }

        public string buyerInn { get; set; }

        public float woodVolumeBuyer { get; set; }

        public float woodVolumeSeller { get; set; }

        public string dealDate { get; set; }

        public string dealNumber { get; set; }

        public string __typename { get; set; }

    }
    
    
    internal class Program
    {

        
        private static readonly HttpClient client = new HttpClient();

        public static int curentNumber = 0;
        public static async void  Updater(object obj)
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
            bool flag = true;
            DbConnect dbConnector = new DbConnect();
            while (flag)
            {
                string insertString = "INSERT INTO DealWood(Id, sellerName, sellerInn, buyerName, buyerInn, woodVolumeBuyer, woodVolumeSeller, dealDate, dealNumber) ";
                string insertStringValuesConst = "VALUES ";
                string insertStringValues = " ";
                QueryRequest ourRequest = new QueryRequest
                {
                    query = "query SearchReportWoodDeal($size: Int!, $number: Int!, $filter: Filter, $orders: [Order!]) {\n  searchReportWoodDeal(filter: $filter, pageable: { number: $number, size: $size}, orders: $orders) {\n content {\n sellerName\n sellerInn\n buyerName\n buyerInn\n woodVolumeBuyer\n woodVolumeSeller\n dealDate\n dealNumber\n __typename\n }\n __typename\n}\n}\n ",
                    variables = new Variable { size = 500, number = curentNumber, filters = null, orders = null },
                    operationName = "SearchReportWoodDeal"
                };

                string json = JsonSerializer.Serialize(ourRequest);

                var stringContent = new StringContent(json);


                var response = await client.PostAsync("https://www.lesegais.ru/open-area/graphql", stringContent);

                var responseString = await response.Content.ReadAsStringAsync();
                CancellationTokenSource cts = new CancellationTokenSource();
                
                JsonNode jsonDocument = JsonNode.Parse(responseString);
                Thread.Sleep(3000);
                cts.Cancel();

               
                
                int IndexCount = jsonDocument["data"]["searchReportWoodDeal"]["content"].AsArray().Count;

                if (IndexCount != 0)
                {
                    int IdIndex = 0;
                    for (int index = 0; index < IndexCount; index++)
                    {
                       

                        Deal request = new Deal
                        {
                            sellerName = (string)jsonDocument["data"]["searchReportWoodDeal"]["content"][index]["sellerName"] == null ? "" : (string)jsonDocument["data"]["searchReportWoodDeal"]["content"][index]["sellerName"],
                            sellerInn = (string)jsonDocument["data"]["searchReportWoodDeal"]["content"][index]["sellerInn"] == null ? "" : (string)jsonDocument["data"]["searchReportWoodDeal"]["content"][index]["sellerInn"],
                            buyerName = (string)jsonDocument["data"]["searchReportWoodDeal"]["content"][index]["buyerName"] == null ? "" : (string)jsonDocument["data"]["searchReportWoodDeal"]["content"][index]["buyerName"],
                            buyerInn = (string)jsonDocument["data"]["searchReportWoodDeal"]["content"][index]["buyerInn"] == null ? "" : (string)jsonDocument["data"]["searchReportWoodDeal"]["content"][index]["buyerInn"],
                            woodVolumeBuyer = (float)jsonDocument["data"]["searchReportWoodDeal"]["content"][index]["woodVolumeBuyer"],
                            woodVolumeSeller = (float)jsonDocument["data"]["searchReportWoodDeal"]["content"][index]["woodVolumeSeller"],
                            dealDate = (string)jsonDocument["data"]["searchReportWoodDeal"]["content"][index]["dealDate"] == null ? "" : (string)jsonDocument["data"]["searchReportWoodDeal"]["content"][index]["dealDate"],
                            dealNumber = (string)jsonDocument["data"]["searchReportWoodDeal"]["content"][index]["dealNumber"] == null ? "" :(string)jsonDocument["data"]["searchReportWoodDeal"]["content"][index]["dealNumber"],
                            __typename = (string)jsonDocument["data"]["searchReportWoodDeal"]["content"][index]["__typename"]
                        };
                        
                        request.sellerName = Regex.Replace(request.sellerName, @"[^\w\.@-]", " ");
                        request.sellerInn = Regex.Replace(request.sellerInn, @"[^\w\.@-]", " ");
                        request.buyerName = Regex.Replace(request.buyerName, @"[^\w\.@-]", " ");
                        request.buyerInn = Regex.Replace(request.buyerInn, @"[^\w\.@-]", " ");
                        request.dealDate = Regex.Replace(request.dealDate, @"[^\w\.@-]", " ");
                        request.dealNumber = Regex.Replace(request.dealNumber, @"[^\w\.@-]", " ");


                        if (dbConnector.IsDeal(request.dealNumber))
                        {
                             insertStringValues = insertStringValues  + "(" + (curentNumber * 500 + IdIndex) + ", '" + request.sellerName.Trim('\'') + "', '" + request.sellerInn + "', '" + request.buyerName.Trim('\'') +
                                    "', '" + request.buyerInn + "', " + request.woodVolumeBuyer + ", " + request.woodVolumeSeller + ", '" + request.dealDate +
                                    "', '" + request.dealNumber + "'),";
                                    IdIndex++;
                           
                        }
                        
                    }
                    if(insertStringValues != " ") 
                    {
                        
                        insertStringValues.LastIndexOf(',');
                        dbConnector.InsertToSql(insertString  + "VALUES " + insertStringValues.Remove(insertStringValues.LastIndexOf(',')));
                        Console.WriteLine("Выборка получена");
                    }
                    insertStringValues = " ";
                    
                }
                else
                {
                    curentNumber = 0;
                    flag = false;
                }

                curentNumber++;
            }
            
        }
        static async Task Main(string[] args)
        {

            int num = 0;
            TimerCallback tm = new TimerCallback(Updater);
            Timer timer = new Timer(tm, num, 0, 600000);
            Console.ReadLine();
            
        }
    }
}
