using System;
using System.Collections.Generic;
using DatabaseManager;

namespace Ordering
{
    static class OrderSerializer
    {
        private static CSVDatabase orderDatabase;
        private static List<StockItem> stockItems;
        private static List<Record> stockItemRecords;

        static OrderSerializer()
        {
            orderDatabase = new CSVDatabase("OrderDatabase");
            orderDatabase.CreateTable("Customers", new CSVFields("Name:text,Address:text,Email:text,Phone:text"));
            orderDatabase.CreateTable("Items", new CSVFields("Name:text,CostPence:int,Stock:int"));
            orderDatabase.CreateTable("Orders", new CSVFields("CustomerID:number,CardID:number"));
            orderDatabase.CreateTable("CreditCards", new CSVFields("Name:text,MainNumber:text,ExpiryDate:text,SecurityCode:text"));
            orderDatabase.CreateTable("PurchasedItems", new CSVFields("OrderID:int,ItemID:int,Quantity:int"));
            stockItemRecords = new List<Record>(orderDatabase.GetTable("Items").GetRecords());
            stockItems = new List<StockItem>();
            foreach (Record itemRecord in stockItemRecords)
            {
                Item item;
                if (Enum.TryParse((string)itemRecord.GetValue("Name"), out item))
                {
                    int cost = (int)itemRecord.GetValue("CostPence");
                    int remainingStock = (int)itemRecord.GetValue("Stock");
                    StockItem stockItem = new StockItem(item, cost, remainingStock);
                    stockItems.Add(stockItem);
                }
            }
        }

        public static void PlaceOrder(Order order)
        {
            Record customerRecord;
            Record[] customerRecords = orderDatabase.GetRecords("Customers", "Email", order.customer.email);
            if (customerRecords.Length == 0)
                customerRecord = orderDatabase.AddRecord("Customers", new object[] { order.customer.name, order.customer.address, order.customer.email, order.customer.phone });
            else customerRecord = customerRecords[0];

            Record cardRecord;
            Record[] cardRecords = orderDatabase.GetRecords("CreditCards", "MainNumber", order.card.mainNumber);
            if (cardRecords.Length == 0)
                cardRecord = orderDatabase.AddRecord("CreditCards", new object[] { order.card.name, order.card.mainNumber, order.card.expiryDate, order.card.securityCode });
            else cardRecord = cardRecords[0];

            Console.WriteLine(customerRecord);
            Console.WriteLine(cardRecord);

            if (cardRecord != null && customerRecord != null)
            {
                Record orderRecord = orderDatabase.AddRecord("Orders", new object[] { customerRecord.ID, cardRecord.ID });
                Console.WriteLine(orderRecord);

                foreach (OrderedItem orderedItem in order.orderedItems)
                {
                    int itemRecordID = GetItemRecordID(orderedItem.item);
                    orderDatabase.UpdateRecord("Items", itemRecordID, new object[] { null, null, stockItems[(int)orderedItem.item].remainingStock - orderedItem.quantity });
                    orderDatabase.AddRecord("PurchasedItems", new object[] { orderRecord.ID, itemRecordID, orderedItem.quantity });
                }

                orderDatabase.SaveChanges();
                Console.WriteLine("Order Placed");
            }
            else Console.WriteLine("Order Failed");
        }

        public static void UpdateOrder()
        {
            orderDatabase.SaveChanges();
        }

        private static int GetItemRecordID(Item item)
        {
            return stockItemRecords[(int)item].ID;
        }

        public static void OutputOrders()
        {

        }
    }

    class Order
    {
        public Customer customer;
        public OrderedItem[] orderedItems;
        public CreditCard card;

        public Order() { }
        public Order(Customer customer, OrderedItem[] orderedItems, CreditCard card)
        {
            this.customer = customer;
            this.orderedItems = orderedItems;
            this.card = card;
        }

        public override string ToString()
        {
            return string.Format("Order({0}, {1})", customer, orderedItems.Length);
        }
    }

    class Customer
    {
        public string name;
        public string address;
        public string phone;
        public string email;

        public Customer() { }
        public Customer(string name, string address, string phone, string email)
        {
            this.name = name;
            this.address = address;
            this.phone = phone;
            this.email = email;
        }

        public override string ToString()
        {
            return string.Format("Customer('{0}')", name);
        }
    }

    class OrderedItem
    {
        public Item item;
        public int quantity = 1;

        public OrderedItem() { }
        public OrderedItem(Item item, int quantity)
        {
            this.item = item;
            this.quantity = quantity;
        }

        public override string ToString()
        {
            return string.Format("OrderedItem({0}, {1})", item, quantity);
        }
    }

    class StockItem
    {
        public Item item;
        public int cost = 0;
        public int remainingStock = 0;

        public StockItem() { }
        public StockItem(Item item, int cost, int remainingStock)
        {
            this.item = item;
            this.cost = cost;
            this.remainingStock = remainingStock;
        }

        public override string ToString()
        {
            return string.Format("StockItem({0}, £{1:0.00})", item, Math.Round((float)(cost / 100f), 2));
        }
    }

    enum Item
    {
        Book,
        Pen,
        Paper
    }

    class CreditCard
    {
        public string name;
        public string mainNumber;
        public string expiryDate;
        public string securityCode;

        public CreditCard() { }
        public CreditCard(string name, string mainNumber, string expiryDate, string securityCode)
        {
            this.name = name;
            this.mainNumber = mainNumber;
            this.expiryDate = expiryDate;
            this.securityCode = securityCode;
        }

        public override string ToString()
        {
            return string.Format("CreditCard({0})", name);
        }
    }
}
