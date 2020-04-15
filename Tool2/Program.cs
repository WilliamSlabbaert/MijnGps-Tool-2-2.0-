using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Tool2
{
    class Program
    {
        static void Main(string[] args)
        {
            Task straten = Task.Factory.StartNew(() => ReadStraten(ReadFile(@"C:\Users\willi\Desktop\DataRapportStraten.csv")));
            
            Task Gemeenten = Task.Factory.StartNew(() => ReadGemeenten(ReadFile(@"C:\Users\willi\Desktop\DataRapportGemeente.csv")));

            Task Provincies = Task.Factory.StartNew(() => ReadProvincies(ReadFile(@"C:\Users\willi\Desktop\DataRapportProvincie.csv")));

            Task.WaitAll(Provincies, Gemeenten, straten);
            Task Finish = Task.Factory.StartNew(() => PrintFinish());
            Finish.Wait();



        }
        public static string[] ReadFile(String file)
        {
            return File.ReadAllLines(file);
        }
        public static void ReadStraten(string[] lines)
        {
            Console.WriteLine("Loading Data Streets.....");
            SqlConnection connect = new SqlConnection(@"Data Source=WILLIAM-SLABBAE\SQLEXPRESS;Initial Catalog=AdressenOpdracht;Integrated Security=True");
            connect.Open();
            lines = lines.Skip(1).ToArray();
            foreach (string line in lines)
            {
                DecomposeStraten(line,connect);
            }
            Console.ForegroundColor = ConsoleColor.Green;
            Console.Write("(Done)");
            Console.ResetColor();
            Console.WriteLine();
            connect.Close();
        }
        public static void DecomposeStraten(string line, SqlConnection connect)
        {
            String[] data = line.Split(";");
            //SQL command
            SqlCommand command = new SqlCommand("INSERT INTO [dbo].[Streets] (ID,STREETNAME,LENGHT,SEGMENT) VALUES (@id,@name,@len,@seg)", connect);
            command.Parameters.AddWithValue("@id", data[0]);
            command.Parameters.AddWithValue("@name", data[1]);
            command.Parameters.AddWithValue("@len", data[2]);
            command.Parameters.AddWithValue("@seg", data[3]);

            command.ExecuteNonQuery();
        }
        public static void ReadGemeenten(string[] lines)
        {
            Console.WriteLine("Loading Data Gemeente.....");
            SqlConnection connect = new SqlConnection(@"Data Source=WILLIAM-SLABBAE\SQLEXPRESS;Initial Catalog=AdressenOpdracht;Integrated Security=True");
            connect.Open();
            foreach (string line in lines)
            {
                DecomposeGemeenten(line, connect);
            }
            Console.ForegroundColor = ConsoleColor.Green;
            Console.Write("(Done)");
            Console.ResetColor();
            Console.WriteLine();
            connect.Close();
        }
        private static void DecomposeGemeenten(string line, SqlConnection connect)
        {
            String[] data = line.Split(";");
            //SQL command
            SqlCommand command = new SqlCommand("INSERT INTO [dbo].[Gemeente] (ID,GEMEENTENAME,STREETID,MAX,MIN) VALUES (@id,@name,@street,@max,@min)", connect);
            command.Parameters.AddWithValue("@id", data[0]);
            command.Parameters.AddWithValue("@name", data[1]);
            if(data[2]==null)
            {
                command.Parameters.AddWithValue("@street", null);
            }
            else
            command.Parameters.AddWithValue("@street", data[2]);
            if (data[3] == null)
                command.Parameters.AddWithValue("@max", null);
            else
                command.Parameters.AddWithValue("@max", data[3]);
            if (data[4] == null)
                command.Parameters.AddWithValue("@min", null);
            else
                command.Parameters.AddWithValue("@min", data[4]);
            try
            {
                command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }

        }
        public static void ReadProvincies(string[] lines)
        {
            Console.WriteLine("Loading Data Provincies.....");
            SqlConnection connect = new SqlConnection(@"Data Source=WILLIAM-SLABBAE\SQLEXPRESS;Initial Catalog=AdressenOpdracht;Integrated Security=True");
            connect.Open();
            foreach (string line in lines)
            {
                DecomposeProvincies(line,connect);
            }
            Console.ForegroundColor = ConsoleColor.Green;
            Console.Write("(Done)");
            Console.ResetColor();
            Console.WriteLine();
            connect.Close();
        }
        private static void DecomposeProvincies(string line, SqlConnection connect)
        {
            String[] data = line.Split(";");
            //SQL command
            SqlCommand command = new SqlCommand("INSERT INTO [dbo].[Provincie] (ID,PROVINCIENAME,GEMEENTEID) VALUES (@id,@name,@gemeente)", connect);
            command.Parameters.AddWithValue("@id", data[0]);
            command.Parameters.AddWithValue("@name", data[1]);
            command.Parameters.AddWithValue("@gemeente", data[2]);
            try
            {
                command.ExecuteNonQuery();
            }
            catch(Exception e)
            {
                Console.WriteLine(e.ToString());
            }

        }
        public static void PrintFinish()
        {
            Console.Clear();
            Console.WriteLine("Done!!!");
        }
        
    }
}
