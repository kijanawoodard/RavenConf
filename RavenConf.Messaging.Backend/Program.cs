using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Extensions;

namespace RavenConf.Messaging.Backend
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var store = new DocumentStore { Url = "http://localhost:8080" }.Initialize())
            {
                EnsureDatabaseExists(store, "Messages");

                Console.Clear();
                Console.WriteLine("Hello");
                Console.WriteLine();

                while (true)
                {
                    Console.Write("A NumberToSquare Please: ");
                    var input = Console.ReadLine() ?? string.Empty;
                    if (input.ToLower() == "q") break;
                    
                    int number;
                    var ok = int.TryParse(input, out number);

                    if (!ok)
                    {
                        Console.WriteLine("Hmmmm. That's not a number.");
                        continue;
                    }

                    using (var session = store.OpenSession())
                    {
                        var task = new NumberTask() { NumberToSquare = number };
                        session.Store(task);

                        var manifest = new RoutingSlip {TaskId = task.Id, Handlers = new[] {"square", "cube", "display"}};
                        session.Store(manifest);
                        
                        session.SaveChanges();
                        Console.WriteLine("NumberToSquare Task Accepted for {0}", task.NumberToSquare);
                    }

                    Console.WriteLine();
                }
            }

            Console.WriteLine();
            Console.WriteLine("Goodbye");
        }

        private static void EnsureDatabaseExists(IDocumentStore store, string defaultDatabase)
        {
            store.DatabaseCommands.CreateDatabase(new DatabaseDocument
            {
                Id = defaultDatabase,
                Settings =
                {
                    {"Raven/ActiveBundles", "ScriptedIndexResults"},
                    {"Raven/DataDir", "~\\Databases\\Messages"}
                }
            });

            ((DocumentStore) store).DefaultDatabase = defaultDatabase;
        }

        class NumberTask
        {
            public string Id { get; set; }
            public int NumberToSquare { get; set; }
            public int NumberToCube { get; set; }
        }

        class RoutingSlip
        {
            public static string FormatId(string taskId) {  return string.Format("{0}/manifest", taskId); }
            public string Id { get { return FormatId(TaskId); } }
            public string TaskId { get; set; }
            public string[] Handlers { get; set; }
            public Dictionary<string, int> Answers { get; set; }

            public RoutingSlip()
            {
                Answers = new Dictionary<string, int>();
            }
        }
    }
}
