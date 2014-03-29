using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;

namespace RavenConf.Messaging.Worker
{
    class Program
    {
        private static int pow;
        private static IDocumentStore _store;

        private static void Main(string[] args)
        {
            var arg = args.Length > 0 ? args[0] : "square";
            Bootstap(arg);

            using (_store = new DocumentStore {Url = "http://localhost:8080"}.Initialize())
            {
                var subscription =
                    _store
                        .Changes()
                        .ForDocumentsInCollection("Routing")
                        .Subscribe(Handler);
                
                Console.ReadLine();
                subscription.Dispose();
            }
        }

        static void Bootstap(string arg)
        {
            pow = 2;
            if (arg == "cube") pow = 3;
            if (arg == "quad") pow = 4;

            Console.WriteLine("Running in {0} mode.", arg);
        }

        static void Handler(DocumentChangeNotification change)
        {
            if (change.Type != DocumentChangeTypes.Put) return;

            using (var session = _store.OpenSession())
            {
                var slip = session
                                .Include<RoutingSlip>(x => x.TaskId)
                                .Load<RoutingSlip>(change.Id);
                
                var task = session.Load<Task>(slip.TaskId);

                var result = Math.Pow(task.NumberToSquare, pow);
                Console.WriteLine("Turned {0} into {1}", task.NumberToSquare, result);
            }
        }
    }

    class Task
    {
        public string Id { get; set; }
        public int NumberToSquare { get; set; }
    }
    class RoutingSlip
    {
        public string Id { get; set; }
        public string TaskId { get; set; }
        public string[] Steps { get; set; }
        public Dictionary<string, int> Results { get; set; }

        public RoutingSlip()
        {
            Results = new Dictionary<string, int>();
        }
    }
}
