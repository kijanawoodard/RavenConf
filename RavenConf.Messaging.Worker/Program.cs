using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Timers;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Timer = System.Timers.Timer;

namespace RavenConf.Messaging.Worker
{
    class Program
    {
        private static IDocumentStore _store;
        private static Action<DocumentChangeNotification> _work = change => Console.WriteLine("No work done");
        private static Timer _timer;
        public static int WorkerSleep = 1000;

        private static void Main(string[] args)
        {
            using (_store = new DocumentStore {Url = "http://localhost:8080", DefaultDatabase="Messages"}.Initialize())
            {
                Boostrap(args);

                _store
                    .Changes()
                    .ConnectionStatusChanged += 
                        (sender, a) => 
                            Console.WriteLine("Raven Connection Status Changed to {0}", _store.Changes().Connected); ;

                var sleepingSubscription =
                    _store
                        .Changes()
                        .ForDocument("admin/config")
                        .Subscribe(AdjustSleep);

                var routingSubcription =
                    _store
                        .Changes()
                        .ForDocumentsStartingWith("routing/")
                        .Subscribe(
                            _work,
                            exception => Console.WriteLine("Changes Subscription Error! {0}", exception),
                            () => Console.WriteLine("Changes Subscription Completed Unexpectedly!")
                        );

                while (true)
                {
                    var input = Console.ReadLine();
                    if (string.IsNullOrWhiteSpace(input)) break;

                    var sleep = WorkerSleep > 0 ? 0 : 1000;
                    SetSleep(sleep);
                }
                routingSubcription.Dispose();
                sleepingSubscription.Dispose();
            }
        }

        private static void Boostrap(string[] args)
        {
            Console.Clear();
            Console.WriteLine("Hello");

            var arg = args.Length > 0 ? args[0] : SquareService.Worker.Step;

            switch (arg)
            {
                case SquareService.Worker.Step:
                    _work = new SquareService.Worker(_store).Work;
                    break;
                case CubeService.Worker.Step:
                    _work = new CubeService.Worker(_store).Work;
                    break;
                case QuadService.Worker.Step:
                    _work = new QuadService.Worker(_store).Work;
                    break;
                case "shaker":
                    _work = notification => Console.WriteLine("I see changes for {0}", notification.Id);
                    _timer = new Timer(5000);
                    _timer.AutoReset = false;
                    _timer.Elapsed += TimerOnElapsed;
                    _timer.Start();
                    break;
                default:
                    Console.WriteLine("{0} is not a known service", arg);
                    arg = SquareService.Worker.Step;
                    _work = new SquareService.Worker(_store).Work;
                    break;
            }
            
            Console.WriteLine("Running in {0} mode.", arg);          
        }

        private static void TimerOnElapsed(object sender, ElapsedEventArgs elapsedEventArgs)
        {
            try
            {
                ShakeTasks();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error while shaking tasks: {0}", ex);
            }

            _timer.Start();
        }

        private static void ShakeTasks()
        {
            using (var session = _store.OpenSession())
            using (var enumerator = session.Advanced.Stream<RoutingSlip>(
                                                                startsWith: "routing/",
                                                      start: 0, pageSize: int.MaxValue))
            {
                while (enumerator.MoveNext())
                {
                    var slip = enumerator.Current.Document;
                    if (slip.IsInGoodShape()) continue;

                    slip.Shake();
                    session.Store(slip);
                    session.SaveChanges();
                }
            }
        }

        private static void SetSleep(int sleep)
        {
            using (var session = _store.OpenSession())
            {
                var config = session.Load<Configuration>(Configuration.StableId);
                if (config == null)
                {
                    config = new Configuration();
                    session.Store(config);
                }

                WorkerSleep = sleep;
                config.WorkerSleepInMilliseconds = sleep;
                session.SaveChanges();
                Console.WriteLine("Set worker sleep to {0}", sleep);
            }
        }

        private static void AdjustSleep(DocumentChangeNotification change)
        {
            if (change.Type != DocumentChangeTypes.Put) return;

            using (var session = _store.OpenSession())
            {
                var config = session.Load<Configuration>(Configuration.StableId);
                WorkerSleep = config.WorkerSleepInMilliseconds;
            }
            Console.WriteLine("Will now sleep to {0}", WorkerSleep);
        }
    }

    namespace SquareService
    {
        class Worker
        {
            public const string Step = "square";
            private readonly IDocumentStore _store;

            public Worker(IDocumentStore store)
            {
                _store = store;
            }

            public void Work(DocumentChangeNotification change)
            {
                if (change.Type != DocumentChangeTypes.Put) return;

                using (var session = _store.OpenSession())
                {
                    var slip = session.Load<RoutingSlip>(change.Id);
                    var ok = slip.Steps.Any() && slip.Steps[0] == Step;
                    if (!ok) return;
                    
                    Thread.Sleep(Program.WorkerSleep);
                        
                    var task = session.Load<SquareTask>(slip.TaskId);
                    var result = Math.Pow(task.NumberToSquare, 2);
                    slip.Results.Add(Step, result);
                    
                    Console.WriteLine("{0} squared is {1}", task.NumberToSquare, result);
                    slip.CompleteStep();

                    session.Advanced.Evict(task);
                    session.SaveChanges();
                }
            }
        }

        class SquareTask
        {
            public string Id { get; set; }
            public int NumberToSquare { get; set; }
        }
    }


    namespace CubeService
    {
        class Worker
        {
            public const string Step = "cube";
            private readonly IDocumentStore _store;

            public Worker(IDocumentStore store)
            {
                _store = store;
            }

            public void Work(DocumentChangeNotification change)
            {
                if (change.Type != DocumentChangeTypes.Put) return;

                using (var session = _store.OpenSession())
                {
                    var slip = session.Load<RoutingSlip>(change.Id);
                    var ok = slip.Steps.Any() && slip.Steps[0] == Step;
                    if (!ok) return;

                    Thread.Sleep(Program.WorkerSleep);

                    var task = session.Load<CubeTask>(slip.TaskId);
                    var result = Math.Pow(task.NumberToCube, 3);
                    slip.Results.Add(Step, result);

                    Console.WriteLine("Wow! {0} cubed is {1}", task.NumberToCube, result);
                    slip.CompleteStep();

                    session.Advanced.Evict(task);
                    session.SaveChanges();
                }
            }
        }

        class CubeTask
        {
            public string Id { get; set; }
            public int NumberToCube { get; set; }
        } 
    }

    namespace QuadService
    {
        class Worker
        {
            public const string Step = "quad";
            private readonly IDocumentStore _store;

            public Worker(IDocumentStore store)
            {
                _store = store;
            }

            public void Work(DocumentChangeNotification change)
            {
                if (change.Type != DocumentChangeTypes.Put) return;

                using (var session = _store.OpenSession())
                {
                    var slip = session.Load<RoutingSlip>(change.Id);
                    var ok = slip.Steps.Any() && slip.Steps[0] == Step;
                    if (!ok) return;

                    Thread.Sleep(Program.WorkerSleep);

                    var task = session.Load<QuadTask>(slip.TaskId);
                    var result = task.NumberToQuadruple * 4;
                    slip.Results.Add(Step, result);

                    Console.WriteLine("Quadruple {0} is just {1}", task.NumberToQuadruple, result);
                    slip.CompleteStep();

                    session.Advanced.Evict(task);
                    session.SaveChanges();
                }
            }
        }

        class QuadTask
        {
            public string Id { get; set; }
            public int NumberToQuadruple { get; set; }
        } 
    }

    public class RoutingSlip
    {
        public string Id { get; set; }
        public string TaskId { get; set; }
        public List<string> Steps { get; set; }
        public List<string> Completed { get; set; }
        public Dictionary<string, double> Results { get; set; }
        public DateTimeOffset Updated { get; set; }

        public RoutingSlip()
        {
            Steps = new List<string>();
            Completed = new List<string>();
            Results = new Dictionary<string, double>();
        }

        public void CompleteStep()
        {
            Completed.Add(Steps.First());
            Steps.RemoveAt(0);
        }

        public bool IsInGoodShape()
        {
            return Steps.Count == 0 || DateTimeOffset.UtcNow.Subtract(Updated).TotalSeconds < 10;
        }
        public void Shake()
        {
            Updated = DateTimeOffset.UtcNow;
        }
    }

    public class Configuration
    {
        public const string StableId = "admin/config";
        public string Id { get { return StableId; } }
        public int WorkerSleepInMilliseconds { get; set; }
    }
}
