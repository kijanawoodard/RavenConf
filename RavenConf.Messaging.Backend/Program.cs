﻿using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Indexes;

namespace RavenConf.Messaging.Backend
{
    class Program
    {
        private static readonly Random Random = new Random();
        private static readonly string[] ExitCommands = { "quit", "x", "exit" };
        private static string[] _automaticSteps = { "square", "cube" };
        
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
                    Console.Write("Enter the steps: ");
                    var input = Console.ReadLine() ?? string.Empty;
                    if (ExitCommands.Contains(input.ToLower())) break;

                    var steps = new List<string>();
                    var chars = input.ToLower().ToArray().Distinct();

                    foreach (char c in chars)
                    {
                        if (c == 's') steps.Add("square");
                        if (c == 'c') steps.Add("cube");
                        if (c == 'q') steps.Add("quad");
                    }

                    var automatic = !steps.Any();
                    if (automatic) steps.AddRange(_automaticSteps);
                    _automaticSteps = steps.ToArray();

                    using (var session = store.OpenSession())
                    {
                        var task = new Task()
                        {
                            NumberToSquare = Random.Next(1, 10),
                            NumberToCube = Random.Next(1, 10),
                            NumberToQuadruple = Random.Next(1, 10),
                            Steps = steps.ToArray()
                        };
                        
                        session.Store(task);
                        session.SaveChanges();
                        
                        Console.WriteLine("Task Accepted:");
                        Console.WriteLine(task);
                    }

                    Console.WriteLine();
                }
            }

            Console.WriteLine();
            Console.WriteLine("Goodbye");
        }

        class TaskIndex : AbstractIndexCreationTask<Task>
        {
            public TaskIndex()
            {
                Map = tasks => 
                    from task in tasks
                    select new
                    {
                        task.NumberToSquare,
                        task.NumberToCube,
                        NumberToQuad = task.NumberToQuadruple,
                        task.Steps
                    };
            }

            public static readonly ScriptedIndexResults Sir =
                new ScriptedIndexResults
                {
                    Id = ScriptedIndexResults.IdPrefix + new TaskIndex().IndexName,
                    IndexScript = @"
                        var routing = {};
                        routing.Id = 'routing/' + this.__document_id;
                        routing.TaskId = this.__document_id;
                        routing.Steps = this.Steps;
                        routing.Updated = new Date();
                        PutDocument(routing.Id, routing);
                        
                        routing = LoadDocument(routing.Id);
                        routing['@metadata']['Raven-Entity-Name'] = 'Routing';
                        PutDocument(routing.Id, routing);
                    ",
                    DeleteScript = @""
                };
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

            ((DocumentStore)store).DefaultDatabase = defaultDatabase;
            
            new TaskIndex().Execute(store);

            using (var session = store.OpenSession())
            {
                session.Store(TaskIndex.Sir);
                session.SaveChanges();
            }
        }

        class Task
        {
            public string Id { get; set; }
            public int NumberToSquare { get; set; }
            public int NumberToCube { get; set; }
            public int NumberToQuadruple { get; set; }
            public string[] Steps { get; set; }

            struct Step
            {
                public const string Square = "square";
                public const string Cube = "cube";
                public const string Quad = "quad";
            }

            public override string ToString()
            {
                var steps = string.Join("", Steps.Select(Display));
                return string.Format("{0}{1}{2}", Id, Environment.NewLine, steps);
            }

            string Display(string step)
            {
                var value = 0;
                switch (step)
                {
                    case Step.Square:
                        value = NumberToSquare;
                        break;
                    case Step.Cube:
                        value = NumberToCube;
                        break;
                    case Step.Quad:
                        value = NumberToQuadruple;
                        break;
                }

                return string.Format("{0} {1}{2}", step, value, Environment.NewLine);
            }
        }
    }
}
