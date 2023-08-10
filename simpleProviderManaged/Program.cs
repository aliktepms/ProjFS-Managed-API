// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

using CommandLine;
using Serilog;
using System;
using System.Runtime.InteropServices;

namespace SimpleProviderManaged
{
    public class Program
    {
        private enum ReturnCode
        {
            Success = 0,
            InvalidArguments = 1,
            GeneralException = 2,
        }
        
        public static int Main(string[] args)
        {
            try
            {
                var parser = new Parser(with =>
                    {
                        with.AutoHelp = true;
                        with.AutoVersion = true;
                        with.EnableDashDash = true;
                        with.CaseSensitive = false;
                        with.CaseInsensitiveEnumValues = true;
                        with.HelpWriter = Console.Out;
                    });
                    
                var parserResult = parser
                    .ParseArguments<ProviderOptions>(args)
                    .WithParsed((ProviderOptions options) =>
                    {
                        // We want verbose logging so we can see all our callback invocations.
                        var logConfig = new LoggerConfiguration()
                           .WriteTo.Console()
                           .WriteTo.File("SimpleProviderManaged-.log", rollingInterval: RollingInterval.Day);

                        if (options.Verbose)
                        {
                            logConfig = logConfig.MinimumLevel.Verbose();
                        }

                        Log.Logger = logConfig.CreateLogger();

                        Log.Information("Start");
                        Run(options);
                    });

                Log.Information("Exit successfully");
                return (int) ReturnCode.Success;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"Unexpected exception: {ex}");
                return (int)ReturnCode.GeneralException;
            }
        }

        private static void Run(ProviderOptions options)
        {
            SimpleProvider provider;
            try
            {
                provider = new SimpleProvider(options);
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Failed to create SimpleProvider.");
                throw;
            }

            Log.Information("Starting provider");

            if (!provider.StartVirtualization())
            {
                Log.Error("Could not start provider.");
                Environment.Exit(1);
            }

            AppDomain.CurrentDomain.ProcessExit += (object sender, EventArgs e) =>
            {
                Log.Information("Process exit");
                provider.DumpStats();
                provider.StopVirtualization();
            };

            Console.WriteLine("Provider is running.  Press <q> to exit, <d> to dump stats.");

            while (true)
            {
                var key = Console.ReadKey(true);
                if (key.KeyChar == 'q')
                {
                    break;
                }
                else if (key.KeyChar == 'd')
                {
                    provider.DumpStats();
                }
            }
        }
    }
}
