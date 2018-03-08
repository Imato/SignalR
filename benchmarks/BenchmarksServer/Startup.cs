using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;

namespace BenchmarksServer
{
    public class Startup
    {
        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddSignalR()
                .AddMessagePackProtocol();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            //if (env.IsDevelopment())
            //{
            //    app.UseDeveloperExceptionPage();
            //}

            app.UseSignalR(builder =>
            {
                builder.MapHub<EchoHub>("/default");
            });
        }
    }

    public class EchoHub : Hub
    {
        public async Task Echo(int duration)
        {
            try
            {
                var t = new CancellationTokenSource();
                t.CancelAfter(TimeSpan.FromSeconds(duration));
                while (!t.IsCancellationRequested && !Context.Connection.ConnectionAbortedToken.IsCancellationRequested)
                {
                    await Clients.All.SendAsync("echo", DateTime.UtcNow);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            Console.WriteLine("Echo exited");
        }
    }
}
