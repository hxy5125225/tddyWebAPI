using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.OpenApi.Models;
using System;
using System.IO;
using System.Linq;
using System.Text.Encodings.Web;
using System.Text.Unicode;

namespace ElectricalCalculationAPI
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            
            services.AddControllers();
            services.AddControllersWithViews().AddJsonOptions(options =>
            {
                options.JsonSerializerOptions.Encoder = JavaScriptEncoder.Create(UnicodeRanges.All);
            });
            services.AddSwaggerGen(c => { c.SwaggerDoc("v1", new OpenApiInfo { Version = "v1", Title = "配电网网架计算分析API" }); 
                var dir = new DirectoryInfo(AppContext.BaseDirectory); 
                foreach (FileInfo file in dir.EnumerateFiles("*.xml").Distinct())
                { 
                    c.IncludeXmlComments(file.FullName,true); 
                } });
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            //app.UseHttpsRedirection();
            app.UseRouting();
            app.UseAuthorization();
            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
            app.UseSwagger().UseSwaggerUI(c => { c.SwaggerEndpoint("/swagger/v1/swagger.json", "测试Docker发布");
                c.RoutePrefix = "tddy/api"; //自定义访问路由
            });
        }
    }
}
