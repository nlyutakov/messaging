using Messaging.Management;
using Messaging.RabbitMQ.DependencyInjection;
using Microsoft.AspNetCore.Authentication.JwtBearer;

var builder = WebApplication.CreateBuilder(args);
var identity = builder.Configuration.GetSection("ManagementIdentity");
var authority = identity["Authority"];
var audience = identity["Audience"];
if (!Uri.TryCreate(authority, UriKind.Absolute, out var issuer) || issuer.Scheme != Uri.UriSchemeHttps || string.IsNullOrWhiteSpace(audience))
{
    throw new InvalidOperationException("Configure ManagementIdentity with an HTTPS Authority and an Audience.");
}

builder.Services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(options =>
    {
        options.Authority = authority;
        options.Audience = audience;
        options.MapInboundClaims = false;
    });
builder.Services.AddRabbitMessaging(builder.Configuration.GetSection("RabbitMessaging"));
builder.Services.AddDeadLetterManagement(builder.Configuration.GetSection("DeadLetterManagement"), JwtBearerDefaults.AuthenticationScheme);

var app = builder.Build();
app.Use(async (context, next) =>
{
    context.Response.Headers.ContentSecurityPolicy = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'";
    context.Response.Headers.XContentTypeOptions = "nosniff";
    context.Response.Headers["Referrer-Policy"] = "no-referrer";
    await next(context);
});
app.UseDefaultFiles();
app.UseStaticFiles();
app.UseAuthentication();
app.UseAuthorization();
app.MapDeadLetterManagement();
app.Run();
