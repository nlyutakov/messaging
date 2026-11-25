using System.Security.Claims;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Messaging.Management;

public static class DeadLetterManagementExtensions
{
    private const string Read = "messaging.dlq.read";
    private const string Payload = "messaging.dlq.payload";
    private const string Replay = "messaging.dlq.replay";
    private const string Archive = "messaging.dlq.archive";
    private const string Discard = "messaging.dlq.discard";

    public static IServiceCollection AddDeadLetterManagement(this IServiceCollection services, IConfiguration configuration, string authenticationScheme)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(authenticationScheme);
        services.AddOptions<DeadLetterOptions>().Bind(configuration)
            .Validate(options => !string.IsNullOrWhiteSpace(options.DatabasePath) && options.DatabasePath != ":memory:", "A persistent DLQ archive database path is required.")
            .Validate(options => options.MaxReplayAttempts is >= 1 and <= 10, "MaxReplayAttempts must be between 1 and 10.")
            .Validate(options => options.Sources.All(source => !string.IsNullOrWhiteSpace(source.Key) && source.Value.All(target =>
                !string.IsNullOrWhiteSpace(target) && !options.Sources.ContainsKey(target))), "Replay targets must be explicit queue names outside the ingested DLQ set.")
            .ValidateOnStart();
        services.TryAddSingleton<DeadLetterArchive>();
        services.TryAddSingleton<IDeadLetterManager, DeadLetterManager>();
        services.AddHostedService<DeadLetterIngestionService>();
        services.AddAuthorization(options =>
        {
            foreach (var policy in new[] { Read, Payload, Replay, Archive, Discard })
            {
                options.AddPolicy(policy, builder => builder
                    .AddAuthenticationSchemes(authenticationScheme)
                    .RequireAuthenticatedUser()
                    .RequireClaim("messaging.dlq", policy)
                    .RequireAssertion(context => ReadActor(context.User) is { Length: > 0 and <= 256 }));
            }
        });
        return services;
    }

    public static IEndpointRouteBuilder MapDeadLetterManagement(this IEndpointRouteBuilder endpoints)
    {
        var routes = endpoints.MapGroup("/api/dead-letters").AddEndpointFilter(async (context, next) =>
        {
            context.HttpContext.Response.Headers.CacheControl = "no-store";
            return await next(context);
        });

        routes.MapGet("/", async (IDeadLetterManager manager, long? after, int? limit, DeadLetterStatus? status, CancellationToken ct) =>
        {
            if (after is < 0 || limit is < 1 or > 100 || (status.HasValue && !Enum.IsDefined(status.Value)))
            {
                return Results.BadRequest();
            }

            return Results.Ok(await manager.ListAsync(after ?? 0, limit ?? 50, status, ct));
        }).RequireAuthorization(Read);

        routes.MapGet("/counts", async (IDeadLetterManager manager, CancellationToken ct) =>
            Results.Ok(await manager.CountAsync(ct))).RequireAuthorization(Read);

        routes.MapGet("/{id:long}", async (long id, IDeadLetterManager manager, CancellationToken ct) =>
        {
            var item = await manager.InspectAsync(id, ct);
            return item is null ? Results.NotFound() : Results.Ok(item);
        }).RequireAuthorization(Read, Payload);

        MapActions(routes, "replay", DeadLetterAction.Replay, Replay);
        MapActions(routes, "archive", DeadLetterAction.Archive, Archive);
        MapActions(routes, "discard", DeadLetterAction.Discard, Discard);
        return endpoints;
    }

    private static void MapActions(RouteGroupBuilder routes, string operation, DeadLetterAction action, string policy)
    {
        routes.MapPost("/{id:long}/" + operation, async (long id, ClaimsPrincipal principal, IDeadLetterManager manager, CancellationToken ct) =>
        {
            var result = await manager.ApplyAsync(id, action, ReadActor(principal)!, ct);
            return result switch
            {
                DeadLetterResult.Applied => Results.Ok(result),
                DeadLetterResult.Missing => Results.NotFound(),
                DeadLetterResult.Uncertain => Results.Json(result, statusCode: StatusCodes.Status503ServiceUnavailable),
                _ => Results.Conflict(result)
            };
        }).RequireAuthorization(policy);

        routes.MapPost("/batch/" + operation, async (long[] ids, ClaimsPrincipal principal, IDeadLetterManager manager, CancellationToken ct) =>
        {
            if (ids.Length is < 1 or > 20 || ids.Any(id => id <= 0) || ids.Distinct().Count() != ids.Length)
            {
                return Results.BadRequest();
            }

            var actor = ReadActor(principal)!;
            var results = new Dictionary<long, DeadLetterResult>();
            foreach (var id in ids)
            {
                results.Add(id, await manager.ApplyAsync(id, action, actor, ct));
            }

            return Results.Ok(results);
        }).RequireAuthorization(policy);
    }

    private static string? ReadActor(ClaimsPrincipal principal) =>
        principal.FindFirstValue(ClaimTypes.NameIdentifier) ?? principal.FindFirstValue("sub");
}
