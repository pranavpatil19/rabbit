using Microsoft.AspNetCore.Routing;
using WorkerHost.Dal;

namespace WorkerHost.Api;

internal static class TestDataEndpointExtensions
{
    private const string DefaultRoute = "/testdata/load";

    public static IEndpointRouteBuilder MapTestDataEndpoints(this IEndpointRouteBuilder endpoints)
    {
        endpoints.MapPost(
            DefaultRoute,
            static (TestDataPayload payload, TestDataContext dataContext) =>
            {
                if (payload?.Computers == null || payload.Computers.Count == 0)
                {
                    return Results.BadRequest(new { error = "Payload must include at least one computer." });
                }

                try
                {
                    var result = dataContext.Reload(payload, "API:/testdata/load");
                    return Results.Ok(new { result.Computers, result.Agents });
                }
                catch (ArgumentException ex)
                {
                    return Results.BadRequest(new { error = ex.Message });
                }
            });

        return endpoints;
    }
}
