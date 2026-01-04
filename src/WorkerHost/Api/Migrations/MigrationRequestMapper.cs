using WorkerHost.Messaging;

namespace WorkerHost.Api.Migrations;

public static class MigrationRequestMapper
{
    public static MigrationCommand ToCommand(MigrationRequestDto request)
    {
        ArgumentNullException.ThrowIfNull(request.Transfer);

        var transfer = new TransferPayload
        {
            Source = request.Transfer.Source.ToModel(),
            Destination = request.Transfer.Destination.ToModel(),
        };

        var command = new MigrationCommand
        {
            MigrationId = Guid.NewGuid(),
            MigrationScope = request.Scope,
            RequestedBy = request.RequestedBy,
            RequestedAtUtc = DateTimeOffset.UtcNow,
            Priority = request.Priority,
            Transfer = transfer,
            Metadata = string.IsNullOrEmpty(request.Notes)
                ? null
                : new MigrationMetadata { Notes = request.Notes },
        };

        return command;
    }

    private static TransferEndpoint ToModel(this TransferEndpointRequestDto dto) =>
        new()
        {
            ComputerId = dto.ComputerId,
            AgentId = dto.AgentId,
            AgentIds = dto.AgentIds,
            TaskIds = dto.TaskIds,
        };
}
