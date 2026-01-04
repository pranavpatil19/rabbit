using System.Linq;
using FluentValidation;

namespace WorkerHost.Api.Migrations;

public sealed class MigrationRequestValidator : AbstractValidator<MigrationRequestDto>
{
    public MigrationRequestValidator()
    {
        RuleFor(x => x.RequestedBy).NotEmpty().MaximumLength(100);
        RuleFor(x => x.Transfer).NotNull();

        When(x => x.Scope == WorkerHost.Messaging.MigrationScope.Computer, () =>
        {
            RuleFor(x => x.Transfer!.Source.ComputerId).NotEmpty();
            RuleFor(x => x.Transfer!.Destination.ComputerId).NotEmpty();
        });

        When(x => x.Scope == WorkerHost.Messaging.MigrationScope.Agent, () =>
        {
            RuleFor(x => x.Transfer!.Source.ComputerId).NotEmpty();
            RuleFor(x => x.Transfer!.Source.AgentId).NotEmpty();
            RuleFor(x => x.Transfer!.Destination.ComputerId).NotEmpty();
            RuleFor(x => x.Transfer!.Destination.AgentId).NotEmpty();
        });

        When(x => x.Scope == WorkerHost.Messaging.MigrationScope.TaskBatch, () =>
        {
            RuleFor(x => x.Transfer!.Source.ComputerId).NotEmpty();
            RuleFor(x => x.Transfer!.Source.AgentId).NotEmpty();
            RuleFor(x => x.Transfer!.Source.TaskIds!)
                .NotEmpty()
                .WithMessage("At least one taskId is required.")
                .Must(ids => ids.All(id => !string.IsNullOrWhiteSpace(id)))
                .WithMessage("TaskIds cannot contain blank values.");
        });
    }
}
