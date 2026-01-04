using System.Linq;
using FluentValidation.Results;

namespace WorkerHost.Api;

public static class ValidationExtensions
{
    public static IDictionary<string, string[]> ToDictionary(this ValidationResult result)
        => result.Errors
            .GroupBy(
                e => e.PropertyName,
                e => e.ErrorMessage,
                StringComparer.OrdinalIgnoreCase)
            .ToDictionary(
                g => g.Key,
                g => g.ToArray(),
                StringComparer.OrdinalIgnoreCase);
}
