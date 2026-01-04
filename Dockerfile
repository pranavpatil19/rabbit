# syntax=docker/dockerfile:1.6

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS base
WORKDIR /app
EXPOSE 8080
ENV ASPNETCORE_URLS=http://+:8080

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY Directory.Build.props .
COPY src/WorkerHost/ WorkerHost/
COPY docs/ docs/
COPY testdata/ testdata/
RUN dotnet restore WorkerHost/WorkerHost.csproj
RUN dotnet publish WorkerHost/WorkerHost.csproj -c Release -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "WorkerHost.dll"]
