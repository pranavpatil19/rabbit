# Code Quality & Automation Plan

Goal: augment the WorkerInfrastructure/WorkerHost solution with automated analyzers, scanners, and testing tools that surface loopholes (static + dynamic). Each step introduces a focused tool, captures how to run it, and explains how we’ll respond to findings before moving to the next step.

---

## Tool Matrix

| Step | Tool | Purpose | Invocation |
|------|------|---------|------------|
| 1 | SonarAnalyzer (already referenced) | Static code analysis during build (`dotnet build`/`dotnet test`) | `dotnet test` |
| 2 | Roslynator analyzers | Additional code-style and correctness rules | `dotnet format` / `dotnet build` |
| 3 | StyleCop.Analyzers | Enforce consistent formatting/documentation | `dotnet build` |
| 4 | DevSkim + SecurityCodeScan | Static security linting (config + source) | `devskim analyze` / `dotnet build` |
| 5 | Stryker.NET | Mutation testing (verifies tests catch defects) | `dotnet stryker` |
| 6 | SharpFuzz + property tests | Fuzz critical serializers/deserializers | `dotnet test --filter Fuzz` |
| 7 | SonarScanner / SonarQube | Centralized dashboards for code smells, coverage, vulnerabilities | `dotnet sonarscanner ...` |

---

## NuGet / Tool Catalog & Status

| Name / Package | Classification | Coverage (static/dynamic) | Install / Restore | Primary Command | Status |
|----------------|----------------|---------------------------|-------------------|-----------------|--------|
| `SonarAnalyzer.CSharp` | Analyzer NuGet (already referenced) | Static | `<PackageReference>` in csproj | `dotnet build` / `dotnet test` | ✅ Enabled |
| `Roslynator.Analyzers` | Analyzer NuGet | Static | `dotnet add <proj> package Roslynator.Analyzers` | `dotnet build` | ⏳ Step 2 |
| `StyleCop.Analyzers` | Analyzer NuGet | Static | `dotnet add <proj> package StyleCop.Analyzers` | `dotnet build` | ⏳ Step 3 |
| `Microsoft.DevSkim.CLI` | .NET global/local tool | Static security | `dotnet tool install --global devskim` | `devskim analyze src --output reports/devskim.sarif` | ⏳ Step 4 |
| `SecurityCodeScan.VS2019` | Analyzer NuGet | Static security (OWASP, CWE) | `dotnet add <proj> package SecurityCodeScan.VS2019` | `dotnet build` | ⏳ Step 4 |
| `dotnet-stryker` | Mutation-test tool | Dynamic (black-box style) | `dotnet tool install --local dotnet-stryker` | `dotnet stryker` | ⏳ Step 5 |
| `SharpFuzz` (+ FsCheck) | Fuzz/property-test helpers | Dynamic | `dotnet add <test proj> package SharpFuzz` | `dotnet test --filter Fuzz` | ⏳ Step 6 |
| `dotnet-sonarscanner` | CI scanner | Static + coverage aggregation | `dotnet tool install --global dotnet-sonarscanner` | `dotnet sonarscanner begin ...` | ⏳ Step 7 |

> **Black-box style testing:** Steps 5 and 6 (Stryker mutation tests and SharpFuzz fuzzing) simulate adversarial or random inputs to expose loopholes that unit tests miss. They rely on NuGet/tooling listed above.

---

## Step-by-Step Execution

### Step 1 – Baseline (existing)
1. Run `dotnet test` to execute unit tests and SonarAnalyzer rules (already referenced via `SonarAnalyzer.CSharp`).
2. Fix any analyzer warnings that appear in build output (none currently outstanding).

✅ **Status:** Completed; build is clean.

### Step 2 – Introduce Roslynator
1. Add `Roslynator.Analyzers` to `WorkerHost` and `WorkerInfrastructure` `.csproj`.
2. Run `dotnet build` and capture warnings.
3. Fix reported issues (unused members, suspicious patterns).

✅ **Status:** Add package + fix warnings.

### Step 3 – StyleCop Formatting
1. Add `StyleCop.Analyzers` to the shared projects.
2. Configure `.editorconfig` to align with our formatting rules (camelCase fields, PascalCase types, etc.).
3. Run `dotnet build`; address documentation/formatting warnings.

### Step 4 – Security Analyzer Sweep
1. Install DevSkim CLI (`dotnet tool install --global devskim`) for config/secret linting.
2. Add `SecurityCodeScan.VS2019` to the core projects for OWASP/CWE rule coverage.
3. Run `devskim analyze src --output devskim-report.sarif` and `dotnet build` to surface analyzer warnings.
4. Review SARIF + build output (hardcoded credentials, insecure crypto, XXE, etc.) and patch code/config.

### Step 5 – Stryker Mutation Tests
1. Add `dotnet tool install dotnet-stryker`.
2. Configure `stryker-config.json` to target `tests/WorkerHost.Tests`.
3. Run `dotnet stryker`. Address surviving mutants by strengthening tests or fixing code.

### Step 6 – Fuzzing Critical Paths
1. Identify serialization/deserialization entry points (e.g., `MigrationPayloadFactory`, `QueueWorker.DeserializeCommand`).
2. Integrate SharpFuzz or property-based tests (FsCheck) targeting those methods.
3. Automate fuzz runs via dedicated test classes (`dotnet test --filter Fuzz`).

### Step 7 – SonarScanner Integration
1. Install the CLI locally (`dotnet tool install --local dotnet-sonarscanner`). Already done; manifests live under `.config/dotnet-tools.json`.
2. Populate `SONAR_TOKEN`, `SONAR_HOST_URL`, and optionally `SONAR_PROJECT_KEY`/`SONAR_ORG`, then run `tools/run-sonarscanner.sh`. The script wraps:
   ```
   dotnet tool run dotnet-sonarscanner begin ...
   dotnet build WorkerHostSolution.sln
   dotnet test WorkerHostSolution.sln /p:CollectCoverage=true ...
   dotnet tool run dotnet-sonarscanner end ...
   ```
3. Add the same sequence to CI (GitHub Actions/Azure DevOps) using encrypted secrets for the token and host URL. Ensure coverage artifacts are published so Sonar shows line coverage.
4. Review the Sonar dashboard, triage critical/major findings, and block merges on new critical issues.

---

## Operational Notes
- Each step should be merged separately with the build/test suite green before moving on.
- Store SARIF or mutation reports under `docs/reports/<tool>/<date>/` for historical tracking.
- When adding new analyzers, update `.editorconfig` to suppress deprecated rules or justify intentional deviations (document in `docs/code-quality-automation-plan.md`).
- For CI automation (GitHub Actions/Azure DevOps), ensure required tools are installed (`dotnet tool restore`) before running the analyze/test steps.

---

## Execution Tracker
1. **Step 1 – Baseline:** ✅ Complete (SonarAnalyzer in place, clean build).
2. **Step 2 – Roslynator:** 🔜 Add package references, run build, fix warnings, document deltas.
3. **Step 3 – StyleCop:** 🔜 Set rule set, update `.editorconfig`, resolve formatting/doc issues.
4. **Step 4 – DevSkim:** 🔜 Install tool, generate SARIF, remediate flagged code smells/secrets.
5. **Step 5 – Stryker Mutation Tests:** 🔜 Configure `stryker-config.json`, run mutation suite, harden tests.
6. **Step 6 – Fuzzing:** 🔜 Add SharpFuzz/FsCheck helpers, create fuzz-focused tests, gate via CI.
7. **Step 7 – SonarScanner Integration:** 🔜 Wire scanner into CI once server credentials are supplied, monitor dashboards per PR/release.

Once all steps are in place, every PR will run analyzers, security checks, mutation tests, and (optionally) SonarQube scanning, significantly reducing the risk of undiscovered loopholes. 
