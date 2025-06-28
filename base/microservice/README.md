# Framework Architecture

```mermaid
graph TD

subgraph Framework_Layer ["🧱 Framework Layer"]
  Cfg["🧩 Config Loader"]
  DI["🧠 Dependency Injection (FrameworkContext)"]
  Logger["📜 Logger"]
  Metrics["📈 Metrics"]
  Transport["📡 Transport (Redis, etc)"]
  Storage["💾 Storage API"]
  Compliance["🛡️ Compliance Tracker"]
  InfraHealth["🩺 Infra HealthCheck"]
end

subgraph Service_Layer ["⚙️ Service Layer"]
  Heartbeat["💓 Heartbeat Monitor"]
  HealthServer["🌐 Monitor Server (/healthz)"]
  Lifecycle["🔁 Lifecycle Hooks (Start / Stop / HealthCheck)"]
  RPC["📨 RPC Registration & Handling"]
  Emitters["📤 Emitter Logic"]
  Receivers["📥 Receiver Logic"]
  UseCTX["🧭 Access to Framework Context"]
end

MainApp["🚀 main.go"] --> Cfg
MainApp --> DI

Cfg --> DI
DI -->|Injects| Logger
DI --> Metrics
DI --> Transport
DI --> Storage
DI --> Compliance
DI --> InfraHealth

DI --> Service_Layer
Service_Layer --> Heartbeat
Service_Layer --> HealthServer
Service_Layer --> Lifecycle
Service_Layer --> RPC
Service_Layer --> Emitters
Service_Layer --> Receivers
Service_Layer --> UseCTX
UseCTX -->|Provides| DI
```

### Framework Lifecycle

```mermaid
graph TD
    Start[Bootstrap Called]
    CreateCtx[Create Root Context]
    InitFramework[NewFrameworkContext]
    CreateService[Create Developer Service]
    BindLifecycle[Bind ServiceStruct Lifecycle]
    StartService[Supervisor Go: service_run]
    TrapSignal[Supervisor Go: signal_handler]
    WaitShutdown[WaitAndShutdown Called]
    RunService[ServiceStruct.Run Loop]
    Goroutines[Auto Register Methods]
    HealthCheck[Start Health Server + Check Health]
    WaitExit[Block on ctx.Done or error]
    PerformShutdown[Run Shutdown Logic]
    Exit[Process Exit]

    Start --> CreateCtx --> InitFramework --> CreateService --> BindLifecycle
    BindLifecycle --> StartService --> RunService --> Goroutines --> HealthCheck --> WaitExit
    BindLifecycle --> TrapSignal
    TrapSignal --> PerformShutdown
    StartService --> WaitShutdown
    PerformShutdown --> Exit
    WaitShutdown --> Exit
```

### Redis Stream Naming

```mermaid
graph TB
    subgraph Stream Naming Logic
        direction TB
        A[Service Name] --> B[Method Name]
        B --> C[Type RPC / Receiver / Emitter]
        C --> D[Instance ID optional]
        D --> E[Stream Name Format]
    end

    E --> F["rpc:authservice:Ping"]
    E --> G["receiver:billing:GenerateInvoices"]
    E --> H["emitter:fxrate:UpdateRates:instance-01"]
```

# CAASPay Core Framework — Developer Guide

This document describes the high-level structure of the CAASPay microservices framework, its core packages, and how they interrelate.

---

## 🎯 Overview

- **Purpose**: Provide reusable, compliant, production-grade building blocks for:
  - Microservices (`caaspay-core`)
  - API gateway (`caaspay-api`)
  - Shared components (Logging, Metrics, Supervisor, Transport)

---

## 📁 Packages and Ownership

### Package Hierarchy

```mermaid
graph TD
    A[caaspay-core] --> B(pkg/common/logger)
    A --> C(pkg/common/metrics)
    A --> D(pkg/common/supervisor)
    A --> E(pkg/common/transport)
    A --> F(pkg/common/validation)
    A --> G(pkg/api)
    A --> H(internal/framework)

    G --> I[Developer Service]
    H --> I
```

- **pkg/common** — Reusable implementations for Logging, Metrics, Supervisor, Transport, Validation.
- **pkg/api** — Shared interfaces, context keys, types.
- **internal/framework** — Bootstraps framework using common packages.

---

## 📌 Main Flow

```mermaid
sequenceDiagram
    participant Developer
    participant Service as Microservice
    participant Framework
    participant Common

    Developer->>Service: Implements Service logic
    Service->>Framework: Calls Bootstrap()
    Framework->>Common: Loads Logger, Metrics, Supervisor, Transport
    Framework->>Service: Injects Context + Utilities
    Service->>Common: Uses Logger, Metrics, Transport
```

---

## ✅ Design Rules

1. **No cyclic imports** — Common code never imports `api`, only `api` references `common`.
2. **Single source of truth for Context Keys** — All keys live in `pkg/api`.
3. **Unified config** — Config struct in `pkg/api/config.go` holds all nested parts for Logging, Transport, etc.

---

## 📂 Package Breakdown

| Package | Purpose |
| --- | --- |
| `pkg/common/logger` | Logging core (context-aware handler, interfaces) |
| `pkg/common/metrics` | Metrics abstraction (Prometheus, DataDog) |
| `pkg/common/supervisor` | Goroutine manager (health, auto shutdown) |
| `pkg/common/transport` | Redis Streams Transport |
| `pkg/common/validation` | Input struct validator |
| `pkg/api` | Interfaces, context keys, stream types |
| `internal/framework` | Service runtime, lifecycle, health checks |

---

## 🚀 Developer Implementation

- Developer service only imports: `pkg/api` + uses `framework.Bootstrap()`
- Implements:
  - RPC handlers
  - Emitters
  - Receivers

```mermaid
graph LR
    A[Developer Service] --> B[Framework]
    B --> C[Logger]
    B --> D[Metrics]
    B --> E[Supervisor]
    B --> F[Transport]
    B --> G[Validation]
```

---

## 📈 Observability

```mermaid
flowchart TD
    Metrics -->|Prometheus| Grafana
    Metrics -->|Datadog| Datadog
    Logger --> Loki
    Supervisor --> HealthServer
```

---

## 🔐 Compliance & Security

- PCI-DSS friendly
- AES-GCM encryption in Transport
- Supports DLQs and retries

---

## 📄 Example Config

```yaml
framework:
  service_name: payment-service
  logging:
    level: INFO
    redact_sensitive: true
  transport:
    redis_addr: redis:6379
    use_encryption: true
    encryption_key: examplekey
```

---

## 📣 Contact

- Maintainer: <core@caaspay.com>
- Next step: Upgrade to full C4 model with Structurizr
