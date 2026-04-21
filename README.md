# Rate Limit Handler

A step-by-step exploration of how to handle API rate limits in Python, from the simplest possible approach to a production-ready solution.

---

## Contents

- [Naive Rate Limiting Approach](./naive-solution/README.md) -> Fixed Delay Counter Strategy & Its Limitations
- [Naive Solution Execution Guide](./naive-solution/run-guideline.md)
- [Synchronous Queue-Based Rate Limiting](./queue-based-solution-sync/README.md) -> Sliding Window with Retry Logic (Sequential Optimization)
- [Synchronous Queue Execution Guide](./queue-based-solution-sync/run-guideline.md)
- [Asynchronous Queue-Based Rate Limiting](./queue-based-solution-async/README.md) -> AsyncIO-Driven Queue Processing
- [Persistent Asynchronous Task Queue](./async-pg-task-queue/README.md) -> Database-Backed Queue with Async Processing (PostgreSQL)
- [Distributed Tasks Engine](./distributed-async-solution/README.md) -> Shared Queue based async processing (PostgreSQL)