# Rate Limit Handler

A step-by-step exploration of how to handle API rate limits in Python, from the simplest possible approach to a production-ready solution.

---

## Contents

- [Naive Approach](./naive-solution/README.md) -> counter based fixed wait, and why it breaks
- [Steps to Run Naive Script](./naive-solution/run-guideline.md)
- [Queue Based Solution](./queue-based-solution/README.md) —> sliding window with retry logic, an optimized way for sequential processing
- [Steps to Run Queue Based Script](./queue-based-solution/run-guideline.md)