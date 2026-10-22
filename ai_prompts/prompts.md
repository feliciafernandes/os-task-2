# AI Assistance and Prompt Log

## AI Tools Used

- Claude

## 1. Initial Prompt

Provided the assignment requirements for the Readers–Writers and Dining Philosophers synchronization problems and requested Python implementations using semaphores and monitors. The implementations were required to use real threads and produce HTML visualizations based on recorded execution events.

## 2. Follow-up Prompts

- Review the synchronization logic and explain the use of semaphores, locks, and condition-based coordination.
- Check the implementations against the assignment requirements.
- Investigate errors and correct issues identified during testing.
- Improve the HTML visualizations and ensure they reflect the recorded execution events.
- Refine the README files, references, and report formatting.

## 3. Review and Validation

Reviewed the generated source code and supporting files, ran the four implementations, and inspected the HTML simulations. Identified a mismatch in the Dining Philosophers monitor visualization and requested a correction. Tested the invariant checkers with synchronization disabled to verify their ability to detect violations.

## 4. Observations

- Thread scheduling can produce different execution orders across runs, even with the same random seed.
- HTML simulations replay recorded events rather than controlling threads during execution.
- The absence of detected invariant violations does not independently prove correctness.
- Synchronization policies may reduce starvation without guaranteeing strict FIFO fairness.

## 5. Reflection

The development process involved reviewing AI-generated implementations, executing the programs, checking the visualizations, and examining the limitations of concurrent execution and synchronization mechanisms.
