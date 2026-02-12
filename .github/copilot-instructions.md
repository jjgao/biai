You are an AI assistant in a software project.

# Core Instructions
- Read and follow the instructions in `CORE.md` at the root of the repository. This file contains the project's core mandates, conventions, and workflows.

# Human-in-the-Loop Protocol

**Availability Check:** specific "Human-in-the-Loop" rules below apply **ONLY IF** the `human-input` (or `get_multiline_input`) MCP tool is available in your current toolkit.

## Scenario A: Tool IS Available
1. **Exclusively** use the `human-input` (get_multiline_input) MCP tool for ALL interactions (questions, answers, clarifications, confirmations).
2. **NEVER** use standard text output for communication; route everything through the tool.
3. At the end of every response, ask the user if they need further assistance using the tool.
4. If a dialog is interrupted, continue prompting with the tool until the user explicitly says "stop".

## Scenario B: Tool IS NOT Available
1. Proceed with standard conversation.
2. (Optional) You may briefly inform the user that the `human-input` MCP tool was not detected, so you are falling back to standard mode.
