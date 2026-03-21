"""
agent/copilot_agent.py
========================
Mosaic AI Agent definition for the AUS Energy Copilot.

This module defines the agent that powers the copilot chat interface.
It can be:
  1. Run locally for development (python copilot_agent.py)
  2. Registered as a Databricks Model Serving endpoint via MLflow
  3. Called directly from the FastAPI backend when no serving endpoint is
     configured

Architecture:
  - LLM: Claude Sonnet 4.5 accessed via the Anthropic SDK directly (primary
          path) or via LangChain ChatAnthropic for local dev.
  - Tools: 14 Unity Catalog function tools (see agent/tools/)
  - Memory: Conversation history passed in each request (stateless agent)
  - Logging: MLflow experiment tracking for all runs

Usage:
  # Local development
  python copilot_agent.py

  # Register with MLflow for Databricks Model Serving
  python copilot_agent.py --register --model-name energy-copilot-agent
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Generator, Optional

import anthropic
import mlflow
from dotenv import load_dotenv

# LangChain imports — used for local dev / AgentExecutor path
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

from agent.prompt_templates import (
    ERROR_RESPONSE,
    INJECTION_RESPONSE,
    REFUSAL_RESPONSE,
    SYSTEM_PROMPT_TEMPLATE,
)
from agent.tools import ALL_TOOLS

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CLAUDE_MODEL = "claude-sonnet-4-5"

# Maximum agentic tool-call rounds before giving up (prevents infinite loops)
MAX_TOOL_ROUNDS: int = 10

# MLflow experiment for tracking copilot agent runs
MLFLOW_EXPERIMENT = os.getenv(
    "MLFLOW_EXPERIMENT_NAME",
    "/Users/energy-copilot-team/copilot-agent-dev",
)

# NEM regions
NEM_REGIONS = "NSW1, QLD1, VIC1, SA1, TAS1"

# AEST offset
_AEST = timezone(timedelta(hours=10))

# ---------------------------------------------------------------------------
# Security: refusal and injection detection
# ---------------------------------------------------------------------------

# Whole-word patterns that indicate a trading advice request.
# Checked against the lowercased user message before any tool calls.
REFUSAL_PATTERNS: list[str] = [
    "buy",
    "sell",
    "invest",
    "trade",
    "position",
    "short",
    "long",
]

# Tokens that indicate a prompt injection attempt.
INJECTION_TOKENS: list[str] = [
    "</s>",
    "<|im_end|>",
    "<|system|>",
    "<|user|>",
    "<|assistant|>",
    "<<SYS>>",
    "[INST]",
    "###instruction",
    "###system",
]

# Pre-compiled whole-word regex for refusal patterns
_REFUSAL_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(p) for p in REFUSAL_PATTERNS) + r")\b",
    re.IGNORECASE,
)


def _is_trading_advice_request(message: str) -> bool:
    """
    Return True if *message* contains a whole-word match for any refusal
    pattern, indicating the user is requesting trading advice.

    Args:
        message: Raw user message text.

    Returns:
        True if the message should be refused as a trading advice request.
    """
    return bool(_REFUSAL_RE.search(message))


def _contains_injection_token(message: str) -> bool:
    """
    Return True if *message* contains any known prompt injection token.

    Checks are case-insensitive substring matches (injection tokens are
    distinctive enough that substring matching is sufficient).

    Args:
        message: Raw user message text.

    Returns:
        True if a potential prompt injection is detected.
    """
    lower = message.lower()
    return any(token.lower() in lower for token in INJECTION_TOKENS)


# ---------------------------------------------------------------------------
# System prompt builder
# ---------------------------------------------------------------------------

def _build_system_prompt() -> str:
    """
    Build the system prompt with the current AEST date/time and NEM regions
    injected from SYSTEM_PROMPT_TEMPLATE.

    Returns:
        Fully rendered system prompt string.
    """
    now_aest = datetime.now(_AEST).strftime("%Y-%m-%d %H:%M AEST")
    return SYSTEM_PROMPT_TEMPLATE.format(
        current_datetime_aest=now_aest,
        nem_regions=NEM_REGIONS,
    )


# ---------------------------------------------------------------------------
# Anthropic UC-tools schema builder
# ---------------------------------------------------------------------------

def _langchain_tool_to_anthropic_tool(lc_tool) -> dict:
    """
    Convert a LangChain @tool decorated function to an Anthropic tool schema
    dict suitable for the ``tools`` parameter of ``client.messages.create()``.

    Args:
        lc_tool: A LangChain BaseTool instance.

    Returns:
        An Anthropic-compatible tool dict with ``name``, ``description``, and
        ``input_schema``.
    """
    schema = lc_tool.args_schema.model_json_schema() if lc_tool.args_schema else {}
    # Strip the title field Pydantic adds; Anthropic doesn't need it
    schema.pop("title", None)
    return {
        "name": lc_tool.name,
        "description": lc_tool.description or "",
        "input_schema": schema if schema else {"type": "object", "properties": {}},
    }


def _get_anthropic_tools() -> list[dict]:
    """
    Return the full list of Unity Catalog tools formatted for the Anthropic
    messages API.

    Returns:
        List of Anthropic tool dicts built from ALL_TOOLS.
    """
    return [_langchain_tool_to_anthropic_tool(t) for t in ALL_TOOLS]


# ---------------------------------------------------------------------------
# Anthropic agentic loop (primary path)
# ---------------------------------------------------------------------------

def _execute_tool(tool_name: str, tool_input: dict) -> str:
    """
    Dispatch a tool call by name against the ALL_TOOLS registry.

    Args:
        tool_name:  Name of the tool to invoke (must match a tool in ALL_TOOLS).
        tool_input: Dict of keyword arguments to pass to the tool.

    Returns:
        String representation of the tool's return value.
    """
    tool_map = {t.name: t for t in ALL_TOOLS}
    if tool_name not in tool_map:
        return json.dumps({"error": f"Unknown tool: {tool_name}"})
    try:
        result = tool_map[tool_name].invoke(tool_input)
        if isinstance(result, (dict, list)):
            return json.dumps(result)
        return str(result)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Tool %s failed", tool_name)
        return json.dumps({"error": str(exc), "tool": tool_name})


def run_agentic_loop(
    user_message: str,
    chat_history: Optional[list[dict]] = None,
    anthropic_client: Optional[anthropic.Anthropic] = None,
) -> dict[str, Any]:
    """
    Run the primary Anthropic agentic loop with UC tools.

    Executes up to MAX_TOOL_ROUNDS rounds of tool calls before returning the
    final text response.  Tracks ``input_tokens`` and ``output_tokens`` across
    all API calls in the loop.

    Args:
        user_message:     The latest user turn text.
        chat_history:     Optional list of prior messages as Anthropic message
                          dicts (``[{"role": "user"|"assistant", "content": ...}]``).
        anthropic_client: Optionally inject an Anthropic client (for testing or
                          when the caller already holds one).

    Returns:
        Dict with keys:
          - ``content``           (str)  Final assistant text.
          - ``tool_calls``        (list) List of tool invocations made.
          - ``input_tokens``      (int)  Total input tokens consumed.
          - ``output_tokens``     (int)  Total output tokens consumed.
          - ``tool_rounds``       (int)  Number of tool-call rounds executed.
    """
    client = anthropic_client or anthropic.Anthropic(
        api_key=os.environ["ANTHROPIC_API_KEY"]
    )
    tools = _get_anthropic_tools()
    system_prompt = _build_system_prompt()

    # Build the initial message list
    messages: list[dict] = list(chat_history or [])
    messages.append({"role": "user", "content": user_message})

    total_input_tokens = 0
    total_output_tokens = 0
    all_tool_calls: list[dict] = []
    rounds = 0

    while rounds < MAX_TOOL_ROUNDS:
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=2048,
            temperature=0.1,
            system=system_prompt,
            tools=tools,
            messages=messages,
        )

        total_input_tokens += response.usage.input_tokens
        total_output_tokens += response.usage.output_tokens

        # Append the assistant turn to the message list
        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            # No more tool calls — extract the final text block
            text_blocks = [
                block.text
                for block in response.content
                if hasattr(block, "text")
            ]
            final_text = "\n".join(text_blocks).strip()
            break

        if response.stop_reason != "tool_use":
            # Unexpected stop reason — return whatever text we have
            text_blocks = [
                block.text
                for block in response.content
                if hasattr(block, "text")
            ]
            final_text = "\n".join(text_blocks).strip() or ERROR_RESPONSE
            break

        # Process tool calls
        tool_result_content: list[dict] = []
        for block in response.content:
            if block.type != "tool_use":
                continue

            tool_result = _execute_tool(block.name, block.input)
            all_tool_calls.append(
                {
                    "tool": block.name,
                    "tool_input": block.input,
                    "observation": tool_result[:500],
                }
            )
            tool_result_content.append(
                {
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": tool_result,
                }
            )

        messages.append({"role": "user", "content": tool_result_content})
        rounds += 1
    else:
        # Exceeded MAX_TOOL_ROUNDS
        logger.warning("MAX_TOOL_ROUNDS (%d) exceeded for query: %.100s", MAX_TOOL_ROUNDS, user_message)
        final_text = (
            "I was unable to complete your request within the allowed number of "
            "tool-call steps. Please try a more specific question."
        )

    return {
        "content": final_text,
        "tool_calls": all_tool_calls,
        "input_tokens": total_input_tokens,
        "output_tokens": total_output_tokens,
        "tool_rounds": rounds,
    }


# ---------------------------------------------------------------------------
# Streaming variant (primary SSE path)
# ---------------------------------------------------------------------------

def stream_predict(
    user_message: str,
    chat_history: Optional[list[dict]] = None,
    anthropic_client: Optional[anthropic.Anthropic] = None,
) -> Generator[dict[str, Any], None, None]:
    """
    Run the agentic loop and yield Server-Sent Event payloads as they arrive.

    Uses ``client.messages.stream()`` context manager for real streaming of
    text deltas.  Tool calls are executed synchronously between stream chunks.

    Yields dicts with ``event`` (str) and ``data`` (str | dict) keys:
      - ``{"event": "text",        "data": "<chunk>"}``  — text delta
      - ``{"event": "tool_call",   "data": {"tool": ..., "input": ...}}``
      - ``{"event": "tool_result", "data": {"tool": ..., "result": ...}}``
      - ``{"event": "done",        "data": {"input_tokens": int, "output_tokens": int}}``
      - ``{"event": "error",       "data": "<message>"}``

    Args:
        user_message:     The latest user turn text.
        chat_history:     Optional list of prior Anthropic message dicts.
        anthropic_client: Optionally inject an Anthropic client.

    Yields:
        SSE payload dicts.
    """
    client = anthropic_client or anthropic.Anthropic(
        api_key=os.environ["ANTHROPIC_API_KEY"]
    )
    tools = _get_anthropic_tools()
    system_prompt = _build_system_prompt()

    messages: list[dict] = list(chat_history or [])
    messages.append({"role": "user", "content": user_message})

    total_input_tokens = 0
    total_output_tokens = 0
    rounds = 0

    try:
        while rounds < MAX_TOOL_ROUNDS:
            tool_calls_this_round: list[dict] = []
            accumulated_content: list = []

            with client.messages.stream(
                model=CLAUDE_MODEL,
                max_tokens=2048,
                temperature=0.1,
                system=system_prompt,
                tools=tools,
                messages=messages,
            ) as stream:
                for event in stream:
                    event_type = type(event).__name__

                    if event_type == "RawContentBlockDeltaEvent":
                        delta = event.delta
                        if hasattr(delta, "type") and delta.type == "text_delta":
                            yield {"event": "text", "data": delta.text}

                    elif event_type == "RawMessageDeltaEvent":
                        if hasattr(event, "usage") and event.usage:
                            total_output_tokens += getattr(event.usage, "output_tokens", 0)

                # Retrieve the final message object from the stream
                final_message = stream.get_final_message()

            total_input_tokens += final_message.usage.input_tokens
            total_output_tokens += final_message.usage.output_tokens

            # Append assistant turn
            messages.append({"role": "assistant", "content": final_message.content})

            if final_message.stop_reason == "end_turn":
                break

            if final_message.stop_reason != "tool_use":
                break

            # Process tool calls
            tool_result_content: list[dict] = []
            for block in final_message.content:
                if not hasattr(block, "type") or block.type != "tool_use":
                    continue

                yield {
                    "event": "tool_call",
                    "data": {"tool": block.name, "input": block.input},
                }

                tool_result = _execute_tool(block.name, block.input)

                yield {
                    "event": "tool_result",
                    "data": {
                        "tool": block.name,
                        "result": tool_result[:500],
                    },
                }

                tool_result_content.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": tool_result,
                    }
                )

            messages.append({"role": "user", "content": tool_result_content})
            rounds += 1
        else:
            logger.warning("MAX_TOOL_ROUNDS exceeded during stream for query: %.100s", user_message)
            yield {
                "event": "text",
                "data": (
                    "I was unable to complete your request within the allowed "
                    "number of tool-call steps."
                ),
            }

        yield {
            "event": "done",
            "data": {
                "input_tokens": total_input_tokens,
                "output_tokens": total_output_tokens,
                "tool_rounds": rounds,
            },
        }

    except Exception as exc:  # noqa: BLE001
        logger.exception("stream_predict failed")
        yield {"event": "error", "data": str(exc)}


# ---------------------------------------------------------------------------
# LangChain AgentExecutor (local dev / fallback)
# ---------------------------------------------------------------------------

def build_agent_executor(
    streaming: bool = False,
    verbose: bool = False,
) -> AgentExecutor:
    """
    Build and return a LangChain AgentExecutor wired to Claude Sonnet 4.5
    with all 14 market data, forecast, analysis, and RAG tools.

    This is the local-dev / fallback path.  The primary production path is
    :func:`run_agentic_loop` (raw Anthropic client with UC tools).

    Args:
        streaming: Enable streaming output from the LLM.
        verbose:   Print LangChain internal reasoning steps.

    Returns:
        A configured AgentExecutor accepting
        ``{"input": str, "chat_history": list}``.
    """
    llm = ChatAnthropic(
        model=CLAUDE_MODEL,
        api_key=os.environ["ANTHROPIC_API_KEY"],
        streaming=streaming,
        max_tokens=2048,
        temperature=0.1,
    )

    system_prompt = _build_system_prompt()

    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", system_prompt),
            MessagesPlaceholder(variable_name="chat_history"),
            ("human", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad"),
        ]
    )

    agent = create_tool_calling_agent(llm=llm, tools=ALL_TOOLS, prompt=prompt)

    executor = AgentExecutor(
        agent=agent,
        tools=ALL_TOOLS,
        verbose=verbose,
        max_iterations=MAX_TOOL_ROUNDS,
        max_execution_time=90,
        handle_parsing_errors=True,
        return_intermediate_steps=True,
    )

    return executor


# ---------------------------------------------------------------------------
# MLflow PyFunc wrapper for Databricks Model Serving
# ---------------------------------------------------------------------------

class CopilotAgentModel(mlflow.pyfunc.PythonModel):
    """
    MLflow PyFunc wrapper for the AUS Energy Copilot agent.

    Enables deployment to Databricks Model Serving with the standard
    ``/invocations`` endpoint interface.

    Supported input formats:
      1. pandas DataFrame with a ``"messages"`` column (standard MLflow chat
         model format).
      2. Plain string — treated as a single-turn user message.
      3. Dict with ``"messages"`` key containing a list of message dicts.

    Output:
      - List of dicts with ``"content"`` (str), ``"tool_calls"`` (list),
        ``"input_tokens"`` (int), and ``"output_tokens"`` (int).
    """

    def __init__(self) -> None:
        self._anthropic_client: Optional[anthropic.Anthropic] = None

    def load_context(self, context: mlflow.pyfunc.PythonModelContext) -> None:
        """Called once when the model is loaded onto the serving endpoint."""
        self._anthropic_client = anthropic.Anthropic(
            api_key=os.environ["ANTHROPIC_API_KEY"]
        )
        logger.info("CopilotAgentModel loaded (model=%s, tools=%d).", CLAUDE_MODEL, len(ALL_TOOLS))

    def _ensure_client(self) -> None:
        """Lazily initialise the Anthropic client if load_context was not called."""
        if self._anthropic_client is None:
            self._anthropic_client = anthropic.Anthropic(
                api_key=os.environ["ANTHROPIC_API_KEY"]
            )

    @staticmethod
    def _parse_input(model_input: Any) -> list[list[dict]]:
        """
        Normalise *model_input* into a list of message lists.

        Handles:
          - pandas DataFrame with a ``"messages"`` column
          - plain str  → single-turn user message
          - dict with ``"messages"`` key
          - list of message dicts (single conversation)

        Args:
            model_input: Raw input from the MLflow ``/invocations`` endpoint.

        Returns:
            List of conversations, each represented as a list of message dicts.
        """
        import pandas as pd  # noqa: PLC0415  (lazy import)

        if isinstance(model_input, pd.DataFrame):
            return [row["messages"] for _, row in model_input.iterrows()]

        if isinstance(model_input, str):
            return [[{"role": "user", "content": model_input}]]

        if isinstance(model_input, dict):
            msgs = model_input.get("messages", [])
            if msgs:
                return [msgs]
            # Single-message dict with "content" key
            content = model_input.get("content", "")
            return [[{"role": "user", "content": content}]]

        if isinstance(model_input, list):
            # Either a list of message dicts, or a list of conversations
            if model_input and isinstance(model_input[0], dict) and "role" in model_input[0]:
                return [model_input]
            return model_input  # assume list of message lists

        return [[{"role": "user", "content": str(model_input)}]]

    def predict(
        self,
        context: mlflow.pyfunc.PythonModelContext,
        model_input: Any,
        params: Optional[dict] = None,
    ) -> list[dict]:
        """
        Process one or more chat requests.

        Applies security checks (injection detection, trading advice refusal)
        before invoking the agentic loop.  Logs token metrics to MLflow.

        Args:
            context:     MLflow model context (unused at inference time but
                         required by the interface).
            model_input: Input in any format accepted by :meth:`_parse_input`.
            params:      Optional inference parameters (currently unused).

        Returns:
            List of result dicts, one per input conversation, each containing:
              ``content``, ``tool_calls``, ``input_tokens``, ``output_tokens``.
        """
        self._ensure_client()

        conversations = self._parse_input(model_input)
        results: list[dict] = []

        for messages in conversations:
            if not messages:
                results.append({"content": ERROR_RESPONSE, "tool_calls": [],
                                 "input_tokens": 0, "output_tokens": 0})
                continue

            # Extract the latest user message and prior history
            last_user_msg = ""
            chat_history: list[dict] = []
            for msg in messages:
                role = msg.get("role", "user")
                content = msg.get("content", "")
                if role == "user":
                    last_user_msg = content
                chat_history_entry = {"role": role, "content": content}
                chat_history.append(chat_history_entry)

            # Drop the last user turn from history (it becomes the current query)
            chat_history = chat_history[:-1]

            # Security checks
            if _contains_injection_token(last_user_msg):
                logger.warning("Prompt injection attempt detected: %.80s", last_user_msg)
                results.append({
                    "content": INJECTION_RESPONSE,
                    "tool_calls": [],
                    "input_tokens": 0,
                    "output_tokens": 0,
                })
                continue

            if _is_trading_advice_request(last_user_msg):
                logger.info("Trading advice refusal triggered for: %.80s", last_user_msg)
                results.append({
                    "content": REFUSAL_RESPONSE,
                    "tool_calls": [],
                    "input_tokens": 0,
                    "output_tokens": 0,
                })
                continue

            # Invoke the agentic loop
            try:
                with mlflow.start_run(nested=True):
                    mlflow.log_param("model", CLAUDE_MODEL)
                    mlflow.log_param("num_tools", len(ALL_TOOLS))

                    result = run_agentic_loop(
                        user_message=last_user_msg,
                        chat_history=chat_history,
                        anthropic_client=self._anthropic_client,
                    )

                    mlflow.log_metric("input_tokens", result["input_tokens"])
                    mlflow.log_metric("output_tokens", result["output_tokens"])
                    mlflow.log_metric("tool_rounds", result["tool_rounds"])
                    mlflow.log_metric(
                        "num_tool_calls", len(result.get("tool_calls", []))
                    )

                results.append(result)

            except Exception as exc:  # noqa: BLE001
                logger.exception("Agent prediction failed")
                results.append({
                    "content": f"I encountered an error processing your request: {exc}",
                    "tool_calls": [],
                    "input_tokens": 0,
                    "output_tokens": 0,
                })

        return results


# ---------------------------------------------------------------------------
# Registration helper
# ---------------------------------------------------------------------------

def register_model(model_name: str = "energy-copilot-agent") -> str:
    """
    Log and register the CopilotAgentModel with the MLflow Model Registry.

    Args:
        model_name: Name to register the model under in Unity Catalog.

    Returns:
        The registered model URI (``models:/<name>/<version>``).
    """
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    pip_requirements = [
        "anthropic>=0.44.0",
        "langchain>=0.3.0",
        "langchain-anthropic>=0.3.0",
        "langchain-core>=0.3.0",
        "databricks-sdk>=0.40.0",
        "databricks-sql-connector>=3.7.0",
        "databricks-vectorsearch>=0.40.0",
        "mlflow>=2.19.0",
        "python-dotenv>=1.0.1",
    ]

    with mlflow.start_run(run_name=f"register-{model_name}"):
        mlflow.log_param("model_name", model_name)
        mlflow.log_param("llm_model", CLAUDE_MODEL)
        mlflow.log_param("num_tools", len(ALL_TOOLS))
        mlflow.log_param("tool_names", [t.name for t in ALL_TOOLS])
        mlflow.log_param("max_tool_rounds", MAX_TOOL_ROUNDS)

        model_info = mlflow.pyfunc.log_model(
            artifact_path="copilot_agent",
            python_model=CopilotAgentModel(),
            pip_requirements=pip_requirements,
            registered_model_name=model_name,
        )

        logger.info("Registered model at: %s", model_info.model_uri)
        return model_info.model_uri


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="AUS Energy Copilot Agent")
    parser.add_argument(
        "--register",
        action="store_true",
        help="Register the model with MLflow Model Registry",
    )
    parser.add_argument(
        "--model-name",
        default="energy-copilot-agent",
        help="MLflow model name to register under",
    )
    parser.add_argument(
        "--query",
        type=str,
        help="Run a single query against the agent (interactive test)",
    )
    parser.add_argument(
        "--use-langchain",
        action="store_true",
        help="Use the LangChain AgentExecutor path instead of the raw Anthropic loop",
    )
    args = parser.parse_args()

    if args.register:
        uri = register_model(args.model_name)
        print(f"Model registered at: {uri}")

    elif args.query:
        if args.use_langchain:
            executor = build_agent_executor(verbose=True)
            result = executor.invoke({"input": args.query, "chat_history": []})
            print("\n--- ANSWER ---")
            print(result["output"])
            if result.get("intermediate_steps"):
                print(f"\n[Used {len(result['intermediate_steps'])} tool call(s)]")
        else:
            # Primary Anthropic path
            if _contains_injection_token(args.query):
                print(INJECTION_RESPONSE)
            elif _is_trading_advice_request(args.query):
                print(REFUSAL_RESPONSE)
            else:
                result = run_agentic_loop(user_message=args.query)
                print("\n--- ANSWER ---")
                print(result["content"])
                if result["tool_calls"]:
                    print(f"\n[Used {len(result['tool_calls'])} tool call(s) in {result['tool_rounds']} round(s)]")
                print(f"[Tokens: {result['input_tokens']} in / {result['output_tokens']} out]")

    else:
        # Interactive REPL using the primary Anthropic agentic loop
        print("AUS Energy Copilot — Interactive Mode (Anthropic loop)")
        print("Type 'quit' to exit. Use --use-langchain for the LangChain path.\n")
        history: list[dict] = []
        while True:
            user_input = input("You: ").strip()
            if user_input.lower() in ("quit", "exit", "q"):
                break

            if _contains_injection_token(user_input):
                print(f"\nCopilot: {INJECTION_RESPONSE}\n")
                continue

            if _is_trading_advice_request(user_input):
                print(f"\nCopilot: {REFUSAL_RESPONSE}\n")
                history.append({"role": "user", "content": user_input})
                history.append({"role": "assistant", "content": REFUSAL_RESPONSE})
                continue

            result = run_agentic_loop(
                user_message=user_input,
                chat_history=history,
            )
            answer = result["content"]
            print(f"\nCopilot: {answer}\n")
            print(
                f"[{result['tool_rounds']} round(s), "
                f"{result['input_tokens']} in / {result['output_tokens']} out tokens]\n"
            )
            history.append({"role": "user", "content": user_input})
            history.append({"role": "assistant", "content": answer})
