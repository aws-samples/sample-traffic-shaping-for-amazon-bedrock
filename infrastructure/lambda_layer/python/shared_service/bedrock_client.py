"""Bedrock client abstraction — dual-backend (runtime + mantle) dispatch.

This module isolates "many wire shapes at the edge" behind one logical request
contract, per docs/solution/architecture.md §9 (multi-backend invocation). The Lambda body builds a
single request shape; the concrete client translates it to the backend's wire
format and parses the response back into a uniform structure.

Backends supported in Tier 2:
  - runtime / converse  -> bedrock-runtime.converse() (behavior-identical to the
                           pre-Tier-2 invoke_bedrock_model path)
  - mantle  / messages  -> SigV4 POST to the Anthropic Messages API on
                           bedrock-mantle (verified live: HTTP 200 against
                           anthropic.claude-opus-4-7 on us-east-1)

Tier 3 styles (responses, chat_completions, invoke) are intentionally NOT
implemented here. client_for() fails closed on any unknown (backend, api_style).
"""

import json
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.exceptions import ClientError

# Disable boto3 client-side retries on the runtime Converse call so Bedrock
# throttles surface on the first attempt instead of being silently absorbed by
# boto3's default retry mode (which hid ~96% of 429s in the 2026-07-13 run — see
# docs/testing/results.md Appendix — "Bug 1 root cause"). total_max_attempts=1 == one attempt,
# no retries. The mantle path already POSTs with urllib3 retries=False.
_NO_RETRY_CONFIG = Config(retries={"total_max_attempts": 1, "mode": "standard"})

# Mantle wire constant. A missing anthropic_version returns HTTP 400 (verified).
MANTLE_ANTHROPIC_VERSION = "bedrock-2023-05-31"
# SigV4 service name for the mantle endpoint (verified: 'bedrock-mantle').
MANTLE_SERVICE_NAME = "bedrock-mantle"
# Default request timeout for the mantle HTTP call (seconds).
MANTLE_HTTP_TIMEOUT = 60
# OpenAI-on-mantle path. GPT-5.6 (variants) speaks the OpenAI *Responses*
# API — NOT chat/completions (that path 400s: "model does not support
# /v1/chat/completions") and NOT the Anthropic Messages path. Verified live
# 2026-08-04 (acct 111122223333, us-east-1): POST here with {model,input,
# max_output_tokens} returns HTTP 200 for openai.gpt-5.6-*, with a
# usage block carrying input_tokens/output_tokens (same field names as the
# Anthropic mantle path, so post-call reconciliation is identical).
MANTLE_OPENAI_RESPONSES_PATH = "/openai/v1/responses"

# Token estimation constants (kept in sync with dynamo.estimate_request_tokens).
DEFAULT_BYTES_PER_TOKEN = 4.0
SAFETY_MARGIN = 1.1


@dataclass
class TokenEstimate:
    """Pre-call token estimate.

    For runtime (combined TPM) the caller uses ``combined``. For mantle (split
    iTPM/oTPM) the caller uses ``input_tokens`` / ``output_tokens`` independently.
    All three are always populated so callers never have to branch on which
    field exists — only on which they gate against.
    """

    input_tokens: int = 0
    output_tokens: int = 0
    combined: int = 0


@dataclass
class BedrockResponse:
    """Uniform invoke() result across backends.

    ``actual_input_tokens`` / ``actual_output_tokens`` are the REAL counts parsed
    from the provider response when available (mantle returns them separately in
    the ``usage`` block; runtime Converse returns them in ``usage.inputTokens`` /
    ``usage.outputTokens``). They are None when the backend did not report them,
    in which case the caller keeps the pre-call estimate.
    """

    success: bool
    response_body: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    throttled: bool = False
    duration_ms: float = 0.0
    actual_input_tokens: Optional[int] = None
    actual_output_tokens: Optional[int] = None
    raw_text: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


def _estimate_input_tokens(prompt: Optional[str], bytes_per_token: float) -> int:
    """Byte-heuristic input token estimate (matches dynamo.py exactly)."""
    if not prompt:
        return 0
    byte_length = len(prompt.encode("utf-8"))
    return max(1, int(byte_length / bytes_per_token * SAFETY_MARGIN))


class BedrockClient(ABC):
    """Abstract dual-backend client. One logical request, many wire shapes."""

    @abstractmethod
    def invoke(
        self,
        *,
        model_id: str,
        prompt: str,
        max_tokens: int,
        thinking: Optional[Dict[str, Any]] = None,
        effort: Optional[str] = None,
        strip_sampling: bool = False,
        temperature: Optional[float] = None,
        beta_headers: Optional[List[str]] = None,
    ) -> BedrockResponse:
        """Invoke the model and return a uniform BedrockResponse."""
        raise NotImplementedError

    @abstractmethod
    def estimate_tokens(self, prompt: Optional[str], max_tokens: int) -> TokenEstimate:
        """Pre-call token estimate for admission control."""
        raise NotImplementedError


class RuntimeConverseClient(BedrockClient):
    """bedrock-runtime Converse path.

    Behavior-identical to the pre-Tier-2 invoke_bedrock_model(): same client,
    same inferenceConfig handling, same temperature-strip semantics. The ONLY
    additions are (a) optional thinking/effort passthrough via
    additionalModelRequestFields and (b) parsing actual usage out of the
    response. Neither alters the wire call for a config that does not set them.
    """

    def __init__(self, model_config: Optional[Dict[str, Any]] = None, client=None):
        self._config = model_config or {}
        self._client = client or boto3.client("bedrock-runtime", config=_NO_RETRY_CONFIG)
        self._burndown = float(self._config.get("output_token_burndown_rate", 1.0))
        self._bytes_per_token = float(self._config.get("bytes_per_token", DEFAULT_BYTES_PER_TOKEN))

    def estimate_tokens(self, prompt: Optional[str], max_tokens: int) -> TokenEstimate:
        input_tokens = _estimate_input_tokens(prompt, self._bytes_per_token)
        output_cost = int(max_tokens * self._burndown)
        combined = input_tokens + output_cost
        # Runtime gates on the combined bucket. input/output are informational.
        return TokenEstimate(input_tokens=input_tokens, output_tokens=output_cost, combined=combined)

    def invoke(
        self,
        *,
        model_id: str,
        prompt: str,
        max_tokens: int,
        thinking: Optional[Dict[str, Any]] = None,
        effort: Optional[str] = None,
        strip_sampling: bool = False,
        temperature: Optional[float] = None,
        beta_headers: Optional[List[str]] = None,
    ) -> BedrockResponse:
        start_time = time.time()
        try:
            inference_config = {"maxTokens": max_tokens}
            # Only send temperature when the model accepts sampling params AND the
            # caller actually provided one. Next-gen Claude models 400 otherwise.
            if not strip_sampling and temperature is not None:
                inference_config["temperature"] = temperature

            converse_kwargs = {
                "modelId": model_id,
                "messages": [{"role": "user", "content": [{"text": prompt}]}],
                "inferenceConfig": inference_config,
            }

            additional_fields: Dict[str, Any] = {}
            if thinking:
                additional_fields["thinking"] = thinking
            if effort:
                additional_fields.setdefault("output_config", {})["effort"] = effort
            if additional_fields:
                converse_kwargs["additionalModelRequestFields"] = additional_fields

            response = self._client.converse(**converse_kwargs)
            duration_ms = (time.time() - start_time) * 1000

            usage = response.get("usage", {}) if isinstance(response, dict) else {}
            actual_in = usage.get("inputTokens")
            actual_out = usage.get("outputTokens")

            print(f"Bedrock invocation successful: model={model_id}, duration={duration_ms:.2f}ms")
            return BedrockResponse(
                success=True,
                response_body=response,
                throttled=False,
                duration_ms=duration_ms,
                actual_input_tokens=actual_in,
                actual_output_tokens=actual_out,
            )

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            error_message = e.response.get("Error", {}).get("Message", "")
            duration_ms = (time.time() - start_time) * 1000
            is_throttled = error_code in (
                "ThrottlingException",
                "TooManyRequestsException",
                "ServiceQuotaExceededException",
            )
            if is_throttled:
                print(f"Bedrock throttled: model={model_id}, error={error_code}, duration={duration_ms:.2f}ms")
            else:
                print(f"Bedrock invocation failed: model={model_id}, error={error_code}, "
                      f"message={error_message}, duration={duration_ms:.2f}ms")
            return BedrockResponse(
                success=False,
                error=f"{error_code}: {error_message}",
                throttled=is_throttled,
                duration_ms=duration_ms,
            )

        except Exception as e:  # noqa: BLE001 — uniform failure contract
            duration_ms = (time.time() - start_time) * 1000
            print(f"Unexpected error invoking Bedrock: model={model_id}, error={str(e)}, duration={duration_ms:.2f}ms")
            return BedrockResponse(success=False, error=str(e), throttled=False, duration_ms=duration_ms)


class MantleMessagesClient(BedrockClient):
    """bedrock-mantle Anthropic Messages API path (SigV4-signed HTTPS POST).

    Verified ground truth (2026-06-18, account 111122223333, us-east-1):
      - POST https://bedrock-mantle.{region}.api.aws/anthropic/v1/messages
      - SigV4 service name 'bedrock-mantle'; temp creds carry a session token, so
        X-Amz-Security-Token MUST be in the signed headers (botocore SigV4Auth
        adds it automatically when the credentials object has a token).
      - Body MUST include anthropic_version (missing -> 400).
      - Response usage block returns input_tokens / output_tokens SEPARATELY.

    On mantle, burndown does NOT apply to output estimation (oTPM is gated
    directly), so estimate_tokens() uses a 1:1 output estimate pre-call and the
    caller reconciles to the actual usage post-call.
    """

    def __init__(self, model_config: Optional[Dict[str, Any]] = None, session: Optional[boto3.Session] = None):
        self._config = model_config or {}
        self._session = session or boto3.Session()
        self._region = self._config.get("region") or os.environ.get("AWS_REGION") or "us-east-1"
        self._endpoint = self._config.get("endpoint_override") or (
            f"https://{MANTLE_SERVICE_NAME}.{self._region}.api.aws/anthropic/v1/messages"
        )
        self._bytes_per_token = float(self._config.get("bytes_per_token", DEFAULT_BYTES_PER_TOKEN))

    def estimate_tokens(self, prompt: Optional[str], max_tokens: int) -> TokenEstimate:
        # Mantle: no burndown on output estimation. Pre-call estimate is a 1:1
        # ceiling on max_tokens; reconcile to actuals after the call.
        input_tokens = _estimate_input_tokens(prompt, self._bytes_per_token)
        output_tokens = int(max_tokens)
        return TokenEstimate(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            combined=input_tokens + output_tokens,
        )

    def _build_body(
        self,
        *,
        model_id: str,
        prompt: str,
        max_tokens: int,
        thinking: Optional[Dict[str, Any]],
        effort: Optional[str],
        strip_sampling: bool,
        temperature: Optional[float],
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "anthropic_version": MANTLE_ANTHROPIC_VERSION,
            "model": model_id,
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        }
        # Sampling params go top-level on the Messages API. Strip for next-gen.
        if not strip_sampling and temperature is not None:
            body["temperature"] = temperature
        if thinking:
            body["thinking"] = thinking
        if effort:
            body.setdefault("output_config", {})["effort"] = effort
        return body

    def invoke(
        self,
        *,
        model_id: str,
        prompt: str,
        max_tokens: int,
        thinking: Optional[Dict[str, Any]] = None,
        effort: Optional[str] = None,
        strip_sampling: bool = False,
        temperature: Optional[float] = None,
        beta_headers: Optional[List[str]] = None,
    ) -> BedrockResponse:
        start_time = time.time()

        # Defer urllib3 import to call-time so module import stays light and the
        # runtime path never pays for it. urllib3 ships in the Lambda runtime.
        import urllib3

        body = self._build_body(
            model_id=model_id,
            prompt=prompt,
            max_tokens=max_tokens,
            thinking=thinking,
            effort=effort,
            strip_sampling=strip_sampling,
            temperature=temperature,
        )
        payload = json.dumps(body).encode("utf-8")

        headers = {"content-type": "application/json"}
        if beta_headers:
            # Anthropic beta features ride on the anthropic-beta header,
            # comma-joined when multiple are requested.
            headers["anthropic-beta"] = ",".join(beta_headers)

        # SigV4-sign against the mantle service. SigV4Auth pulls the session
        # token from the credentials object and emits X-Amz-Security-Token.
        credentials = self._session.get_credentials()
        if credentials is None:
            return BedrockResponse(
                success=False,
                error="NoCredentialsError: no AWS credentials available for SigV4 signing",
                throttled=False,
                duration_ms=(time.time() - start_time) * 1000,
            )

        aws_request = AWSRequest(method="POST", url=self._endpoint, data=payload, headers=headers)
        SigV4Auth(credentials, MANTLE_SERVICE_NAME, self._region).add_auth(aws_request)
        signed_headers = dict(aws_request.headers)

        http = urllib3.PoolManager()
        try:
            resp = http.request(
                "POST",
                self._endpoint,
                body=payload,
                headers=signed_headers,
                timeout=urllib3.Timeout(total=MANTLE_HTTP_TIMEOUT),
                retries=False,
            )
            duration_ms = (time.time() - start_time) * 1000
            status = resp.status
            raw = resp.data.decode("utf-8", errors="replace")

            if status == 200:
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    parsed = {}
                usage = parsed.get("usage", {}) if isinstance(parsed, dict) else {}
                actual_in = usage.get("input_tokens")
                actual_out = usage.get("output_tokens")
                print(f"Mantle invocation successful: model={model_id}, duration={duration_ms:.2f}ms, "
                      f"actual_in={actual_in}, actual_out={actual_out}")
                return BedrockResponse(
                    success=True,
                    response_body=parsed,
                    throttled=False,
                    duration_ms=duration_ms,
                    actual_input_tokens=actual_in,
                    actual_output_tokens=actual_out,
                    raw_text=raw,
                )

            # Non-200: classify throttle vs hard error. Mantle returns 429 over
            # RPM quota and 503 when rate exceeds available capacity.
            is_throttled = status in (429, 503)
            print(f"Mantle invocation failed: model={model_id}, http_status={status}, "
                  f"duration={duration_ms:.2f}ms, body={raw[:512]}")
            return BedrockResponse(
                success=False,
                error=f"HTTP {status}: {raw[:512]}",
                throttled=is_throttled,
                duration_ms=duration_ms,
                raw_text=raw,
                extra={"http_status": status},
            )

        except Exception as e:  # noqa: BLE001 — uniform failure contract
            duration_ms = (time.time() - start_time) * 1000
            print(f"Unexpected error invoking Mantle: model={model_id}, error={str(e)}, duration={duration_ms:.2f}ms")
            return BedrockResponse(success=False, error=str(e), throttled=False, duration_ms=duration_ms)


class OpenAIResponsesClient(BedrockClient):
    """bedrock-mantle OpenAI *Responses* API path (SigV4-signed HTTPS POST).

    For GPT-5.6 (openai.gpt-5.6-*) fronted by bedrock-mantle. Same
    SigV4 signing and PoolManager/retries=False posture as MantleMessagesClient;
    only the wire shape differs.

    Verified ground truth (2026-08-04, acct 111122223333, us-east-1):
      - POST https://bedrock-mantle.{region}.api.aws/openai/v1/responses
      - Body: {"model", "input" (the prompt string), "max_output_tokens"}. GPT-5.6
        rejects /v1/chat/completions (400 "does not support") and the Anthropic
        Messages path — Responses API is the one that returns 200.
      - Response: output[].content[].text (type == 'output_text'); usage block
        returns input_tokens / output_tokens (SAME field names as the Anthropic
        mantle path, so reconciliation in bedrock_processor is unchanged).

    Like mantle messages, oTPM is gated directly (no burndown), so estimate_tokens
    uses a 1:1 output ceiling pre-call and the caller reconciles to usage actuals.
    """

    def __init__(self, model_config: Optional[Dict[str, Any]] = None, session: Optional[boto3.Session] = None):
        self._config = model_config or {}
        self._session = session or boto3.Session()
        self._region = self._config.get("region") or os.environ.get("AWS_REGION") or "us-east-1"
        self._endpoint = self._config.get("endpoint_override") or (
            f"https://{MANTLE_SERVICE_NAME}.{self._region}.api.aws{MANTLE_OPENAI_RESPONSES_PATH}"
        )
        # GPT tokenizer differs from Claude's ~3.5 bytes/token; default 4.0 is the
        # conservative estimate. Tune per-model via config bytes_per_token; the
        # post-call usage reconciliation corrects any estimate drift regardless.
        self._bytes_per_token = float(self._config.get("bytes_per_token", DEFAULT_BYTES_PER_TOKEN))

    def estimate_tokens(self, prompt: Optional[str], max_tokens: int) -> TokenEstimate:
        input_tokens = _estimate_input_tokens(prompt, self._bytes_per_token)
        output_tokens = int(max_tokens)  # 1:1 pre-call ceiling; reconciled to usage after.
        return TokenEstimate(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            combined=input_tokens + output_tokens,
        )

    @staticmethod
    def _extract_text(parsed: Dict[str, Any]) -> str:
        """Flatten output[].content[].text (type=='output_text') to a single string."""
        parts: List[str] = []
        for item in parsed.get("output", []) or []:
            for c in item.get("content", []) or []:
                if c.get("type") == "output_text" and c.get("text"):
                    parts.append(c["text"])
        return "".join(parts)

    def invoke(
        self,
        *,
        model_id: str,
        prompt: str,
        max_tokens: int,
        thinking: Optional[Dict[str, Any]] = None,
        effort: Optional[str] = None,
        strip_sampling: bool = False,
        temperature: Optional[float] = None,
        beta_headers: Optional[List[str]] = None,
    ) -> BedrockResponse:
        start_time = time.time()
        import urllib3

        body: Dict[str, Any] = {
            "model": model_id,
            "input": prompt,
            "max_output_tokens": max_tokens,
        }
        # Responses API takes sampling top-level; strip for models that reject it.
        if not strip_sampling and temperature is not None:
            body["temperature"] = temperature
        if effort:
            # GPT-5.6 reasoning effort rides on the reasoning block, not output_config.
            body.setdefault("reasoning", {})["effort"] = effort
        payload = json.dumps(body).encode("utf-8")

        credentials = self._session.get_credentials()
        if credentials is None:
            return BedrockResponse(
                success=False,
                error="NoCredentialsError: no AWS credentials available for SigV4 signing",
                throttled=False,
                duration_ms=(time.time() - start_time) * 1000,
            )

        aws_request = AWSRequest(method="POST", url=self._endpoint, data=payload,
                                 headers={"content-type": "application/json"})
        SigV4Auth(credentials, MANTLE_SERVICE_NAME, self._region).add_auth(aws_request)
        signed_headers = dict(aws_request.headers)

        http = urllib3.PoolManager()
        try:
            resp = http.request(
                "POST", self._endpoint, body=payload, headers=signed_headers,
                timeout=urllib3.Timeout(total=MANTLE_HTTP_TIMEOUT), retries=False,
            )
            duration_ms = (time.time() - start_time) * 1000
            status = resp.status
            raw = resp.data.decode("utf-8", errors="replace")

            if status == 200:
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    parsed = {}
                usage = parsed.get("usage", {}) if isinstance(parsed, dict) else {}
                actual_in = usage.get("input_tokens")
                actual_out = usage.get("output_tokens")
                print(f"OpenAI-mantle invocation successful: model={model_id}, duration={duration_ms:.2f}ms, "
                      f"actual_in={actual_in}, actual_out={actual_out}")
                return BedrockResponse(
                    success=True,
                    response_body=parsed,
                    throttled=False,
                    duration_ms=duration_ms,
                    actual_input_tokens=actual_in,
                    actual_output_tokens=actual_out,
                    raw_text=self._extract_text(parsed) or raw,
                )

            is_throttled = status in (429, 503)
            print(f"OpenAI-mantle invocation failed: model={model_id}, http_status={status}, "
                  f"duration={duration_ms:.2f}ms, body={raw[:512]}")
            return BedrockResponse(
                success=False,
                error=f"HTTP {status}: {raw[:512]}",
                throttled=is_throttled,
                duration_ms=duration_ms,
                raw_text=raw,
                extra={"http_status": status},
            )

        except Exception as e:  # noqa: BLE001 — uniform failure contract
            duration_ms = (time.time() - start_time) * 1000
            print(f"Unexpected error invoking OpenAI-mantle: model={model_id}, error={str(e)}, duration={duration_ms:.2f}ms")
            return BedrockResponse(success=False, error=str(e), throttled=False, duration_ms=duration_ms)


def client_for(model_config: Dict[str, Any]) -> BedrockClient:
    """Factory: dispatch on (backend, api_style). Fail closed on unknown.

    Backward-compat (per plan §2):
      - missing backend -> 'runtime' (legacy configs predate the field).
      - missing api_style -> 'converse' for runtime, 'messages' for mantle.

    Supported: runtime/converse, mantle/messages (Anthropic), mantle/responses
    (OpenAI GPT-5.6). Remaining Tier-3 styles (chat_completions, invoke) raise.
    """
    backend = model_config.get("backend", "runtime")
    api_style = model_config.get("api_style") or ("converse" if backend == "runtime" else "messages")

    match (backend, api_style):
        case ("runtime", "converse"):
            return RuntimeConverseClient(model_config)
        case ("mantle", "messages"):
            return MantleMessagesClient(model_config)
        case ("mantle", "responses"):
            return OpenAIResponsesClient(model_config)
        case _:
            raise ValueError(
                f"unsupported backend/api_style: {backend}/{api_style} "
                f"(supported: runtime/converse, mantle/messages, mantle/responses)"
            )
