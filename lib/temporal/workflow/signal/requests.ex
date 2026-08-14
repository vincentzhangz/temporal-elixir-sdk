defmodule Temporal.Workflow.Signal.Requests do
  @moduledoc """
  Pure builders for Temporal signal service requests.

  These helpers intentionally do not perform RPCs or generate request IDs.
  Callers should create one stable, non-empty request ID and reuse it across
  transport retries so the Temporal Service can deduplicate the request.
  """

  alias Temporal.Api.Common.V1.{
    Header,
    Payloads,
    WorkflowExecution,
    WorkflowType
  }

  alias Temporal.Api.Taskqueue.V1.TaskQueue

  alias Temporal.Api.Workflowservice.V1.{
    SignalWithStartWorkflowExecutionRequest,
    SignalWorkflowExecutionRequest
  }

  @signal_required [:namespace, :identity, :workflow_id, :signal_name, :request_id]
  @signal_with_start_required [
    :namespace,
    :identity,
    :workflow_id,
    :workflow_type,
    :task_queue,
    :signal_name,
    :request_id
  ]

  @spec signal_workflow(keyword()) :: {:ok, struct()} | {:error, term()}
  def signal_workflow(options) when is_list(options) do
    with :ok <- validate_required(options, @signal_required),
         :ok <- validate_run_id(options),
         :ok <- validate_header(options),
         {:ok, input} <- encode(Keyword.get(options, :input), options) do
      {:ok,
       %SignalWorkflowExecutionRequest{
         namespace: Keyword.fetch!(options, :namespace),
         workflow_execution: %WorkflowExecution{
           workflow_id: Keyword.fetch!(options, :workflow_id),
           run_id: Keyword.get(options, :run_id, "")
         },
         signal_name: Keyword.fetch!(options, :signal_name),
         input: input,
         identity: Keyword.fetch!(options, :identity),
         request_id: Keyword.fetch!(options, :request_id),
         header: Keyword.get(options, :header)
       }}
    end
  end

  @spec signal_with_start(keyword()) :: {:ok, struct()} | {:error, term()}
  def signal_with_start(options) when is_list(options) do
    with :ok <- validate_required(options, @signal_with_start_required),
         :ok <- validate_header(options),
         {:ok, workflow_input} <- encode(Keyword.get(options, :workflow_input), options),
         {:ok, signal_input} <- encode(Keyword.get(options, :signal_input), options) do
      {:ok,
       %SignalWithStartWorkflowExecutionRequest{
         namespace: Keyword.fetch!(options, :namespace),
         workflow_id: Keyword.fetch!(options, :workflow_id),
         workflow_type: %WorkflowType{name: Keyword.fetch!(options, :workflow_type)},
         task_queue: %TaskQueue{name: Keyword.fetch!(options, :task_queue)},
         input: workflow_input,
         identity: Keyword.fetch!(options, :identity),
         request_id: Keyword.fetch!(options, :request_id),
         workflow_id_reuse_policy:
           Keyword.get(
             options,
             :workflow_id_reuse_policy,
             :WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
           ),
         workflow_id_conflict_policy:
           Keyword.get(
             options,
             :workflow_id_conflict_policy,
             :WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED
           ),
         signal_name: Keyword.fetch!(options, :signal_name),
         signal_input: signal_input,
         retry_policy: Keyword.get(options, :retry_policy),
         cron_schedule: Keyword.get(options, :cron_schedule, ""),
         memo: Keyword.get(options, :memo),
         search_attributes: Keyword.get(options, :search_attributes),
         header: Keyword.get(options, :header),
         workflow_execution_timeout: Keyword.get(options, :workflow_execution_timeout),
         workflow_run_timeout: Keyword.get(options, :workflow_run_timeout),
         workflow_task_timeout: Keyword.get(options, :workflow_task_timeout),
         workflow_start_delay: Keyword.get(options, :workflow_start_delay)
       }}
    end
  end

  defp validate_required(options, keys) do
    Enum.reduce_while(keys, :ok, fn key, :ok ->
      value = Keyword.get(options, key)

      if is_binary(value) and value != "" do
        {:cont, :ok}
      else
        {:halt, {:error, {:invalid_option, key, value}}}
      end
    end)
  end

  defp validate_run_id(options) do
    case Keyword.get(options, :run_id, "") do
      run_id when is_binary(run_id) -> :ok
      run_id -> {:error, {:invalid_option, :run_id, run_id}}
    end
  end

  defp validate_header(options) do
    case Keyword.get(options, :header) do
      nil -> :ok
      %Header{} -> :ok
      header -> {:error, {:invalid_option, :header, header}}
    end
  end

  defp encode(value, options) do
    encoder = Keyword.get(options, :encoder, &Temporal.Payload.encode/1)

    case encoder.(value) do
      %Payloads{} = payloads -> {:ok, payloads}
      {:ok, %Payloads{} = payloads} -> {:ok, payloads}
      {:error, reason} -> {:error, {:payload_conversion_failed, reason}}
      other -> {:error, {:invalid_encoder_result, other}}
    end
  end
end
