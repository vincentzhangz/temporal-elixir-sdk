defmodule Temporal.Workflow.TaskKernel.Completion do
  @moduledoc false

  alias Temporal.Api.Sdk.V1.WorkflowTaskCompletedMetadata
  alias Temporal.Api.Workflowservice.V1.RespondWorkflowTaskCompletedRequest

  @spec request(binary(), String.t(), [struct()], map(), [struct()]) ::
          RespondWorkflowTaskCompletedRequest.t()
  def request(task_token, identity, commands, query_results \\ %{}, messages \\ [])
      when is_binary(task_token) and is_binary(identity) and is_list(commands) and
             is_map(query_results) and is_list(messages) do
    %RespondWorkflowTaskCompletedRequest{
      task_token: task_token,
      identity: identity,
      commands: commands,
      query_results: query_results,
      messages: messages,
      sdk_metadata: %WorkflowTaskCompletedMetadata{
        sdk_name: Temporal.SDKMetadata.name(),
        sdk_version: Temporal.SDKMetadata.version()
      }
    }
  end
end
