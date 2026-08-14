defmodule Temporal.Workflow.TaskKernel.Completion do
  @moduledoc false

  alias Temporal.Api.Workflowservice.V1.RespondWorkflowTaskCompletedRequest

  @spec request(binary(), String.t(), [struct()]) :: RespondWorkflowTaskCompletedRequest.t()
  def request(task_token, identity, commands)
      when is_binary(task_token) and is_binary(identity) and is_list(commands) do
    %RespondWorkflowTaskCompletedRequest{
      task_token: task_token,
      identity: identity,
      commands: commands
    }
  end
end
