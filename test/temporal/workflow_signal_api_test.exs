defmodule Temporal.WorkflowSignalAPITest do
  use ExUnit.Case, async: true

  alias Temporal.Workflow.Machines.SignalInbox
  alias Temporal.Workflow.Signal
  alias Temporal.Workflow.Signal.Dispatcher

  setup do
    on_exit(&Temporal.Workflow.clear_context/0)
    :ok
  end

  test "Continue-As-New rejects unfinished buffered signal work" do
    pending = %Signal{
      event_id: 7,
      signal_name: "later",
      input: "value",
      headers: nil,
      identity: "client",
      request_id: "request-7"
    }

    dispatcher = %Dispatcher{inbox: %SignalInbox{buffered: [pending]}}

    Temporal.Workflow.put_context(%{
      workflow_type: "SignalWorkflow",
      task_queue: "signals",
      pending_commands: [],
      signal_dispatcher: dispatcher
    })

    assert catch_throw(Temporal.Workflow.continue_as_new(%{"carried" => ["value"]})) ==
             {:temporal_signal_failed,
              {:unfinished_signal_handlers, %{pending: 1, buffered_event_ids: [7]}}}
  end

  test "wait helper blocks while an unknown signal remains buffered" do
    pending = %Signal{
      event_id: 9,
      signal_name: "unknown",
      input: nil,
      headers: nil,
      identity: "client",
      request_id: ""
    }

    dispatcher = %Dispatcher{inbox: %SignalInbox{buffered: [pending]}}

    Temporal.Workflow.put_context(%{
      pending_commands: [],
      signal_dispatcher: dispatcher
    })

    assert catch_throw(Temporal.Workflow.wait_for_all_signal_handlers()) ==
             {:temporal_workflow_blocked, []}
  end
end
