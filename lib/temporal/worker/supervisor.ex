defmodule Temporal.Worker.Supervisor do
  @moduledoc false

  use Supervisor

  def start_link(options), do: Supervisor.start_link(__MODULE__, options)

  @impl true
  def init(options) do
    activity_task_queue = Keyword.get(options, :activity_task_queue, options[:task_queue])

    workflow_options =
      options
      |> Keyword.delete(:activities)
      |> Keyword.delete(:activity_task_queue)
      |> Keyword.put(:internal, true)

    activity_options = [
      connection: Keyword.fetch!(options, :connection),
      task_queue: activity_task_queue,
      activities: Keyword.fetch!(options, :activities),
      poll_timeout: Keyword.get(options, :poll_timeout, 30_000)
    ]

    Supervisor.init(
      [
        {Temporal.Worker, workflow_options},
        {Temporal.Activity.Worker, activity_options}
      ],
      strategy: :one_for_one
    )
  end
end
