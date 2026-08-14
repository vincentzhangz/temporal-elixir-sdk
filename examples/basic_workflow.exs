# This project is an unofficial community SDK and is not developed,
# maintained, supported, or endorsed by Temporal Technologies.
#
# Run a Temporal dev server separately, then:
#   mix run examples/basic_workflow.exs

{:ok, connection} =
  Temporal.Connection.open(
    target: System.get_env("TEMPORAL_ADDRESS", "localhost:7233"),
    namespace: System.get_env("TEMPORAL_NAMESPACE", "default")
  )

task_queue = "elixir-basic"

{:ok, worker} =
  Temporal.Worker.start_link(
    connection: connection,
    task_queue: task_queue,
    workflows: %{"Greeting" => fn name -> "hello #{name}" end}
  )

IO.inspect(
  Temporal.Client.execute_workflow(connection, "Greeting", "Temporal",
    id: "elixir-example-#{System.unique_integer([:positive])}",
    task_queue: task_queue
  ),
  label: "workflow result"
)

Temporal.Worker.stop(worker)
Temporal.Connection.close(connection)
