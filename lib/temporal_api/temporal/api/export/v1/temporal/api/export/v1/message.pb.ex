defmodule Temporal.Api.Export.V1.WorkflowExecution do
  @moduledoc false

  use Protobuf,
    full_name: "temporal.api.export.v1.WorkflowExecution",
    proto_source: "temporal/api/export/v1/message.proto",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :history, 1, type: Temporal.Api.History.V1.History
end

defmodule Temporal.Api.Export.V1.WorkflowExecutions do
  @moduledoc false

  use Protobuf,
    full_name: "temporal.api.export.v1.WorkflowExecutions",
    proto_source: "temporal/api/export/v1/message.proto",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :items, 1, repeated: true, type: Temporal.Api.Export.V1.WorkflowExecution
end
