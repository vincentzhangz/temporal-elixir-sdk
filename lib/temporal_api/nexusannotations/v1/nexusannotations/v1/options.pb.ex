defmodule Nexusannotations.V1.OperationOptions do
  @moduledoc false

  use Protobuf,
    full_name: "nexusannotations.v1.OperationOptions",
    proto_source: "nexusannotations/v1/options.proto",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :name, 1, type: :string
  field :tags, 2, repeated: true, type: :string
end

defmodule Nexusannotations.V1.ServiceOptions do
  @moduledoc false

  use Protobuf,
    full_name: "nexusannotations.v1.ServiceOptions",
    proto_source: "nexusannotations/v1/options.proto",
    protoc_gen_elixir_version: "0.17.0",
    syntax: :proto3

  field :name, 1, type: :string
  field :tags, 2, repeated: true, type: :string
end
