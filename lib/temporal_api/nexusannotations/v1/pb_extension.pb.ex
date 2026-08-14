defmodule Nexusannotations.V1.PbExtension do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.17.0"

  extend Google.Protobuf.ServiceOptions, :service, 8233,
    optional: true,
    type: Nexusannotations.V1.ServiceOptions

  extend Google.Protobuf.MethodOptions, :operation, 8234,
    optional: true,
    type: Nexusannotations.V1.OperationOptions
end
